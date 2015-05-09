/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package storm.feedback;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.Timer;
import java.util.TimerTask;

import javax.net.ssl.HttpsURLConnection;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;

public class EmailWrapper implements IFeedbackAlgorithm {
	private static EmailWrapper _instance;

	private AlgorithmState state;
	private IFeedbackAlgorithm algorithm;
	private int k;

	private Timer timer;
	private Long emailStartTime;
	private List<Map<String, Object>> emailLog;
	private List<String> sentEmails;

	public EmailWrapper(int k, IFeedbackAlgorithm algorithm) {
		this.k = k;
		this.algorithm = algorithm;
		_instance = this;
	}

	public void setState(AlgorithmState state) {
		this.state = state;
		algorithm.setState(state);
	}

	public void load() {
		emailLog = (List<Map<String, Object>>) state.loadObject("emailLog");
		emailStartTime = (Long) state.loadObject("emailStartTime");
		sentEmails = (List<String>) state.loadObject("sentEmails");
		algorithm.load();

		if (emailLog == null) {
			emailLog = new ArrayList<Map<String, Object>>();
		}

		if (emailStartTime == null) {
			emailStartTime = System.currentTimeMillis();
		}

		if (sentEmails == null) {
			sentEmails = new ArrayList<String>();
		}

		if (timer == null) {
			timer = new Timer();
			// scheduleEmail("Progress (15 minutes)", 15 * 60 * 1000);
			scheduleEmail("Progress (30 minutes)", 30 * 60 * 1000);
			scheduleEmail("Progress (45 minutes)", 45 * 60 * 1000);
		}
	}

	public void save() {
		state.saveObject("emailLog", emailLog);
		state.saveObject("emailStartTime", emailStartTime);
		state.saveObject("sentEmails", sentEmails);
		algorithm.save();
	}

	private void scheduleEmail(String header, long time) {
		long elapsed = System.currentTimeMillis() - emailStartTime;
		long deltaTime = time - elapsed;

		if (deltaTime > 0) {
			EmailTask task = new EmailTask(this, header);
			timer.schedule(task, deltaTime);
		} else {
			// if (!sentEmails.contains(header)) {
			// 	sendEmail(header);
			// }
		}
	}

	public static void log(String key, Object value) {
		if (_instance != null) {
			_instance.addToLog(key, value);
		}
	}

	public void addToLog(String key, Object value) {
		int n = emailLog.size();
		if (n > 0) {
			emailLog.get(n - 1).put(key, value);
		}
	}

	public void run(Map<String, ComponentStatistics> statistics) {
		emailLog.add(new HashMap<String, Object>());

		// we want to make it independent of the number of hosts?
		// to be able to guess the tolerance without knowing that information?
		// Also, we want to favor smaller configurations, so we need to
		// take a statisically significant max favoring smaller configurations

		long ms = System.currentTimeMillis() - emailStartTime;
		log("seconds", (ms / 1000));

		log("parallelism", state.parallelism);
		log("throughput", state.newThroughput);
		log("throughputs", state.newThroughputs);
		log("statistics", statistics);

		if (emailLog.size() <= k) {
			algorithm.run(statistics);
		}

		if (emailLog.size() == k+1) {
			long elapsed = System.currentTimeMillis() - emailStartTime;

			// Send finished email
			String finishedHeader = String.format("Finished (%d minutes)", elapsed / (1000 * 60));
			sendEmail(finishedHeader, getContent());

			// Send extra
			// sendEmail("Extra Information", getExtraContent());

			state.deactivate();
		}
	}

	private String getContent() {
		List<String> columns = new ArrayList<String>();

		columns.add("seconds");
		columns.add("parallelism");
		columns.add("throughput");
		columns.add("correlation");
		columns.add("cpuBound");
		columns.add("cores");
		columns.add("penalty");
		columns.add("throughputs");

		return getContent(columns);
	}

	public String getContent(List<String> columns) {
		List<String> lines = new ArrayList<String>();
		List<String> header = new ArrayList<String>();
		for (String column : columns) {
		 	header.add(column);
		}
		lines.add(StringUtils.join(header, "; "));
		for (Map<String, Object> entry : emailLog) {
			List<String> line = new ArrayList<String>();
			for (String column : columns) {
				line.add(String.format("%s", entry.get(column)));
			}
			lines.add(StringUtils.join(line, "; "));
		}
		return StringUtils.join(lines, "\n");
	}

	private String getExtraContent() {
		StringBuilder builder = new StringBuilder();

		int n = emailLog.size();
		for (int i=0; i<n; i++) {
			Map<String, Object> data = emailLog.get(i);
			Map<String, Integer> parallelism =
				(Map<String, Integer>)data.get("parallelism");
			Map<String, ComponentStatistics> statistics =
				(Map<String, ComponentStatistics>)data.get("statistics");
			if (parallelism == null || statistics == null) {
				continue;
			}

			builder.append(String.format("Statistics for iteration %d:\n", i));
			for (String component : statistics.keySet()) {
				ComponentStatistics stats = statistics.get(component);
				if (!parallelism.containsKey(component))
					continue;

				builder.append(String.format("%s.receiveLatency=%s\n", component, stats.receiveLatency));
				builder.append(String.format("%s.sendLatency=%s\n", component, stats.sendLatency));

				if (!stats.isSpout) {
					// potential throughput
					double potential = 1000.0 / stats.executeLatency * parallelism.get(component);
					builder.append(String.format("%s.potential=%s\n", component, potential));

					// actual throughput
					builder.append(String.format("%s.actual=%s\n", component, stats.executeCount));
				}
			}
			builder.append("\n");
		}

		return builder.toString();
	}

	private void sendEmail(String header, String content) {
		try {
			String url = "https://api.mailgun.net/v3/sandbox9a097dfb563f4cbe8ffa1fa931fa76ea.mailgun.org/messages";
			String data = "";
			data += String.format("from=%s", "bot@525project.io");
			data += String.format("&to=%s", "xmasotto@gmail.com");
			data += String.format("&subject=%s", "Status Update for Topology " + state.topologyContext.getStormId());
			data += String.format("&text=%s", header + "\n\n" + content);

			URL obj = new URL(url);
			HttpURLConnection con = (HttpURLConnection) obj.openConnection();

			// add request header
			con.setRequestMethod("POST");
			// con.setRequestProperty("User-Agent", USER_AGENT);
			con.setRequestProperty("Accept-Language", "en-US,en;q=0.5");
			con.setRequestProperty("Authorization", "Basic " + getBasicAuthenticationEncoding("api", "key-d3a333320e26c946dca91740de66bb10"));

			// Send post request
			con.setDoOutput(true);
			DataOutputStream wr = new DataOutputStream(con.getOutputStream());
			wr.writeBytes(data);
			wr.flush();
			wr.close();

			int responseCode = con.getResponseCode();
			System.out.println("\nSending 'POST' request to URL : " + url);
			System.out.println("Post parameters : " + data);
			System.out.println("Response Code : " + responseCode);

			BufferedReader in = new BufferedReader(
				new InputStreamReader(con.getInputStream()));
			String inputLine;
			StringBuffer response = new StringBuffer();

			while ((inputLine = in.readLine()) != null) {
				response.append(inputLine);
			}
			in.close();

			System.out.println(response.toString());

			sentEmails.add(header);
			state.save();

		} catch (Exception e) {
			System.out.println("sendEmail() exception: " + e);
		}
	}

	private String getBasicAuthenticationEncoding(String username, String password) {
        String userPassword = username + ":" + password;
        return new String(Base64.encodeBase64(userPassword.getBytes()));
    }

	class EmailTask extends TimerTask {
		private EmailWrapper wrapper;
		private String header;
		public EmailTask(EmailWrapper wrapper, String header) {
			this.wrapper = wrapper;
			this.header = header;
		}

		public void run() {
			wrapper.sendEmail(header, wrapper.getContent());
		}
	}
}
