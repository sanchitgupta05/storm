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

import javax.net.ssl.HttpsURLConnection;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;

public class EmailWrapper implements IFeedbackAlgorithm {
	private static EmailWrapper _instance;

	private AlgorithmState state;
	private IFeedbackAlgorithm algorithm;
	private int k;

	private List<Map<String, String>> emailLog;

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
		emailLog = (List<Map<String, String>>) state.loadObject("emailLog");
		algorithm.load();

		if (emailLog == null) {
			emailLog = new ArrayList<Map<String, String>>();
		}
	}

	public void save() {
		state.saveObject("emailLog", emailLog);
		algorithm.save();
	}

	public static void log(String key, Object value) {
		if (_instance != null) {
			_instance.addToLog(key, value);
		}
	}

	public void addToLog(String key, Object value) {
		int n = emailLog.size();
		if (n > 0) {
			String s = (value == null) ? "null" : value.toString();
			emailLog.get(n - 1).put(key, s);
		}
	}

	public void run(Map<String, ComponentStatistics> statistics) {
		emailLog.add(new HashMap<String, String>());

		// we want to make it independent of the number of hosts?
		// to be able to guess the tolerance without knowing that information?
		// Also, we want to favor smaller configurations, so we need to
		// take a statisically significant max favoring smaller configurations

		log("parallelism", state.parallelism);
		log("throughput", state.newThroughput);
		log("throughputs", state.newThroughputs);

		if (emailLog.size() <= k) {
			algorithm.run(statistics);
		}

		if (emailLog.size() == k+1) {
			try {
				sendEmail();
				state.deactivate();
			} catch (Exception e) {
				System.out.println("sendEmail() exception: " + e);
			}
		}
	}

	private String getContent() {
		List<String> columns = new ArrayList<String>();

		columns.add("parallelism");
		columns.add("throughput");
		columns.add("correlation");
		columns.add("cpuBound");
		columns.add("cores");
		columns.add("penalty");

		// Set<String> fields = new HashSet<String>();
		// for (Map<String, String> entry : emailLog) {
		// 	for (String key : entry.keySet()) {
		// 		fields.add(key);
		// 	}
		// }
		// List<String> columns = new ArrayList<String>(fields);

		return getContent(columns);
	}

	private String getContent(List<String> columns) {
		List<String> lines = new ArrayList<String>();
		List<String> header = new ArrayList<String>();
		for (String column : columns) {
			header.add(column);
		}
		lines.add(StringUtils.join(header, "; "));
		for (Map<String, String> entry : emailLog) {
			List<String> line = new ArrayList<String>();
			for (String column : columns) {
				line.add(entry.get(column));
			}
			lines.add(StringUtils.join(line, "; "));
		}
		return StringUtils.join(lines, "\n");
	}

	private void sendEmail() throws Exception {
		String content = getContent();

		String url = "https://api.mailgun.net/v3/sandbox9a097dfb563f4cbe8ffa1fa931fa76ea.mailgun.org/messages";
		String data = "";
		data += String.format("from=%s", "bot@525project.io");
		data += String.format("&to=%s", "xmasotto@gmail.com");
		data += String.format("&subject=%s", "Results for " + state.topologyName);
		data += String.format("&text=%s", content);

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

		//print result
		System.out.println(response.toString());
	}

	private String getBasicAuthenticationEncoding(String username, String password) {
        String userPassword = username + ":" + password;
        return new String(Base64.encodeBase64(userPassword.getBytes()));
    }
}
