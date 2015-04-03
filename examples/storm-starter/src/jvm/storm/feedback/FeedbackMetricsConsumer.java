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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.HashMap;

import backtype.storm.metric.api.IMetricsConsumer;
import backtype.storm.task.IErrorReporter;
import backtype.storm.task.TopologyContext;
import backtype.storm.utils.Utils;

public class FeedbackMetricsConsumer implements IMetricsConsumer {
    public static final Logger LOG = LoggerFactory.getLogger(FeedbackMetricsConsumer.class);

	private TopologyContext _context;

    @Override
    public void prepare(Map stormConf, Object registrationArgument, TopologyContext context, IErrorReporter errorReporter) {
		_context = context;
		lastUptimeSecs = 0;
		reset();
	}

    static private String padding = "                       ";

	public class ComponentStatistics {
		public long ackCount;
		public double ackLatency;
		public long receiveQueueLength;
		public long sendQueueLength;
		public long counter;

		public ComponentStatistics() {
			ackCount = 0;
			ackLatency = 0;
			receiveQueueLength = 0;
			sendQueueLength = 0;
			counter = 0;
		}

		public void processDataPoints(Map<String, Object> dpMap) {
			counter++;
			if (dpMap.containsKey("__ack-count")
				&& dpMap.containsKey("__process-latency")) {
				long ackCount = 0;
				Map<String, Long> ackCountMap = (Map<String, Long>)dpMap.get("__ack-count");
				for (Long val : ackCountMap.values()) {
					ackCount += val;
				}

				double ackLatency = 0;
				Map<String, Double> ackLatencyMap = (Map<String, Double>)dpMap.get("__process-latency");
				for (Double val : ackLatencyMap.values()) {
					ackLatency += val;
				}

				if (ackCount > 0) {
					this.ackLatency = (ackLatency * ackCount + this.ackLatency * this.ackCount) / (ackCount + this.ackCount);
					this.ackCount += ackCount;
				}
			}
			if (dpMap.containsKey("__receive")) {
				Map<String, Long> receive =
					(Map<String, Long>)dpMap.get("__receive");
				receiveQueueLength += receive.get("population");
			}
			if (dpMap.containsKey("__sendqueue")) {
				Map<String, Long> send =
					(Map<String, Long>)dpMap.get("__sendqueue");
				sendQueueLength += send.get("population");
			}
		}
	}

	long totalAcks;
	double totalSeconds;
	double lastUptimeSecs;

	Map<String, ComponentStatistics> statistics;
	Map<Integer, Boolean> seen;

	private boolean isMetricComponent(String component) {
		String metricPrefix = "__metrics";
		return component.length() >= metricPrefix.length()
			&& metricPrefix.equals(component.substring(0, metricPrefix.length()));
	}

	public void reset() {
		totalAcks = 0;
		totalSeconds = 0;
		statistics = new HashMap<String, ComponentStatistics>();

		seen = new HashMap<Integer, Boolean>();
		seen.put(-1, false);
		for (int i=0; i<_context.getTaskToComponent().size(); i++) {
			String component = _context.getTaskToComponent().get(i);
			if (component != null) {
				if (!isMetricComponent(component)) {
					seen.put(i, false);
				}
			}
		}
	}

	public void onReceivedStatistics() {
		System.out.println("FEEDBACK RECEIVED DATA");
		System.out.println("FEEDBACK acks/second = " + ((double)totalAcks / totalSeconds) + " and seconds = " + totalSeconds);
		for (String component : statistics.keySet()) {
			ComponentStatistics cstats = statistics.get(component);
			System.out.println("FEEDBACK " + component + ".sendQueueLength = " + cstats.sendQueueLength / stats.counter);
			System.out.println("FEEDBACK " + component + ".receiveQueueLength = " + cstats.receiveQueueLength / stats.counter);
			System.out.println("FEEDBACK " + component + ".ackCount = " + cstats.ackCount);
			System.out.println("FEEDBACK " + component + ".ackLatency = " + cstats.ackLatency);
		}
	}

    @Override
    public void handleDataPoints(TaskInfo taskInfo, Collection<DataPoint> dataPoints) {
		// Load Datapoints into Map
		Map<String, Object> dpMap = new HashMap<String, Object>();
		for (DataPoint p : dataPoints) {
			dpMap.put(p.name, p.value);
		}

		// Update statistics for given task
		ComponentStatistics stats = statistics.get(taskInfo.srcComponentId);
		if (stats == null)
			stats = new ComponentStatistics();
		stats.processDataPoints(dpMap);
		statistics.put(taskInfo.srcComponentId, stats);

		// If the task is a spout, increment totalAcks
		if (dpMap.containsKey("__ack-count")) {
			Map<String, Long> ackCounts = (Map<String, Long>)dpMap.get("__ack-count");
			if (ackCounts.containsKey("default")) {
				totalAcks += ackCounts.get("default");
			}
		}

		// If the task is a system task, increment the total seconds
		if (taskInfo.srcTaskId == -1) {
			double uptimeSecs = (Double)dpMap.get("uptimeSecs");
			totalSeconds += (uptimeSecs - lastUptimeSecs);
			lastUptimeSecs = uptimeSecs;
		}

		// Check for whether we've seen all the tasks
		seen.put(taskInfo.srcTaskId, true);
		boolean done = true;
		for (boolean val : seen.values()) {
			done = done && val;
		}
		if (done) {
			onReceivedStatistics();
			reset();
		}

        // StringBuilder sb = new StringBuilder();
        // String header = String.format("FEEDBACK %d\t%15s:%-4d\t%3d:%-11s\t",
        //     taskInfo.timestamp,
        //     taskInfo.srcWorkerHost, taskInfo.srcWorkerPort,
        //     taskInfo.srcTaskId,
        //     taskInfo.srcComponentId);
        // sb.append(header);
        // for (DataPoint p : dataPoints) {
        //     sb.delete(header.length(), sb.length());
        //     sb.append(p.name)
        //         .append(padding).delete(header.length()+23,sb.length()).append("\t")
        //         .append(p.value);
		// 	System.out.println(sb.toString());
        // }
    }

    @Override
    public void cleanup() { }
}
