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
import java.util.Queue;
import java.util.ArrayDeque;
import java.util.ArrayList;

import org.apache.thrift7.TException;

import backtype.storm.metric.api.IMetricsConsumer;
import backtype.storm.task.IErrorReporter;
import backtype.storm.task.TopologyContext;
import backtype.storm.generated.Nimbus;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.ClusterSummary;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.Nimbus;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.SubmitOptions;
import backtype.storm.generated.TopologySummary;
import backtype.storm.utils.BufferFileInputStream;
import backtype.storm.utils.Utils;
import backtype.storm.utils.NimbusClient;

public class FeedbackMetricsConsumer implements IMetricsConsumer {
    public static final Logger LOG = LoggerFactory.getLogger(FeedbackMetricsConsumer.class);
	private TopologyContext _context;

	// For each task, keep track of a window of data points
	private Map<Integer, DataPointWindow> dpwindow;

	// How many times a task has sent metrics
	private Map<Integer, Integer> counter;

	// How many samples to keep in the window
	private int windowSize;

	private boolean isMetricComponent(String component) {
		String metricPrefix = "__metrics";
		return component.length() >= metricPrefix.length()
			&& metricPrefix.equals(component.substring(0, metricPrefix.length()));
	}

    @Override
    public void prepare(Map stormConf, Object registrationArgument, TopologyContext context, IErrorReporter errorReporter) {
		_context = context;

		windowSize = 5;
		dpwindow = new HashMap<Integer, DataPointWindow>();
		counter = new HashMap<Integer, Integer>();
		counter.put(-1, 0);
		for (int i : _context.getTaskToComponent().keySet()) {
			if (!isMetricComponent(_context.getTaskToComponent().get(i))) {
				dpwindow.put(i, new DataPointWindow(windowSize));
				counter.put(i, 0);
			}
		}
	}

	// Aggregate the collected data points into component-specific metrics
	public Map<String, ComponentStatistics> collectStatistics() {
		Map<String, ComponentStatistics> result = new HashMap<String, ComponentStatistics>();
		result.put("__acker", new ComponentStatistics());
		for (String component : _context.getComponentIds()) {
			result.put(component, new ComponentStatistics());
		}
		for (int task : dpwindow.keySet()) {
			String component = _context.getTaskToComponent().get(task);
			for (Map<String, Object> dp : dpwindow.get(task).dps) {
				if (dp != null && result.containsKey(component)) {
					result.get(component).processDataPoints(dp);
				}
			}
		}
		return result;
	}

	// Get the total number of acks from all spout tasks
	public long getTotalAcks(Map<String, ComponentStatistics> statistics) {
		long totalAcks = 0;
		for (ComponentStatistics stats : statistics.values()) {
			if (stats.isSpout) {
				totalAcks += stats.ackCount;
			}
		}
		return totalAcks;
	}

	public void printStatistics(Map<String, ComponentStatistics> statistics) {
		long totalAcks = getTotalAcks(statistics);
		double elapsedTime = windowSize;

		System.out.println("FEEDBACK RECEIVED DATA");
		System.out.println("Total Seconds: " + elapsedTime);
		System.out.println("Acks/second: " + totalAcks / elapsedTime);

		for (String component : statistics.keySet()) {
			ComponentStatistics stats = statistics.get(component);
			if (stats.counter > 0) {
				System.out.println("FEEDBACK " + component + ".sendQueueLength = " + stats.sendQueueLength / stats.counter);
				System.out.println("FEEDBACK " + component + ".receiveQueueLength = " + stats.receiveQueueLength / stats.counter);
			}
			System.out.println("FEEDBACK " + component + ".ackCount = " + stats.ackCount);
			System.out.println("FEEDBACK " + component + ".ackLatency = " + stats.ackLatency);
			System.out.println("FEEDBACK " + component + ".capacity = " + (1000 / stats.ackLatency) * elapsedTime);
		}
	}

    @Override
    public void handleDataPoints(TaskInfo taskInfo, Collection<DataPoint> dataPoints) {
		int taskId = taskInfo.srcTaskId;
		int count = counter.get(taskId);
		counter.put(taskId, count + 1);

		Map<String, Object> dp = new HashMap<String, Object>();
		for (DataPoint p : dataPoints) {
			dp.put(p.name, p.value);
		}

		// Every so often, report the statistics
		if (taskId == -1 && count > windowSize) {
			printStatistics(collectStatistics());
		}

		// Update the window
		if (taskId >= 0) {
			dpwindow.get(taskId).putDataPoints(count, dp);
			// System.out.println(taskId + ":" + taskInfo.srcComponentId);
			// System.out.println(dp);
		}
    }

    @Override
    public void cleanup() { }
}

class DataPointWindow {
	public ArrayList<Map<String, Object>> dps;
	public int k;

	public DataPointWindow(int k) {
		dps = new ArrayList<Map<String, Object>>(k);
		for (int i=0; i<k; i++) {
			dps.add(null);
		}
		this.k = k;
	}

	void putDataPoints(int counter, Map<String, Object> dataPoints) {
		dps.set(counter % k, dataPoints);
	}
}

class ComponentStatistics {
	public boolean isSpout;

	public long ackCount;
	public double ackLatency;
	public long receiveQueueLength;
	public long sendQueueLength;
	public long counter;

	public ComponentStatistics() {
		isSpout = false;
		ackCount = 0;
		ackLatency = 0;
		receiveQueueLength = 0;
		sendQueueLength = 0;
		counter = 0;
	}

	private void processBolt(Map<String, Object> dpMap) {
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

	private void processSpout(Map<String, Object> dpMap) {
		isSpout = true;

		Map<String, Long> ackCounts = (Map<String, Long>)dpMap.get("__ack-count");
		if (ackCounts.containsKey("default")) {
			ackCount += ackCounts.get("default");
		}
	}

	public void processDataPoints(Map<String, Object> dpMap) {
		counter++;
		if (dpMap.containsKey("__ack-count")) {
			if (dpMap.containsKey("__execute-latency")) {
				processBolt(dpMap);
			} else {
				processSpout(dpMap);
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
