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

import java.lang.Math;
import java.util.Collection;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.LinkedList;
import java.util.Queue;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.thrift7.TException;
import org.apache.commons.math3.distribution.NormalDistribution;

import backtype.storm.ILocalCluster;
import backtype.storm.metric.api.IMetricsConsumer;
import backtype.storm.task.IErrorReporter;
import backtype.storm.task.TopologyContext;
import backtype.storm.generated.*;
import backtype.storm.utils.BufferFileInputStream;
import backtype.storm.utils.Utils;
import backtype.storm.utils.NimbusClient;

public class FeedbackMetricsConsumer implements IMetricsConsumer {
	public static IFeedbackAlgorithm algorithm;

	public static final Logger LOG = LoggerFactory.getLogger(FeedbackMetricsConsumer.class);
	private TopologyContext _context;

	/* For each task, keep track of a window of data points */
	private Map<Integer, DataPointWindow> dpwindow;

	/* How many times a task has sent metrics */
	private Map<Integer, Integer> counter;

	/* How many samples to keep in the window */
	private int windowSize;

	private boolean isMetricComponent(String component) {
		String metricPrefix = "__metrics";
		return component.length() >= metricPrefix.length()
			&& metricPrefix.equals(component.substring(0, metricPrefix.length()));
	}

	 @Override
    public void prepare(Map stormConf, Object registrationArgument, TopologyContext context, IErrorReporter errorReporter) {
		_context = context;

		if (algorithm != null) {
			if (algorithm.isPrepared()) {
				algorithm.onRebalance();
			} else {
				algorithm.prepare(stormConf, context);
			}
		}

		windowSize = 5;

		// set up data collection
		dpwindow = new HashMap<Integer, DataPointWindow>();
		counter = new HashMap<Integer, Integer>();
		// mapLastAction = new HashMap<String, LastAction>();
		counter.put(-1, 0);
		for (int i : _context.getTaskToComponent().keySet()) {
			if (!isMetricComponent(_context.getTaskToComponent().get(i))) {
				dpwindow.put(i, new DataPointWindow(windowSize));
				counter.put(i, 0);
			}
		}

	}

	private double mean(List<Double> a) {
		double sum = 0;
		for (Double val : a) {
			sum += val;
		}
		return sum / a.size();
	}

	// Model "a" with a normal distribution, and test whether cdf(mean(b)) > 0.95
	public boolean significantIncrease(List<Double> a, List<Double> b) {
		double meanA = mean(a);
		double sd = 0;
		for (Double val : a) {
			sd += (val - meanA) * (val - meanA);
		}
		sd = Math.sqrt(sd / (a.size() - 1));

		double meanB = mean(b);
		NormalDistribution dist = new NormalDistribution(meanA, sd);
		double p = dist.cumulativeProbability(meanB);

		boolean significant = (p > 0.95);
		System.out.println("p-value=" + p + ", " +
						   (significant ? "increase" : "no increase"));
		return significant;
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

		// calculate the normalized congestion value (num queued / num processed)
		double total = 0;
		for (ComponentStatistics stats : result.values()) {
			stats.congestion = (double)stats.receiveQueueLength / stats.ackCount;
			total += stats.congestion;
		}
		for (ComponentStatistics stats : result.values()) {
			stats.congestion /= total;
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
		// record.add((double)totalAcks / elapsedTime);
		// if (record.size() > 5) {
		// 	// hard coded data for testing
		// 	// Double[] sample = new Double[] {
		// 	// 	945.6,
		// 	// 	1011.4,
		// 	// 	950.8,
		// 	// 	1013.6,
		// 	// 	1013.6
		// 	// };
		// 	significantIncrease(
		// 		Arrays.asList(sample),
		// 		record.subList(record.size()-5, record.size()));
		// }

		System.out.println("FEEDBACK acks/second: " + getTotalAcks(statistics) / windowSize);

		for (String component : statistics.keySet()) {
			ComponentStatistics stats = statistics.get(component);
			System.out.println(component + ".congestion = " + Math.round(stats.congestion * 100) + "%");
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
		Map<String, ComponentStatistics> stats;
		// Every so often, report the statistics & run the algorithm
		if (taskId == -1 && count > windowSize) {
			stats = collectStatistics();
			printStatistics(stats);
			if (algorithm != null) {
				algorithm.update(getTotalAcks(stats)/windowSize, stats);
			}
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
}
