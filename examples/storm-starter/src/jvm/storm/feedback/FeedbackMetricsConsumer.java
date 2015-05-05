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

import backtype.storm.Config;
import backtype.storm.ILocalCluster;
import backtype.storm.metric.api.IMetricsConsumer;
import backtype.storm.task.IErrorReporter;
import backtype.storm.task.TopologyContext;
import backtype.storm.generated.*;
import backtype.storm.utils.BufferFileInputStream;
import backtype.storm.utils.Utils;
import backtype.storm.utils.NimbusClient;

import storm.feedback.ranking.CongestionRanker;

public class FeedbackMetricsConsumer implements IMetricsConsumer {
	public static final Logger LOG = LoggerFactory.getLogger(FeedbackMetricsConsumer.class);

	private TopologyContext _context;

	/* For each task, keep track of a window of data points */
	private Map<Integer, DataPointWindow> dpwindow;

	/* How many times a task has sent metrics */
	private Map<Integer, Integer> counter;

	private int windowSize;
	private int lastMinCounter;
	private AlgorithmState algorithmState;

	private boolean isMetricComponent(String component) {
		String metricPrefix = "__metrics";
		return component.length() >= metricPrefix.length()
			&& metricPrefix.equals(component.substring(0, metricPrefix.length()));
	}

	 @Override
    public void prepare(Map stormConf, Object registrationArgument, TopologyContext context, IErrorReporter errorReporter) {
		_context = context;
		algorithmState = createAlgorithmState(stormConf, context, registrationArgument);

		windowSize = 5;
		lastMinCounter = 0;

		// set up data collection
		dpwindow = new HashMap<Integer, DataPointWindow>();
		counter = new HashMap<Integer, Integer>();

		counter.put(-1, 0); // -1 is the __system task
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
		for (ComponentStatistics stats : result.values()) {
			stats.finish(windowSize);
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

	private int componentCounter(String component) {
		int max = 0;
		Map<Integer, String> taskToComponent = _context.getTaskToComponent();
		for (Integer task : taskToComponent.keySet()) {
			if (taskToComponent.get(task) == component) {
				int count = counter.get(task);
				if (count > max) {
					max = count;
				}
			}
		}
		return max;
	}

	private int minComponentCounter() {
		int minCount = -1;
		for (String component : _context.getComponentIds()) {
			int count = componentCounter(component);
			if (minCount < 0 || count < minCount) {
				minCount = count;
			}
		}
		return minCount;
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

		// System.out.println(_context.getTaskToComponent().get(taskId)
		// 				   + "=> " + dataPoints);

		// if (taskId == -1) {
		// 	System.out.println(counter);
		// }

		// When the min counter increases, report new statistics
		int minCounter = minComponentCounter();
		if (minCounter > lastMinCounter && minCounter > windowSize) {
			lastMinCounter = minCounter;
			Map<String, ComponentStatistics> stats = collectStatistics();
			algorithmState.update(getTotalAcks(stats), stats);
		}

		// Update the window
		if (taskId >= 0) {
			dpwindow.get(taskId).putDataPoints(count, dp);
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

	private IFeedbackAlgorithm createAlgorithm(Map stormConf) {
		// TODO: select algorithm based on stormConf
		return new RoundRobin();
		// IFeedbackAlgorithm algorithm = new CombinatorialAlgorithm(new CongestionRanker());
		// IFeedbackAlgorithm algorithm = new GAlgorithm(new CongestionRanker());
		// return new BeamSearchAlgorithm();
	}

	private AlgorithmState createAlgorithmState(Map stormConf, TopologyContext context, Object arg) {
		Map argDict = (Map)arg;
		String name = (String)argDict.get("name");

		// argument is serialized to JSON, so we have to convert Longs into Integers
		Map<String, Long> temp = (Map<String, Long>)argDict.get("parallelism");
		Map<String, Integer> parallelism = new HashMap<String, Integer>();
		for (String key : temp.keySet()) {
			parallelism.put(key, temp.get(key).intValue());
		}

		AlgorithmState state = new AlgorithmState(createAlgorithm(stormConf));
		state.initialize(name, stormConf, context, parallelism);
		return state;
	}

	private static Map<String, Integer> getParallelism(StormTopology topology) {
		Map<String, Integer> configuration = new HashMap<String, Integer>();
		Map<String, Bolt> bolts = topology.get_bolts();
		for(String i : bolts.keySet()) {
			int p = bolts.get(i).get_common().get_parallelism_hint();
			configuration.put(i, p);
		}
		Map<String, SpoutSpec> spouts = topology.get_spouts();
		for(String i : spouts.keySet()) {
			int p = spouts.get(i).get_common().get_parallelism_hint();
			configuration.put(i, p);
		}
		return configuration;
	}

	public static void register(Config conf, String name, StormTopology topology) {
		Map<String, Object> arg = new HashMap<String, Object>();
		arg.put("name", name);
		arg.put("parallelism", getParallelism(topology));
		conf.registerMetricsConsumer(FeedbackMetricsConsumer.class, arg, 1);
		conf.setStatsSampleRate(1);
		conf.put(Config.TOPOLOGY_BUILTIN_METRICS_BUCKET_SIZE_SECS, 1);
		conf.setMaxSpoutPending(8);
	}

	public static void register(Config conf, String name, StormTopology topology,
								ILocalCluster localCluster) {
		register(conf, name, topology);
		AlgorithmState.localCluster = localCluster;
	}
}
