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
	private Map localStormConf;
	public static final Logger LOG = LoggerFactory.getLogger(FeedbackMetricsConsumer.class);
	private TopologyContext _context;

	static Integer MAX_PARALLELISM_HINT = 5;
	private static Integer DESIRED_ACKS_PER_SECONDS = 3000 ;
	static LastAction _lastAction;
	double currThroughput;
	Integer numWindowsToPass;
	Integer numBottlenecksToFix;
	Integer numAlgorithmRun;

	/* A Queue of all the components in the Topology */
	Queue<String> componentsQueue;

	/* Maps Receive Queue Length to Component*/
	HashMap<Long, String> mapReceiveQueueLengthToComponents;

	/* Maps the parallelism Hint per Component*/
	HashMap<String, Integer> mapTaskParallel;

	/* For each Component, keep track of the lastAction taken by the algorithm*/
	HashMap<String, LastAction> mapLastAction;

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

		windowSize = 5;
		currThroughput = 0;
		numWindowsToPass = 0;
		numAlgorithmRun = 0;

		// set up data collection
		dpwindow = new HashMap<Integer, DataPointWindow>();
		counter = new HashMap<Integer, Integer>();
		mapLastAction = new HashMap<String, LastAction>();
		counter.put(-1, 0);
		for (int i : _context.getTaskToComponent().keySet()) {
			if (!isMetricComponent(_context.getTaskToComponent().get(i))) {
				dpwindow.put(i, new DataPointWindow(windowSize));
				counter.put(i, 0);
			}
		}

		_lastAction = new LastAction();
		componentsQueue = new LinkedList<String>();
		mapTaskParallel = new HashMap<String, Integer>();
		getComponentsOfTopology();
		numBottlenecksToFix = 1;
		_last_acks = 0;
		_last_parallel = 0;
		_last_comp = "";
		mapReceiveQueueLengthToComponents = new HashMap<Long, String>();

		this.localStormConf = stormConf;
      System.out.println("FEEDBACK_CONF: " + this.localStormConf);
      /*NimbusClient client = NimbusClient.getConfiguredClient(stormConf);
		  try {

				client.getClient().getClusterInfo();

		  } catch(AuthorizationException e) {
            LOG.warn("exception: "+e.get_msg());
            // throw e;
        } catch(TException msg) {
            LOG.warn("exception: "+ msg);

		  } finally {
            client.close();
        } */
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

	private void getComponentsOfTopology() {
		for (int i=0; i<_context.getTaskToComponent().size(); i++) {
			String component = _context.getTaskToComponent().get(i);
			if (component != null) {
				componentsQueue.add(component);
				mapTaskParallel.put(component, 1);
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

	List<Double> record = new ArrayList<Double>();

	public void printStatistics(Map<String, ComponentStatistics> statistics) {
		long totalAcks = getTotalAcks(statistics);
		double elapsedTime = windowSize;
		mapReceiveQueueLengthToComponents.clear();
		currThroughput = totalAcks/elapsedTime;

		record.add((double)totalAcks / elapsedTime);
		if (record.size() > 5) {
			// hard coded data for testing
			Double[] sample = new Double[] {
				945.6,
				1011.4,
				950.8,
				1013.6,
				1013.6
			};
			significantIncrease(
				Arrays.asList(sample),
				record.subList(record.size()-5, record.size()));
		}

		System.out.println("FEEDBACK acks/second: " + totalAcks / elapsedTime);

		for (String component : statistics.keySet()) {
			ComponentStatistics stats = statistics.get(component);
			System.out.println(component + ".congestion = " + Math.round(stats.congestion * 100) + "%");

			if (stats.counter > 0) {
				mapReceiveQueueLengthToComponents.put(stats.receiveQueueLength+ ((long)Math.random()), component);
			}
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

		// Every so often, report the statistics & run the algorithm
		if (taskId == -1 && count > windowSize) {
			printStatistics(collectStatistics());
			// runAlgorithm();
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

	// Stores the information about the last action performed by the algorithm
	public class LastAction {
		public String component;
		public Integer oldParallelismHint;
		public double oldAcksPerSecond;
		Boolean oldStartAParallelBolt;

		public  LastAction() {
			component = "";
			oldAcksPerSecond = 0;
			oldParallelismHint = 0;
			oldStartAParallelBolt = false;
		}

		public void updateAction(String comp, Integer parallelHint, double acksPerSecond, Boolean startABolt) {
			component = comp;
			oldParallelismHint = parallelHint;
			oldAcksPerSecond = acksPerSecond;
			oldStartAParallelBolt = startABolt;
		}
	}

	private void runAlgorithm() {
		numWindowsToPass++;

		if(currThroughput != 0 && numWindowsToPass > 5
		 && currThroughput < DESIRED_ACKS_PER_SECONDS) {
			__algorithm2();
			numWindowsToPass = 0;
			numAlgorithmRun++;
			if(numAlgorithmRun >= componentsQueue.size()) {
				numBottlenecksToFix++;
				numAlgorithmRun = 0;
			}
		 }
	}

	String _last_comp;
	Integer _last_parallel;
	double _last_acks;
	Boolean _last_startABolt;

	/* __algorithm() --
	 * A simple Round Robin algorithm that changes either the # of threads/componenet or
	 * adds a new parallel bolt to Storm (if possible)i
	 */
	private void __algorithm() {

		Integer taskParallelHint;
		String component;

		if(currThroughput <= _lastAction.oldAcksPerSecond) {
			/* revert to last Action */
			component = _lastAction.component;
			taskParallelHint = _lastAction.oldParallelismHint;

		} else {
			_lastAction.updateAction(_last_comp, _last_parallel, _last_acks, _last_startABolt);
			component = componentsQueue.poll();
			taskParallelHint = mapTaskParallel.get(component);

			if(taskParallelHint < MAX_PARALLELISM_HINT)  {
				mapTaskParallel.put(component, taskParallelHint++);	// updated the new parallelhint for the component in hashmap
				_last_comp = component;
				_last_parallel = taskParallelHint;
				_last_acks = currThroughput;
				_last_startABolt = false;
			} else {
				/* TODO ROHIT: Add a parallel bolt on a new node*/

				/* update the insertion of the newly added bolt to all data structures*/
				_last_startABolt = true;
			}
			componentsQueue.add(component);
		}
		// TODO ROHIT:  Call rebalance from NimbusClient -- I have updated the new taskParallelHint above

	}

	/* __algorithm2()
	 * Try to fix a combinatorial combination of 'k' largest bottlenecks by
	 * treating the Queue Size as the performance metric.
	 */
	private void __algorithm2() {
		Integer taskParallelHint;
		String component;
		Boolean startABolt = false;
		for(Integer i = 0; i < numBottlenecksToFix; i++) {
			taskParallelHint = 0;
			startABolt = false;

			/* Check for older changes and need to revert */
			if(currThroughput <= _last_acks) {
				if(!mapLastAction.isEmpty()) {
						/* Implement Later  */
				}
			} else {

				mapLastAction.clear();
				long maxVal = 0;
				for(long kk : mapReceiveQueueLengthToComponents.keySet()) {
					if(kk > maxVal)
						maxVal = kk;
				}
				component = mapReceiveQueueLengthToComponents.remove(maxVal);
				taskParallelHint = mapTaskParallel.get(component);
				if(taskParallelHint < MAX_PARALLELISM_HINT) {
					mapTaskParallel.put(component, taskParallelHint++);
				} else {
					/* TODO ROHIT: Add a parallel bolt on a new node for this component*/

					/* update the insertion of the newly added bolt to all needed data structures by an update function */
					startABolt = true;
				}
				LastAction lAction = new LastAction();
				lAction.updateAction(component, taskParallelHint-1 , currThroughput, startABolt);
				mapLastAction.put(component, lAction);
				_last_acks = currThroughput;
			}
			/* TODO ROHIT: Call rebalance from NimbusClient -- I have updated the new taskParallelHint above */
		}
	}
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
	public double congestion;
	public long counter;

	public ComponentStatistics() {
		isSpout = false;
		ackCount = 0;
		ackLatency = 0;
		receiveQueueLength = 0;
		sendQueueLength = 0;
		congestion = 0;
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
