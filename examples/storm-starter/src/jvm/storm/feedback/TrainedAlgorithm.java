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

import java.util.PriorityQueue;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.Random;
import java.util.Arrays;

import backtype.storm.generated.*;

import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;

public class TrainedAlgorithm implements IFeedbackAlgorithm {
	private AlgorithmState state;
	private List<Map<String, Integer>> parallelismHistory;
	private List<Double> throughputHistory;
	private List<Map<String, ComponentStatistics>> statisticsHistory;

	private int numTraining;

	public TrainedAlgorithm(int numTraining) {
		this.numTraining = numTraining;
	}

	public void setState(AlgorithmState state) {
		this.state = state;
	}

	public void load() {
		parallelismHistory = (List<Map<String, Integer>>) state.loadObject("parallelismHistory");
		throughputHistory = (List<Double>) state.loadObject("throughputHistory");
		statisticsHistory = (List<Map<String, ComponentStatistics>>)
			state.loadObject("statisticsHistory");
	}

	public void save() {
		state.saveObject("parallelismHistory", parallelismHistory);
		state.saveObject("throughputHistory", throughputHistory);
		state.saveObject("statisticsHistory", statisticsHistory);
	}

	public void run(Map<String, ComponentStatistics> statistics) {
		if (parallelismHistory == null) {
			parallelismHistory = new ArrayList<Map<String, Integer>>();
			throughputHistory = new ArrayList<Double>();
			statisticsHistory = new ArrayList<Map<String, ComponentStatistics>>();
		}

		// Add the latest statistics
		parallelismHistory.add(state.parallelism);
		throughputHistory.add(state.newThroughput);
		statisticsHistory.add(statistics);

		int n = parallelismHistory.size();
		if (n < numTraining) {
			// Still collecting data, rebalance
			Map<String, Integer> next = new HashMap<String, Integer>();
			for (String component : state.topologyContext.getComponentIds()) {
				if (statistics.get(component).isSpout) {
					next.put(component, 1);
				} else {
					int numTasks = state.topologyContext.getComponentTasks(component).size();
					next.put(component, 1 + (int)(Math.random() * (numTasks-1)));
				}
			}
			state.parallelism = next;
			state.rebalance();
		}

		if (n >= numTraining) {
			// Get number of worker processes for this topology
			int numWorkers = 1;
			ClusterSummary summary = state.getClusterInfo();
			for (TopologySummary topology : summary.get_topologies()) {
				if (topology.get_id().equals(state.topologyContext.getStormId())) {
					numWorkers = topology.get_num_workers();
				}
			}

			// Train the model, and do one final rebalance
			double bestScore = Double.NEGATIVE_INFINITY;
			boolean bestCpuBound = true;
			int bestNumCores = 0;
			double bestPenalty = 0;
			int bestStatisticsIndex = 0;

			// Search through parameter space
			double alpha = Math.pow(2, 1.0/25);
			for (int i=0; i<2; i++) {
				boolean cpuBound = (i == 0);
				for (int c=1; c<=4; c++) {
					for (int j=0; j<25; j++) {
						double penalty = Math.pow(alpha, j) - 1;
						for (int k=0; k<n; k++) {
							double score = getScore(
								statisticsHistory.get(k),
								cpuBound,
								numWorkers * c,
								penalty);
							if (!Double.isNaN(score)
								&& score > bestScore) {
								bestScore = score;
								bestCpuBound = cpuBound;
								bestNumCores = c;
								bestPenalty = penalty;
								bestStatisticsIndex = k;
							}
						}
					}
				}
			}

			EmailWrapper.log("correlation", bestScore);
			EmailWrapper.log("cpuBound", bestCpuBound);
			EmailWrapper.log("penalty", bestPenalty);
			EmailWrapper.log("cores", bestNumCores);

			System.out.format("bestScore: %s\n", bestScore);
			System.out.format("bestCpuBound: %s\n", bestCpuBound);
			System.out.format("bestPenalty: %s\n", bestPenalty);
			System.out.format("bestNumCores: %s\n", bestNumCores);
			System.out.format("bestStatisticsIndex: %s\n", bestStatisticsIndex);

			Map<String, Integer> next = getBestConfiguration(
				statisticsHistory.get(bestStatisticsIndex),
				bestCpuBound,
				numWorkers * bestNumCores,
				bestPenalty);
			if (!next.equals(state.parallelism)) {
				state.parallelism = next;
				state.rebalance();
			}
		}
	}

	private double getScore(Map<String, ComponentStatistics> statistics,
							boolean isCpuBound,
							int numWorkers,
							double penalty) {
		int n = parallelismHistory.size();
		double[] actual = new double[n];
		double[] predicted = new double[n];
		List<Double> predictedList = new ArrayList<Double>();

		for (int i=0; i<n; i++) {
			double throughput = ThroughputModel.predict(
				isCpuBound,
				state.topologyContext,
				statistics,
				parallelismHistory.get(i),
				numWorkers,
				penalty);
			predicted[i] = throughput;
			actual[i] = throughputHistory.get(i);
			predictedList.add(throughput);
		}

		double ratio = state.mean(throughputHistory)
			/ state.mean(predictedList);
		for (int i=0; i<n; i++) {
			// predicted.set(i, predicted.get(i) * ratio);
			predicted[i] *= ratio;
		}

		double score = (new PearsonsCorrelation()).correlation(actual, predicted);

		double diffsum = 0;
		int diffcount = 0;
		for (int i=0; i<n; i++) {
			diffsum += Math.abs(actual[i] - predicted[i]) / actual[i];
			diffcount++;
		}
		double alpha = 0.5;
		score -= alpha * (diffsum / diffcount);

		// double avg1 = state.mean(throughputHistory);
		// double avg2 = state.mean(predicted);
		// double covariance = 0;
		// for (int i=0; i<n; i++) {
		// 	double d1 = (throughputHistory.get(i) - avg1);
		// 	double d2 = (predicted.get(i) - avg2);
		// 	covariance += d1 * d1 + d2 * d2;
		// }

		System.out.println("actual: " + throughputHistory);
		System.out.println("predicted: " + Arrays.toString(predicted));
		System.out.println("correlation - avg pdiff: " + score);
		// System.out.println("scaled l1 norm: " + score);
		// System.out.println("scaled covariance: " + covariance);

		return score;
	}

	private Map<String, Integer> getBestConfiguration(
		Map<String, ComponentStatistics> statistics,
		boolean isCpuBound,
		int numWorkers,
		double penalty) {

		List<String> components = new ArrayList<String>();
		for (String component : state.topologyContext.getComponentIds()) {
			if (!statistics.get(component).isSpout) {
				components.add(component);
			}
		}

		Configuration bestConfiguration = null;

		Map<String, Integer> start = initialState();
		Set<Configuration> membership = new HashSet<Configuration>();
		PriorityQueue<Configuration> queue = new PriorityQueue<Configuration>();
		double startThroughput = ThroughputModel.predict(
			isCpuBound, state.topologyContext, statistics, start, numWorkers, penalty);
		queue.add(new Configuration(start, startThroughput));

		Configuration current = null;
		for (int i=0; i<100; i++) {
			if (queue.size() == 0) {
				break;
			}
			current = queue.poll();
			membership.remove(current);

			if (bestConfiguration == null || current.compareTo(bestConfiguration) == -1) {
				bestConfiguration = current;
			}

			for (String component : components) {
				Map<String, Integer> next = new HashMap<String, Integer>(current.parallelism);
				next.put(component, next.get(component) + 1);

				int numTasks = state.topologyContext.getComponentTasks(component).size();
				if (next.get(component) > numTasks)
					continue;

				double throughput = ThroughputModel.predict(
					isCpuBound, state.topologyContext, statistics, next, numWorkers, penalty);
				Configuration c = new Configuration(next, throughput);
				if (!membership.contains(c)) {
					queue.add(c);
					membership.add(c);
				}
			}
		}

		System.out.println("penalty: " + penalty);
		System.out.println("best: " + bestConfiguration);

		EmailWrapper.log("penalty", penalty);
		EmailWrapper.log("bsBestParallelism", bestConfiguration.parallelism);
		EmailWrapper.log("bsBestThroughput", bestConfiguration.throughput);

		return bestConfiguration.parallelism;
	}


	private Map<String, Integer> initialState() {
		Map<String, Integer> result = new HashMap<String, Integer>();
		Set<String> components = state.topologyContext.getComponentIds();
		for (String component : components) {
			result.put(component, 1);
		}
		return result;
	}

	class Configuration implements Comparable<Configuration> {
		public Map<String, Integer> parallelism;
		public double throughput;

		public Configuration(Map<String, Integer> parallelism, double throughput) {
			this.parallelism = parallelism;
			this.throughput = throughput;
		}

		public int compareTo(Configuration other) {
			if (throughput == other.throughput)
				return 0;
			return (throughput > other.throughput ? -1 : 1);
		}

		public String toString() {
			return String.format("Configuration(%s, %f)", parallelism, throughput);
		}
	}

}
