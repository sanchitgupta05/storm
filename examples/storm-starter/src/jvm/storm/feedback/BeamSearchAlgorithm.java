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

import backtype.storm.generated.*;

import storm.feedback.ranking.IRanker;
import storm.feedback.ranking.CongestionRanker;

public class BeamSearchAlgorithm implements IFeedbackAlgorithm {
	private AlgorithmState state;

	public void setState(AlgorithmState state) {
		this.state = state;
	}

	public void load() {
	}

	public void save() {
	}

	public void run(Map<String, ComponentStatistics> statistics) {
		List<Map<String, Integer>> combinations = new ArrayList<Map<String, Integer>>();
		List<String> components = new ArrayList<String>();
		for (String component : state.topologyContext.getComponentIds()) {
			if (!statistics.get(component).isSpout) {
				components.add(component);
			}
		}

		int budget = 0;
		int coresPerWorker = 4;
		ClusterSummary summary = state.getClusterInfo();
		for (TopologySummary topology : summary.get_topologies()) {
			if (topology.get_id().equals(state.topologyContext.getStormId())) {
				budget = coresPerWorker * topology.get_num_workers();
				break;
			}
		}

		double penalty = 0.01;

		boolean isCpuBound = ThroughputModel.isCpuBound(
			state.mean(state.newThroughputs),
			state.topologyContext,
			statistics,
			state.parallelism,
			budget,
			penalty);

		if (isCpuBound) {
			System.out.println("CPU BOUND");
		} else {
			System.out.println("IO BOUND");
		}

		Map<String, Integer> best = null;
		double maxThroughput = 0;

		// for (Map<String, Integer> candidate : combinations) {
		// 	for (String component : topologyContext.getComponentIds()) {
		// 		if (!candidate.containsKey(component)) {
		// 			candidate.put(component, 1);
		// 		}
		// 	}

		// 	double throughput = ThroughputModel.predict(
		// 		isCpuBound,
		// 		topologyContext,
		// 		statistics,
		// 		candidate,
		// 		budget,
		// 		penalty);

		// 	if (throughput > maxThroughput) {
		// 		maxThroughput = throughput;
		// 		best = candidate;
		// 	}
		// 	System.out.format("%s => %f\n", candidate, throughput);
		// }

		IRanker ranker = new CongestionRanker();

		Configuration bestConfiguration = null;

		Map<String, Integer> start = initialState();
		Set<Configuration> membership = new HashSet<Configuration>();
		PriorityQueue<Configuration> queue = new PriorityQueue<Configuration>();
		double startThroughput = ThroughputModel.predict(
			isCpuBound, state.topologyContext, statistics, start, budget, penalty);
		queue.add(new Configuration(start, startThroughput));

		Configuration current = null;
		for (int i=0; i<100; i++) {
			if (queue.size() == 0) {
				break;
			}
			current = queue.poll();
			membership.remove(current);

			System.out.format("%s: %f\n",
							  current.parallelism,
							  current.throughput);

			if (bestConfiguration == null || current.compareTo(bestConfiguration) == -1) {
				bestConfiguration = current;
			}

			List<String> ranked = ranker.rankComponents(
				state.topologyContext, statistics, current.parallelism);
			int k = Math.min(10, ranked.size());

			for (int j=0; j<k; j++) {
				Map<String, Integer> next = new HashMap<String, Integer>(current.parallelism);
				String component = ranked.get(j);
				next.put(component, next.get(component) + 1);

				int numTasks = state.topologyContext.getComponentTasks(component).size();
				if (next.get(component) > numTasks)
					continue;

				double throughput = ThroughputModel.predict(
					isCpuBound, state.topologyContext, statistics, next, budget, penalty);
				Configuration c = new Configuration(next, throughput);
				if (!membership.contains(c)) {
					queue.add(c);
					membership.add(c);
				}
			}
		}

		System.out.println("bs throughput: " + bestConfiguration.throughput);
		System.out.println("bs output: " + bestConfiguration.parallelism);

		// System.out.println("best: " + best);
		// System.out.println("maxThroughput: " + maxThroughput);

		if (!state.parallelism.equals(bestConfiguration.parallelism)) {
			state.parallelism = bestConfiguration.parallelism;
			state.rebalance();
		}
	}

	private Map<String, Integer> initialState() {
		Map<String, Integer> result = new HashMap<String, Integer>();
		Set<String> components = state.topologyContext.getComponentIds();
		for (String component : components) {
			result.put(component, 1);
		}
		return result;
	}

	private void getCombinations(List<String> components, Map<String, Integer> current, List<Map<String, Integer>> out) {
		for (String component : components) {
			if (current == null || !current.containsKey(component)) {
				int n = state.topologyContext.getComponentTasks(component).size();
				for (int i=1; i<=n; i++) {
					Map<String, Integer> next = null;
					if (current == null) {
						next = new HashMap<String, Integer>();
					} else {
						next = new HashMap<String, Integer>(current);
					}
					next.put(component, i);
					getCombinations(components, next, out);
				}
				return;
			}
		}
		out.add(current);
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
	}
}
