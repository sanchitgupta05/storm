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

import storm.feedback.ranking.IRanker;

import backtype.storm.generated.*;

import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;

public class WalkAlgorithm implements IFeedbackAlgorithm {
	private AlgorithmState state;
	private List<Map<String, Integer>> parallelismHistory;
	private List<Double> throughputHistory;
	private List<Map<String, ComponentStatistics>> statisticsHistory;
	private IRanker ranker;
	private double alpha;
	private int stepSize;
	private int iterations;

	public WalkAlgorithm(int iterations, double alpha, int stepSize, IRanker ranker) {
		this.iterations = iterations;
		this.alpha = alpha;
		this.stepSize = stepSize;
		this.ranker = ranker;
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
		if (state.iteration > iterations) {
			return;
		}

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
		if (state.iteration < iterations) {
			// Take our best configuration
			int rank = sampleRanking(n, alpha);
			int best = getIterationWithRank(rank);
			Map<String, Integer> newParallelism =
				new HashMap<String, Integer>(parallelismHistory.get(best));

			// Choose how many components to increase
			int numComponents=1;
			for (int i=0; i<(stepSize-1)*2; i++) {
				if (Math.random() < 0.5) {
					numComponents++;
				}
			}

			// Choose which components to increase
			List<String> ranking = ranker.rankComponents(
				state.topologyContext,
				statisticsHistory.get(best),
				newParallelism);
			for (int i=0; i<numComponents; i++) {
				String component = ranking.get(
					sampleRanking(ranking.size(), 0.75));
				int numTasks = state.topologyContext.getComponentTasks(
					component).size();
				int p = newParallelism.get(component) + 1;
				if (p < numTasks) {
					newParallelism.put(component, p);
				}
			}

			state.parallelism = newParallelism;
			state.rebalance();
		}

		if (state.iteration == iterations) {
			// Just switch to the optimal configuration seen
			double max = 0;
			Map<String, Integer> best = null;
			for (int i=0; i<parallelismHistory.size(); i++) {
				if (throughputHistory.get(i) > max) {
					max = throughputHistory.get(i);
					best = parallelismHistory.get(i);
				}
			}

			state.parallelism = best;
			state.rebalance();
		}
	}

	private int getIterationWithRank(int k) {
		List<Double> throughputs = new ArrayList<Double>(throughputHistory);
		int best = 0;
		while (k >= 0) {
			double max = 0;
			for (int i=0; i<throughputs.size(); i++) {
				Double t = throughputs.get(i);
				if (t != null && t > max) {
					max = t;
					best = i;
				}
			}
			throughputs.set(best, null);
			k--;
		}
		return best;
	}

	// Sample from a truncated geometric distribution
	private int sampleRanking(int total, double alpha) {
		double sum = 0;
		double[] distribution = new double[total];
		for (int i=0; i<distribution.length; i++) {
			distribution[i] = Math.pow(alpha, i);
			sum += distribution[i];
		}
		for (int i=0; i<distribution.length; i++) {
			distribution[i] /= sum;
		}
		return sampleDiscrete(distribution);
	}

	private int sampleDiscrete(double[] distribution) {
		double[] cumulative = new double[distribution.length];
		cumulative[0] = distribution[0];
		for (int i=1; i<cumulative.length; i++) {
			cumulative[i] = cumulative[i-1] + distribution[i];
		}
		double x = Math.random();
		for (int i=0; i<cumulative.length; i++) {
			if (x < cumulative[i]) {
				return i;
			}
		}
		return -1;
	}
}
