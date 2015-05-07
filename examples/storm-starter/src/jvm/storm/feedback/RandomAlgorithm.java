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

public class RandomAlgorithm implements IFeedbackAlgorithm {
	private AlgorithmState state;
	private List<Map<String, Integer>> parallelismHistory;
	private List<Double> throughputHistory;
	private int iterations;

	public RandomAlgorithm(int iterations) {
		this.iterations = iterations;
	}

	public void setState(AlgorithmState state) {
		this.state = state;
	}

	public void load() {
		parallelismHistory = (List<Map<String, Integer>>) state.loadObject("parallelismHistory");
		throughputHistory = (List<Double>) state.loadObject("throughputHistory");
	}

	public void save() {
		state.saveObject("parallelismHistory", parallelismHistory);
		state.saveObject("throughputHistory", throughputHistory);
	}

	public void run(Map<String, ComponentStatistics> statistics) {
		if (state.iteration > iterations) {
			return;
		}

		if (parallelismHistory == null) {
			parallelismHistory = new ArrayList<Map<String, Integer>>();
			throughputHistory = new ArrayList<Double>();
		}

		// Add the latest statistics
		parallelismHistory.add(state.parallelism);
		throughputHistory.add(state.newThroughput);

		int n = parallelismHistory.size();
		if (state.iteration < iterations) {
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
}
