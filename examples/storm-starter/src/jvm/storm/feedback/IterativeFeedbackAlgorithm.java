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

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;


public abstract class IterativeFeedbackAlgorithm implements IFeedbackAlgorithm {
	protected AlgorithmState state;
	private Map<String, Integer> oldParallelism;
	private List<Double> oldThroughputs;
	private List<Set<String>> history;

	public void setState(AlgorithmState state) {
		this.state = state;
	}

	public void load() {
		oldParallelism = (Map<String, Integer>) state.loadObject("oldParallelism");
		oldThroughputs = (List<Double>) state.loadObject("oldThroughputs");
		history = (List<Set<String>>) state.loadObject("history");
	}

	public void save() {
		state.saveObject("oldParallelism", oldParallelism);
		state.saveObject("oldThroughputs", oldThroughputs);
		state.saveObject("history", history);
	}

	public void run(Map<String, ComponentStatistics> statistics) {
		boolean reverted = (history != null && oldParallelism == null);
		if (reverted) {
			// last action failed, try another
			applyNextAction(statistics);
		} else {
			if (throughputIncreased()) {
				// successfully applied action, clear the history
				history = null;
				applyNextAction(statistics);
			} else {
				revertAction();
			}
		}
	}

	protected boolean throughputIncreased() {
		return oldThroughputs == null
			|| state.significantIncrease(
				oldThroughputs, state.newThroughputs, 0.80);
	}

	private void applyNextAction(Map<String, ComponentStatistics> statistics) {
		if (history == null) {
			history = new ArrayList<Set<String>>();
		}

		// find the first action that isn't in the history
		Set<String> action = null;
		List<Set<String>> actions = getActions(statistics);
		for (int i=0; i<actions.size(); i++) {
			if (!history.contains(actions.get(i))) {
				action = actions.get(i);
				break;
			}
		}

		if (action != null) {
			history.add(action);
			oldThroughputs = state.newThroughputs;
			oldParallelism = new HashMap<String, Integer>(state.parallelism);

			boolean valid = true;
			for (String component : action) {
				int p = oldParallelism.get(component);
				int numTasks = state.topologyContext.getComponentTasks(
					component).size();
				state.parallelism.put(component, p + 1);
				if (p + 1 > numTasks) {
					valid = false;
				}
			}
			if (valid) {
				state.rebalance();
			} else {
				// didn't work, try again
				state.parallelism = oldParallelism;
				oldParallelism = null;
				applyNextAction(statistics);
			}
		}
	}

	private void revertAction() {
		state.parallelism = oldParallelism;
		oldParallelism = null;
		oldThroughputs = null;
		state.rebalance();
	}

	protected abstract List<Set<String>> getActions(Map<String, ComponentStatistics> statistics);
}
