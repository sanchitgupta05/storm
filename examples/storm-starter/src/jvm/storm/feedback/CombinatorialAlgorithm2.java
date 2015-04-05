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


public class CombinatorialAlgorithm2 extends BaseFeedbackAlgorithm {

	private static Integer DESIRED_ACKS_PER_SECOND = 3000 ;
	private static Integer MAX_PARALLELISM_HINT = 10;

	private ILocalCluster localCluster;
	private String localTopologyName;

	private TopologyContext _context;

	/* Maps the parallelism Hint per Component*/
	private HashMap<String, Integer> mapTaskParallel;
	private int counter;
	private boolean reverted;
	private LastAction _action;

	@Override
	public void initialize(ILocalCluster cluster, String name, StormTopology topology) {
		super.initialize(cluster, name,topology);

		localCluster = cluster;
		localTopologyName = name;

		counter = 0;
		mapTaskParallel = new HashMap<String, Integer>();
		getParallelismHint(topology);
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		super.prepare(stormConf, context);

		_context = context;
		getComponentsOfTopology();
	}

	@Override
	protected void runAlgorithm(double acksPerSecond, Map<String, ComponentStatistics> statistics) {
		counter++;

		if (acksPerSecond < DESIRED_ACKS_PER_SECOND
			&& counter >= 10) {
			__algorithm(acksPerSecond, statistics);
			counter = 0;
		}
	}

	private List<String> getSortedComponents(Map<String, ComponentStatistics> statistics) {
		List<String> result = new ArrayList<String>();
		List<String> keys = new ArrayList<String>(mapTaskParallel.keySet());
		for (int i=0; i<mapTaskParallel.size(); i++) {
			double max = 0;
			String maxComponent = null;

			for (String component : keys) {
				double congestion = statistics.get(component).congestion;
				if (congestion >= max) {
					max = congestion;
					maxComponent = component;
				}
			}

			keys.remove(maxComponent);
			result.add(maxComponent);
		}
		return result;
	}

	private void getNextAction(Map<String, ComponentStatistics> statistics) {
		if (_action != null && _action.nextAction()) {
			return;
		}

		if (_action == null) {
			_action = new LastAction(1, getSortedComponents(statistics));
		} else {
			if (_action.k + 1 < statistics.size()) {
				_action = new LastAction(_action.k + 1, getSortedComponents(statistics));
			} else {
				_action = null;
				getNextAction(statistics);
			}
		}
	}

	private void applyNextAction(Map<String, ComponentStatistics> statistics) {
		reverted = false;
		getNextAction(statistics);
		for (String component : _action.getIncreasedComponents()) {
			mapTaskParallel.put(component, mapTaskParallel.get(component) + 1);
		}
	}

	private void revert() {
		reverted = true;
		for (String component : _action.getIncreasedComponents()) {
			mapTaskParallel.put(component, mapTaskParallel.get(component) - 1);
		}
	}

	private void __algorithm(double acksPerSecond,
							 Map<String, ComponentStatistics> statistics) {
		if (reverted) {
			applyNextAction(statistics);
		} else {
			if (throughputIncreased()) {
				_action = null; // accept the action!
				applyNextAction(statistics);
			} else {
				revert();
			}
		}
		rebalance(mapTaskParallel);
	}

	private boolean validComponent(String component) {
		return !component.substring(0, 2).equals("__");
	}

	private void getParallelismHint(StormTopology topology) {
		Map<String, Bolt> bolts = topology.get_bolts();
		Map<String, SpoutSpec> spouts = topology.get_spouts();

		for(String i : bolts.keySet()) {
			if (validComponent(i)) {
				mapTaskParallel.put(i,
									bolts.get(i).get_common().get_parallelism_hint());
			}
		}
		for(String i : spouts.keySet()) {
			if (validComponent(i)) {
				mapTaskParallel.put(i,
									spouts.get(i).get_common().get_parallelism_hint());
			}
		}
	}

	private void getComponentsOfTopology() {
		for (int i=0; i<_context.getTaskToComponent().size(); i++) {
			String component = _context.getTaskToComponent().get(i);
			if (component != null && validComponent(component)) {
				mapTaskParallel.put(component, 1);
			}
		}
	}

	public class LastAction {
		public int k;
		private List<String> components;
		private int[] toIncrease;

		public LastAction(int k, List<String> components) {
			this.k = k;
			this.components = components;
			toIncrease = new int[k];
			for (int i=0; i<k; i++) {
				toIncrease[i] = i;
			}
		}

		private boolean tryIncrement(int i) {
			if (i < 0) {
				return false;
			}

			int next = toIncrease[i] + 1;
			if (next >= components.size()
				|| (i + 1 < toIncrease.length
					&& next >= toIncrease[i+1])) {
				return tryIncrement(i-1);
			} else {
				for (int j=i; j<toIncrease.length; j++) {
					toIncrease[j] = next + (j - i);
				}
				return true;
			}
		}

		public boolean nextAction() {
			return tryIncrement(toIncrease.length - 1);
		}

		public List<String> getIncreasedComponents() {
			ArrayList<String> result = new ArrayList<String>();
			for (int i=0; i<toIncrease.length; i++) {
				result.add(components.get(toIncrease[i]));
			}
			return result;
		}
	}
}
