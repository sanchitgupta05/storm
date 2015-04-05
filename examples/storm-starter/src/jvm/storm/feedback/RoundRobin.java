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


public class RoundRobin implements IFeedbackAlgorithm {

	private static ILocalCluster localCluster;
	private TopologyContext _context;
	static Integer MAX_PARALLELISM_HINT = 10;
	private static Integer DESIRED_ACKS_PER_SECONDS = 3000 ;
	static LastAction _lastAction;
	double currThroughput;
	Integer numWindowsToPass;
	Integer numBottlenecksToFix;
	Integer numAlgorithmRun;

	public static String localTopologyName;

	boolean prepared;
	boolean waitingForRebalance;

	/* A Queue of all the components in the Topology */
	Queue<String> componentsQueue;

	/* Maps Receive Queue Length to Component*/
	HashMap<Double, String> mapReceiveQueueLengthToComponents;

	/* Maps the parallelism Hint per Component*/
	HashMap<String, Integer> mapTaskParallel;

	/* For each Component, keep track of the lastAction taken by the algorithm*/
	HashMap<String, LastAction> mapLastAction;

	public boolean isPrepared() {
		return prepared;
	}

	public void initialize(ILocalCluster cluster, String name, StormTopology topology) {
		localCluster = cluster;
		currThroughput = 0;
		numWindowsToPass = 0;
		numAlgorithmRun = 0;
		waitingForRebalance = false;

		mapLastAction = new HashMap<String, LastAction>();
		_lastAction = new LastAction();
		componentsQueue = new LinkedList<String>();
		mapTaskParallel = new HashMap<String, Integer>();

		localTopologyName = name;

		numBottlenecksToFix = 1;
		_last_acks = 0;
		_last_parallel = 0;
		_last_comp = "";

		getParallelismHint( topology);
	}

	public void update(double acksPerSecond, Map<String, ComponentStatistics> statistics) {
		currThroughput = acksPerSecond;

		if(prepared)
			runAlgorithm();
	}

	public void prepare(Map stormConf, TopologyContext context) {
		_context = context;
		getComponentsOfTopology();
		prepared = true;
	}

	public void onRebalance() {
		waitingForRebalance = false;
	}

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
		if (waitingForRebalance) {
			return;
		}

		numWindowsToPass++;

		if(currThroughput != 0 && numWindowsToPass > 10
		 && currThroughput < DESIRED_ACKS_PER_SECONDS) {
			__algorithm();
			numWindowsToPass = 0;
			numAlgorithmRun++;

			System.out.println("ALGORITHM RUNNING");

			if(numAlgorithmRun >= componentsQueue.size()) {
				numBottlenecksToFix++;
				numAlgorithmRun = 0;
			}
		} else {
			System.out.println("ALGORITHM PASSING");
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
			System.out.println("OLD ACTION REVERT");
			/* revert to last Action */
			component = _lastAction.component;
			taskParallelHint = _lastAction.oldParallelismHint;

		} else {
			_lastAction.updateAction(_last_comp, _last_parallel, _last_acks, _last_startABolt);
			component = componentsQueue.poll();
			taskParallelHint = mapTaskParallel.get(component);

			if(taskParallelHint < MAX_PARALLELISM_HINT)  {
				System.out.println("INCREASING parallelism hint");
				System.out.println("old parallelism: " + mapTaskParallel);
				mapTaskParallel.put(component, ++taskParallelHint);	// updated the new parallelhint for the component in hashmap
				System.out.println("new parallelism: " + mapTaskParallel);
				_last_comp = component;
				_last_parallel = taskParallelHint;
				_last_acks = currThroughput;
				// _last_startABolt = false;
			} else {
				/* TODO ROHIT: Add a parallel bolt on a new node*/

				/* update the insertion of the newly added bolt to all data structures*/
				// _last_startABolt = true;
			}
			componentsQueue.add(component);
		}

		rebalance();
	}

	private void rebalance() {
		waitingForRebalance = true;

		RebalanceOptions options = new RebalanceOptions();
		options.set_wait_secs(0);
		options.set_num_executors(mapTaskParallel);
		try {
			localCluster.rebalance(localTopologyName, options);
			System.out.println("REBALANCING: " + mapTaskParallel);
		} catch (Exception e) {
			// like I give a fuck
		}
	}

	private void getComponentsOfTopology() {
		for (int i=0; i<_context.getTaskToComponent().size(); i++) {
			String component = _context.getTaskToComponent().get(i);
			if (component != null) {
				if (!component.substring(0, 2).equals("__")) {
					componentsQueue.add(component);
					mapTaskParallel.put(component, 1);
				}
			}
		}
	}


	private void getParallelismHint(StormTopology topology) {
		Map<String, Bolt> bolts = topology.get_bolts();
		Map<String, SpoutSpec> spouts = topology.get_spouts();

		for(String i : bolts.keySet()) {
			mapTaskParallel.put(i,
				bolts.get(i).get_common().get_parallelism_hint());
		}
		for(String i : spouts.keySet()) {
			mapTaskParallel.put(i,
				spouts.get(i).get_common().get_parallelism_hint());
		}
	}
}
