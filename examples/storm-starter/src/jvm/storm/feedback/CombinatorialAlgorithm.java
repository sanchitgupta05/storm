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


public class CombinatorialAlgorithm extends BaseFeedbackAlgorithm {
	
	private static Integer DESIRED_ACKS_PER_SECONDS = 3000 ;
	private static Integer MAX_PARALLELISM_HINT = 10;

	private ILocalCluster localCluster;
	private String localTopologyName;

	private TopologyContext _context;
	private Integer numIterationOfAlgorithm;
	
	/* Maps Components to the Last Action*/
	private HashMap<String, LastAction> mapComponentToLastAction;
	private HashMap<String, LastAction> _bufferMapComponentToLastAction;

	/* Maps the parallelism Hint per Component*/
	private HashMap<String, Integer> mapTaskParallel;
	
	/* Maps congestion to the component */
	private HashMap<Double, String> mapCongestionToComponent;

	private int counter;

	@Override
	public void initialize(ILocalCluster cluster, String name, StormTopology topology) {
		super.initialize(cluster, name,topology);
		
		localCluster = cluster;
		numIterationOfAlgorithm = 0;
		localTopologyName = name;

		counter = 0;

		mapComponentToLastAction = new HashMap<String, LastAction>();
		_bufferMapComponentToLastAction = new HashMap<String, LastAction>();
		mapTaskParallel = new HashMap<String, Integer>();
		mapCongestionToComponent = new HashMap<Double, String>();

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

		int numOfIterations = (int)Math.ceil(++numIterationOfAlgorithm/(double)mapTaskParallel.size());
		
		if(numOfIterations > mapTaskParallel.size()) {
			numOfIterations = 1;
			numIterationOfAlgorithm = 0;
		}

		if(acksPerSecond < DESIRED_ACKS_PER_SECONDS &&
				counter >= 10) {		
			__algorithm(numOfIterations, acksPerSecond, statistics);
			counter = 0;
		}	
	}

	private void __algorithm(int numRunAlgorithm, double acksPerSecond, Map<String, 
									ComponentStatistics> statistics) {

		if(throughputIncreased() == true) {
			// can safely append the buffered lastAction
			mapComponentToLastAction.clear();
			if(!_bufferMapComponentToLastAction.isEmpty()) 
				mapComponentToLastAction.putAll(_bufferMapComponentToLastAction);

		} else if(!_bufferMapComponentToLastAction.isEmpty()) {
			// get the fuck back to the prev config
			for(String comp : _bufferMapComponentToLastAction.keySet()) {
				mapTaskParallel.put(comp, 
						_bufferMapComponentToLastAction.get(comp).oldParallelismHint - 1);
			}
			_bufferMapComponentToLastAction.clear();
			numRunAlgorithm = 0;		// cannot run algorithm anymore
		} else {
			return;
		}

		int taskParallel = 0; 
		double maxCongestion = 0;
		String component;
		
		/* get the mapping from Queue length to Component */
		for(String kk : statistics.keySet()) {
			mapCongestionToComponent.put((double)(statistics.get(kk).receiveQueueLength
													+Math.random()), kk);
		}

		for(int i = numRunAlgorithm; i > 0; i--) {
			for(double k : mapCongestionToComponent.keySet()) {
				if(k > maxCongestion) {
					maxCongestion = k;
				}
			}
			component = mapCongestionToComponent.remove(maxCongestion);
			taskParallel =  mapTaskParallel.get(component);
			mapTaskParallel.put(component, taskParallel+1);
			
			// create a buffer last action for this
			LastAction _lastAction = new LastAction();
			_lastAction.updateAction(component, taskParallel+1, acksPerSecond, false);
			_bufferMapComponentToLastAction.put(component, _lastAction);
		}

		rebalance(mapTaskParallel);
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

	private void getComponentsOfTopology() {
		for (int i=0; i<_context.getTaskToComponent().size(); i++) {
			String component = _context.getTaskToComponent().get(i);
			if (component != null) {
				if (!component.substring(0, 2).equals("__")) {
					mapTaskParallel.put(component, 1);
				}
			}
		}
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

}


