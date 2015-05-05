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
import java.util.Set;
import java.util.HashSet;

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

import storm.feedback.ranking.IRanker;

// public class GAlgorithm extends IterativeFeedbackAlgorithm {
// 	private IRanker ranker;
// 	private static final double CONG_RATIO = 0.20;
// 	private static final int MAX_STEP = 10;
// 	private static final double MAX_LATENCY = -999999;
// 	private static final double INIT_TEMP = 10;

// 	public GAlgorithm(IRanker r) { ranker = r; }

// 	public Map<String, Integer> getActions(Map<String, ComponentStatistics> stats){
// 		Map<String,ComponentData> componentToAlgoData =
// 									new HashMap<String, ComponentData>();
// 		BuildGraph graph = new BuildGraph(topologyContext.getRawTopology());

// 		System.out.println("the graph adj list: " + graph.adjList);
// 		System.out.println("the graph REV adj list: " + graph.revAdjList);


// 		// Setup ComponentData structure
// 		for(String componentID: stats.keySet()) {
// 		//	if(stats.get(componentID).isSpout == true)
// 		//		continue;
// 			double inp_rate = 0;
// 			try {
// 				for(String parents: graph.revAdjList.get(componentID))
// 				inp_rate += stats.get(parents).emitCount;
// 			} catch(Exception e) {
// 				System.out.println("EXCEPTION CAUGHT: " + e);
// 			}
// 			try {
// 				inp_rate /= (double)parallelism.get(componentID);
// 			} catch(Exception e) {
// 				System.out.println("EXCEPTION CAUGHT: " + e);
// 				inp_rate = 0;
// 			}
// 			ComponentStatistics s =	stats.get(componentID);

// 			// XXX
// 			double avgLatencyAtComponent = (s.receiveLatency
// 														+s.sendLatency
// 														+s.executeLatency);
// 			boolean isCongested = false;
// 			double congestionRate = (Math.abs(s.emitCount - inp_rate)/s.emitCount);
// 			if( congestionRate > CONG_RATIO) {
// 				isCongested = true;
// 			}
// 			ComponentData c = new ComponentData(parallelism.get(componentID),
// 															avgLatencyAtComponent,
// 															s.executeLatency/s.executeCount,
// 															inp_rate, s.emitCount, isCongested,
// 															congestionRate);
// 			componentToAlgoData.put(componentID, c);
// 		}

// 		// Simulated Annealing Algorithm
// 		Map<String, Integer> bestAssignment = parallelism;
// 		double bestMaxLatency = MAX_LATENCY;
// 		for(int i = 1; i >= 0; --i) {
// 			Map<String, Integer> currAssignment = initState();
// 			Map<String, ComponentData> compData = componentToAlgoData;
// 			double currMaxLatency = computeMaxLatency(graph, compData);
// 			double t = INIT_TEMP;

// 			for(int j = 0; j < MAX_STEP; ++j) {
// 				Map<String, Integer> newAssignment = nextState(currAssignment, compData);
// 				double newMaxLatency = computeMaxLatency(graph, compData);
// 				double r = Math.random();

// 				if(transition(newMaxLatency, currMaxLatency,t) > r) {
// 					currAssignment = newAssignment;
// 					currMaxLatency = newMaxLatency;
// 					// update the latencies and measurements of othernodes


// 				}
// 				if(currMaxLatency >= bestMaxLatency) {
// 					bestMaxLatency = currMaxLatency;
// 					bestAssignment = currAssignment;
// 				}
// 				t = Math.pow(t,0.90);
// 			}
// 		}
// 		return bestAssignment;
// 	}

// 	private Map<String, Integer> initState() {
// 		return parallelism;	// TODO can maybe choose a better startis state than the given user state
// 	}

// 	private Map<String, Integer> nextState(Map<String, Integer> currAssignment,
// 														Map<String, ComponentData> compData) {
// 		Map<String, Integer> newAssignment = new HashMap<String, Integer>();
// 		newAssignment.putAll(currAssignment);
// 		String congestedNode = heavyCongestedNode(compData);

// 		if(congestedNode == null) {
// 			System.out.println("THERE WASN't A CONGESTED NODE FOUND !! -- PICKING A RANDOM NODE FOR BETTERMENT");
// 			congestedNode = (String)currAssignment.keySet().toArray()[0];
// 		}

// 		// incerease the parallelism hint of that node by a congestion ratio
// 		double inputRate = compData.get(congestedNode).inputRate;
// 		double outputRate = compData.get(congestedNode).outputRate;
// 		int newParallelismHint = ((int)(Math.ceil((double)(inputRate/outputRate)))*currAssignment.get(congestedNode));
// 		newAssignment.put(congestedNode, newParallelismHint);

// 		return newAssignment;
// 	}

// 	private String heavyCongestedNode(Map<String, ComponentData> compData) {
// 		double maxCongestionRate = 0;
// 		String maxCongestedNode = null;
// 		for(String comp: compData.keySet()) {
// 			if(compData.get(comp).congestionRate > maxCongestionRate)
// 				maxCongestedNode = comp;
// 		}
// 		return maxCongestedNode;
// 	}

// 	private double computeMaxLatency(BuildGraph graph, Map<String, ComponentData> compData) {
// 		String rootNode = null;
// 		for(String comp: graph.adjList.keySet()) {
// 			if(graph.revAdjList.containsKey(comp) == false) {
// 				rootNode = comp;
// 				break;
// 			}
// 		}
// 		double maxLatency = maxLatencyHelper(rootNode, graph, compData);
// 		return maxLatency*(-1); // we negate because we want to minimize the largest latency
// 	}

// 	private double maxLatencyHelper(String root, BuildGraph graph, Map<String, ComponentData> compData) {
// 		if(root == null) {
// 			System.out.println("NO ROOT FOUND FOR CALCULATING MAX LATENCY !!!! returning");
// 			return 0;
// 		}

// 		double childMaxLatency = 0;
// 		for(String comp : graph.adjList.get(root)) {
// 			double temp = maxLatencyHelper(comp ,graph, compData);
// 			childMaxLatency = Math.max(childMaxLatency, temp);
// 		}
// 		ComponentData c = compData.get(root);
// 		double myLatency = c.avgExecutionLatencyPerTuple + (double)(1/c.outputRate);
// 		return (childMaxLatency + myLatency);
// 	}

// 	private double transition(double oldUtil, double newUtil, double temperature) {
// 		if(newUtil > oldUtil) return 1.0;
// 		else return Math.pow(Math.E, (oldUtil-newUtil)/temperature); // the latencies have been negated
// 	}


// 	private class BuildGraph {
// 		public Map<String, List<String>> adjList;
// 		public Map<String, List<String>> revAdjList;

// 		public BuildGraph(StormTopology topology) {
// 			adjList = new HashMap<String, List<String>>();
// 			revAdjList = new HashMap<String, List<String>>();
// 			Map<String, Bolt> bolts = topology.get_bolts();
// 			Map<String, SpoutSpec> spouts = topology.get_spouts();

// 			// All edges between Bolts
// 			for(String boltId: bolts.keySet()) {

// 				System.out.println("BUILDING GRAPH: boldID: " + boltId);

// 				Map<GlobalStreamId,Grouping> inputs =
// 					bolts.get(boltId).get_common().get_inputs();

// 				for(GlobalStreamId from: inputs.keySet()) {
// 					String inputId = from.get_componentId();
// 					if(adjList.containsKey(inputId) == false) {
// 						List<String> tmp = new ArrayList<String>();
// 						tmp.add(boltId);
// 						adjList.put(inputId, tmp);
// 					}
// 					else {
// 						adjList.get(inputId).add(boltId);
// 					}

// 					if(revAdjList.containsKey(boltId) == false) {
// 						List<String> tmp2 = new ArrayList<String>();
// 						tmp2.add(inputId);
// 						revAdjList.put(boltId,tmp2);
// 					}
// 					else
// 						revAdjList.get(boltId).add(inputId);
// 				}
// 			}

// 		// All edges between Spouts
// 			for(String spoutId: spouts.keySet()) {
// 				Map<GlobalStreamId,Grouping> inputs =
// 					spouts.get(spoutId).get_common().get_inputs();

// 				for(GlobalStreamId from: inputs.keySet()) {
// 					String inputId = from.get_componentId();
// 					if(adjList.containsKey(inputId) == false) {
// 						List<String> tmp = new ArrayList<String>();
// 						tmp.add(spoutId);
// 						adjList.put(inputId, tmp);
// 					}
// 					else {
// 						adjList.get(inputId).add(spoutId);
// 					}

// 					if(revAdjList.containsKey(spoutId) == false) {
// 						List<String> tmp2 = new ArrayList<String>();
// 						tmp2.add(inputId);
// 						revAdjList.put(spoutId,tmp2);
// 					}
// 					else
// 						revAdjList.get(spoutId).add(inputId);
// 				}
// 			}

// 		}
// 	}

// 	private class ComponentData {
// 		public int pHint;
// 		public double avgLatencyAtComponent;
// 		public double avgExecutionLatencyPerTuple; // constant
// 		public double inputRate;
// 		public double outputRate;
// 		public boolean congested;
// 		public double congestionRate;

// 		public ComponentData(int pHint,
// 							double avgLatencyAtComponent,
// 							double avgExecutionLatencyPerTuple,
// 							double inputRate,
// 							double outputRate,
// 							boolean congested,
// 							double congestionRate) {
// 			this.pHint = pHint;
// 			this.avgLatencyAtComponent = avgLatencyAtComponent;
// 			this.avgExecutionLatencyPerTuple = avgExecutionLatencyPerTuple;
// 			this.inputRate = inputRate;
// 			this.outputRate = outputRate;
// 			this.congested = congested;
// 			this.congestionRate = congestionRate;
// 			}
// 	}

// 	public List<Set<String>> run(Map<String, ComponentStatistics> statistics) {
// 		return new ArrayList<Set<String>>();
// 	}
// }
