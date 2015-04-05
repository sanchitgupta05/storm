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

import java.util.List;
import java.util.ArrayList;
import java.util.Map;

import org.apache.commons.math3.distribution.NormalDistribution;

import backtype.storm.ILocalCluster;
import backtype.storm.task.TopologyContext;
import backtype.storm.generated.*;

public abstract class BaseFeedbackAlgorithm implements IFeedbackAlgorithm {
	public static final Logger LOG = LoggerFactory.getLogger(BaseFeedbackAlgorithm.class);

	private ILocalCluster localCluster;
	private String localTopologyName;
	private StormTopology localTopology;
	private boolean prepared;
	private boolean waitingForRebalance;
	private List<Double> oldThroughputs;
	private List<Double> newThroughputs;

	private double mean(List<Double> a) {
		double sum = 0;
		for (Double val : a) {
			sum += val;
		}
		return sum / a.size();
	}

	// Model "a" with a normal distribution, and test whether cdf(mean(b)) > 0.95
	private boolean significantIncrease(List<Double> a, List<Double> b) {
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

	protected boolean throughputIncreased() {
		return oldThroughputs == null
			|| significantIncrease(oldThroughputs, newThroughputs);
	}

	@Override
	public void initialize(ILocalCluster cluster, String name, StormTopology topology) {
		localCluster = cluster;
		localTopologyName = name;
		localTopology = topology;

		LOG.info("parallelism rebalance " + System.currentTimeMillis());
		Map<String, Bolt> bolts = topology.get_bolts();
		for(String i : bolts.keySet()) {
			int p = bolts.get(i).get_common().get_parallelism_hint();
			LOG.info("parallelism " + i + " " + p);
		}
		Map<String, SpoutSpec> spouts = topology.get_spouts();
		for(String i : spouts.keySet()) {
			int p = spouts.get(i).get_common().get_parallelism_hint();
			LOG.info("parallelism " + i + " " + p);
		}
	}

	public boolean isPrepared() {
		return prepared;
	}

	public void prepare(Map stormConf, TopologyContext context) {
		waitingForRebalance = false;
		oldThroughputs = null;
		newThroughputs = new ArrayList<Double>();
		prepared = true;
	}

	public void onRebalance() {
		waitingForRebalance = false;
		oldThroughputs = newThroughputs;
		newThroughputs = new ArrayList<Double>();
	}

	public void update(double acksPerSecond, Map<String, ComponentStatistics> statistics) {
		if (isPrepared() && !waitingForRebalance) {
			LOG.info("acksPerSecond " + acksPerSecond + " " + System.currentTimeMillis());
			LOG.info("cpuUsage " + Util.getProcessCpuLoad() + " " + System.currentTimeMillis());
			newThroughputs.add(acksPerSecond);
			runAlgorithm(acksPerSecond, statistics);
		}
	}

	protected void rebalance(Map<String, Integer> parallelismHints) {
		waitingForRebalance = true;

		LOG.info("parallelism rebalance " + System.currentTimeMillis());
		for (String component : parallelismHints.keySet()) {
			LOG.info("parallelism " + component + " " + parallelismHints.get(component));
		}

		RebalanceOptions options = new RebalanceOptions();
		// options.set_wait_secs(15);
		options.set_num_executors(parallelismHints);
		try {
			localCluster.rebalance(localTopologyName, options);
			System.out.println("REBALANCING: " + parallelismHints);
		} catch (Exception e) {
			System.out.println("EXCEPTION DETECTED!!!!!" + e.toString());
		}
	}

	protected abstract void runAlgorithm(double acksPerSecond, Map<String, ComponentStatistics> statistics);
}
