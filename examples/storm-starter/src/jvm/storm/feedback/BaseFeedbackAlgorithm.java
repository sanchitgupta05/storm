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
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;

import java.io.IOException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.storm.curator.framework.CuratorFramework;
import org.apache.storm.zookeeper.data.Stat;
import org.apache.storm.zookeeper.KeeperException;

import backtype.storm.Config;
import backtype.storm.ILocalCluster;
import backtype.storm.task.TopologyContext;
import backtype.storm.generated.*;
import backtype.storm.utils.Utils;
import backtype.storm.utils.ZookeeperAuthInfo;
import backtype.storm.utils.NimbusClient;

public abstract class BaseFeedbackAlgorithm implements IFeedbackAlgorithm {
	public static final Logger LOG = LoggerFactory.getLogger(BaseFeedbackAlgorithm.class);

	// If this is set, don't connect to Nimbus
	public static ILocalCluster localCluster;

	private String basePath;
	private CuratorFramework zookeeper;

	// These are stored in zookeeper
	protected Map<String, Integer> parallelism;
	protected Map<String, Integer> oldParallelism;
	protected List<Double> oldThroughputs;
	protected List<Set<String>> history;

	protected Map stormConf;
	protected TopologyContext topologyContext;
	private String topologyName;
	private Map<String, Integer> startingParallelism;

	protected List<Double> newThroughputs;
	private int updateCounter;

	@Override
	public void initialize(String topologyName, Map stormConf,
						   TopologyContext topologyContext,
						   Map<String, Integer> startingParallelism) {
		this.topologyName = topologyName;
		this.stormConf = stormConf;
		this.topologyContext = topologyContext;
		this.startingParallelism = startingParallelism;

		newThroughputs = new ArrayList<Double>();
		updateCounter = 0;

		// load previous data from zookeeper
		basePath = "/feedback/" + topologyContext.getStormId();
		zookeeper = Utils.newCurator(
			stormConf,
			(List<String>)stormConf.get(Config.STORM_ZOOKEEPER_SERVERS),
			stormConf.get(Config.STORM_ZOOKEEPER_PORT),
			(String)stormConf.get(Config.STORM_ZOOKEEPER_ROOT),
			new ZookeeperAuthInfo(stormConf));
		zookeeper.start();
		load();

		if (parallelism == null) {
			// First iteration of algorithm
			parallelism = startingParallelism;
		}


		LOG.info("zookeeper path: " + basePath);
		LOG.info("parallelism rebalance " + System.currentTimeMillis());
		for (String c : parallelism.keySet()) {
			LOG.info("parallelism " + c + " " + parallelism.get(c));
		}
	}

	public static byte[] serialize(Object obj) throws IOException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		ObjectOutputStream os = new ObjectOutputStream(out);
		os.writeObject(obj);
		return out.toByteArray();
	}

	public static Object deserialize(byte[] data) throws IOException, ClassNotFoundException {
		ByteArrayInputStream in = new ByteArrayInputStream(data);
		ObjectInputStream is = new ObjectInputStream(in);
		return is.readObject();
	}

	public Object loadObject(String path) {
		try {
			return deserialize(zookeeper.getData().forPath(path));
		} catch (KeeperException.NoNodeException e) {
			return null;
		} catch (Exception e) {
			LOG.error("loadObject Exception: " + e);
			return null;
		}
	}

	public void saveObject(String path, Object obj) {
		try {
			Stat stat = zookeeper.checkExists().forPath(path);
			if (stat == null) {
				// node doesn't exist, create it
				zookeeper.create()
					.creatingParentsIfNeeded()
					.forPath(path);
			}
			zookeeper.setData().forPath(path, serialize(obj));
		} catch (Exception e) {
			LOG.error("saveObject Exception: " + e);
		}
	}

	public void deleteObject(String path) {
		try {
			zookeeper.delete()
				.forPath(path);
		} catch (Exception e) {
			LOG.error("deleteObject Exception: " + e);
		}
	}

	private void load() {
		try {
			parallelism = (Map<String, Integer>)
				loadObject(basePath + "/parallelism");
			oldParallelism = (Map<String, Integer>)
				loadObject(basePath + "/oldParallelism");
			oldThroughputs = (List<Double>)
				loadObject(basePath + "/oldThroughputs");
			history = (List<Set<String>>)
				loadObject(basePath + "/history");
		} catch (ClassCastException e) {
			System.out.println("CLASS CAST EXCEPTION");
			deleteObject(basePath + "/parallelism");
			deleteObject(basePath + "/oldParallelism");
			deleteObject(basePath + "/oldThroughputs");
			deleteObject(basePath + "/history");
			load();
			return;
		}

		System.out.format("loaded parallelism: %s\n", parallelism);
		System.out.format("loaded oldParallelism: %s\n", oldParallelism);
		System.out.format("loaded oldThroughputs: %s\n", oldThroughputs);
		System.out.format("loaded history: %s\n", history);
	}

	// Save algorithm state to zookeeper
	private void save() {
		saveObject(basePath + "/parallelism", parallelism);
		saveObject(basePath + "/oldParallelism", oldParallelism);
		saveObject(basePath + "/oldThroughputs", oldThroughputs);
		saveObject(basePath + "/history", history);
	}

	protected double mean(List<Double> a) {
		double sum = 0;
		for (Double val : a) {
			sum += val;
		}
		return sum / a.size();
	}

	// Model "a" with a normal distribution, and test whether cdf(mean(b)) > pvalue
	protected boolean significantIncrease(List<Double> a, List<Double> b, double pvalue) {
		double meanA = mean(a);
		double sd = 0;
		for (Double val : a) {
			sd += (val - meanA) * (val - meanA);
		}
		sd = Math.sqrt(sd / (a.size() - 1));

		double meanB = mean(b);
		NormalDistribution dist = new NormalDistribution(meanA, sd);
		double p = dist.cumulativeProbability(meanB);

		boolean significant = (p > pvalue);
		System.out.println("p-value=" + p + ", " +
						   (significant ? "increase" : "no increase"));
		return significant;
	}

	public void update(double throughput, Map<String, ComponentStatistics> statistics) {
		String status = topologyStatus();
		LOG.info("Throughput: " + throughput);
		LOG.info("Topology Status: " + status);

		// wait sufficiently after rebalancing to run the algorithm again
		if (status != null && status.equals("REBALANCING")) {
			updateCounter = -5;
		}

		LOG.info("updateCounter = " + updateCounter);

		if (updateCounter > 15) {
			newThroughputs.add(throughput);
		}
		updateCounter++;

		int n = newThroughputs.size();
		if (n > 10) {
			boolean stable = !significantIncrease(
				newThroughputs.subList(n-10, n-5),
				newThroughputs.subList(n-5, n),
				0.6);

			LOG.info("Throughputs! " + newThroughputs);
			LOG.info("stable = " + stable);

			if (stable) {
				// truncate the newThroughputs
				newThroughputs = new ArrayList<Double>(newThroughputs.subList(n-10, n));
				LOG.info("Final Throughputs: " + newThroughputs);
				printStatistics(statistics);
				runAlgorithm(statistics);

				newThroughputs = new ArrayList<Double>();
				updateCounter = 0;
			}
		}
	}

	public void printStatistics(Map<String, ComponentStatistics> statistics) {
		for (String component : statistics.keySet()) {
			ComponentStatistics stats = statistics.get(component);
			if (stats.isSpout) {
				System.out.println(component + ".completeLatency = " + stats.completeLatency + " ms");
			}
		}

		for (String component : statistics.keySet()) {
			ComponentStatistics stats = statistics.get(component);
			System.out.println(component + ".send = " + stats.sendLatency + " ms");
		}

		for (String component : statistics.keySet()) {
			ComponentStatistics stats = statistics.get(component);
			System.out.println(component + ".execute = " + stats.executeLatency + " ms");
		}

		for (String component : statistics.keySet()) {
			ComponentStatistics stats = statistics.get(component);
			System.out.println(component + ".receive = " + stats.receiveLatency + " ms");
		}

		for (String component : statistics.keySet()) {
			ComponentStatistics stats = statistics.get(component);
			System.out.println(component + ".emitCount = " + stats.emitCount + " tuples/s");
		}

		for (String component : statistics.keySet()) {
			ComponentStatistics stats = statistics.get(component);
			System.out.println(component + ".outputRate = " + stats.outputRate + " tuples/s");
		}
	}

	protected String topologyStatus() {
		try {
			if (localCluster != null) {
				return localCluster.getTopologyInfo(
					topologyContext.getStormId()).get_status();
			} else {
				NimbusClient client = NimbusClient.getConfiguredClient(stormConf);
				return client.getClient().getTopologyInfo(
					topologyContext.getStormId()).get_status();
			}
		} catch (Exception e) {
			System.out.println("topologyStatus() exception: " + e);
			return null;
		}
	}

	protected void rebalance() {
		save();

		LOG.info("parallelism rebalance " + System.currentTimeMillis());
		for (String component : parallelism.keySet()) {
			LOG.info("parallelism " + component + " " + parallelism.get(component));
		}

		RebalanceOptions options = new RebalanceOptions();
		options.set_wait_secs(15);
		options.set_num_executors(parallelism);
		try {
			System.out.println("REBALANCING: " + parallelism);
			if (localCluster != null) {
				localCluster.rebalance(topologyName, options);
			} else {
				NimbusClient client = NimbusClient.getConfiguredClient(stormConf);
				client.getClient().rebalance(topologyName, options);
			}
		} catch (Exception e) {
			System.out.println("EXCEPTION DETECTED!!!!!" + e.toString());
		}
	}

	public abstract void runAlgorithm(Map<String, ComponentStatistics> statistics);
}
