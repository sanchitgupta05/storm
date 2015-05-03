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
	private Map<String, Integer> oldParallelism;
	private List<Double> oldThroughputs;
	private List<Set<String>> history;

	protected Map stormConf;
	protected TopologyContext topologyContext;
	private String topologyName;
	private Map<String, Integer> startingParallelism;

	private boolean waitingForRebalance;
	private List<Double> newThroughputs;

	@Override
	public void initialize(String topologyName, Map stormConf,
						   TopologyContext topologyContext, Map<String, Integer> startingParallelism) {
		this.topologyName = topologyName;
		this.stormConf = stormConf;
		this.topologyContext = topologyContext;
		this.startingParallelism = startingParallelism;

		waitingForRebalance = false;
		newThroughputs = new ArrayList<Double>();

		// connect to Nimbus
		if (localCluster == null) {
			// Create nimbus client, etc.
		}

		// load previous data from zookeeper
		basePath = "/feedback/" + topologyName;
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

	public void update(double acksPerSecond, Map<String, ComponentStatistics> statistics) {
		newThroughputs.add(acksPerSecond);

		if (newThroughputs.size() > 10
			&& !waitingForRebalance) {

			boolean reverted = (history != null && oldParallelism == null);
			if (reverted) {
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

			waitingForRebalance = true;
		}
	}

	private void applyNextAction(Map<String, ComponentStatistics> statistics) {
		if (history == null) {
			history = new ArrayList<Set<String>>();
		}

		// find the first action that isn't in the history
		Set<String> action = null;
		List<Set<String>> actions = run(statistics);
		for (int i=0; i<actions.size(); i++) {
			if (!history.contains(actions.get(i))) {
				action = actions.get(i);
				break;
			}
		}

		if (action != null) {
			history.add(action);
			oldThroughputs = newThroughputs;
			oldParallelism = new HashMap<String, Integer>(parallelism);
			System.out.println(parallelism);
			for (String component : action) {
				int p = oldParallelism.get(component);
				parallelism.put(component, p + 1);
			}
			save();
			rebalance();
		}
	}

	private void revertAction() {
		parallelism = oldParallelism;
		oldParallelism = null;
		oldThroughputs = null;
		save();
		rebalance();
	}

	protected void rebalance() {
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

	protected abstract List<Set<String>> run(Map<String, ComponentStatistics> statistics);
}
