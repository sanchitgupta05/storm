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
package storm.starter;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.metric.LoggingMetricsConsumer;
import backtype.storm.task.ShellBolt;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import storm.starter.spout.RandomSentenceSpout;

import storm.auto.AutoSpout;
import storm.auto.AutoBolt;
import storm.auto.AutoTopologyBuilder;

import storm.feedback.FeedbackMetricsConsumer;
import storm.feedback.IFeedbackAlgorithm;
import storm.feedback.RoundRobin;
import storm.feedback.CombinatorialAlgorithm;

import backtype.storm.generated.StormTopology;

import backtype.storm.drpc.LinearDRPCTopologyBuilder;
import storm.starter.ReachTopology;

import backtype.storm.testing.TestWordSpout;
import storm.starter.bolt.IntermediateRankingsBolt;
import storm.starter.bolt.RollingCountBolt;
import storm.starter.bolt.TotalRankingsBolt;
import storm.starter.util.StormRunner;

import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;


/**
 * This topology demonstrates Storm's stream groupings and multilang capabilities.
 */
public class TopologyTester {
  public static class SplitSentence extends BaseBasicBolt {
	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
	  String[] words = tuple.getString(0).split(" ");
	  for (String word : words) {
		collector.emit(new Values(word));
	  }
	}

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word"));
    }
  }

  public static class WordCount extends BaseBasicBolt {
    Map<String, Integer> counts = new HashMap<String, Integer>();

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
      String word = tuple.getString(0);
      Integer count = counts.get(word);
      if (count == null)
        count = 0;
      count++;
      counts.put(word, count);
      collector.emit(new Values(word, count));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word", "count"));
    }
  }

  private static StormTopology createWordCount() {
	int numTasks = 10;
    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("spout", new RandomSentenceSpout(), 1)
	  .setNumTasks(numTasks);
    builder.setBolt("split", new SplitSentence(), 1)
	  .setNumTasks(numTasks).shuffleGrouping("spout");
    builder.setBolt("count", new WordCount(), 1)
	  .setNumTasks(numTasks).fieldsGrouping("split", new Fields("word"));
	return builder.createTopology();
  }

  private static StormTopology createCustom0() {
	AutoTopologyBuilder builder = new AutoTopologyBuilder(10);
	builder.addSpout(AutoSpout.create("a"));
	builder.addBolt(AutoBolt.create("b", 1, 1)
					.addParent("a"));
	builder.addBolt(AutoBolt.create("c", 1, 1)
					.addParent("a"));
	builder.addBolt(AutoBolt.create("d", 0, 1)
					.addParent("b")
					.addParent("c"));
	return builder.createTopology();
  }

  private static StormTopology createStar() {
	AutoTopologyBuilder builder = new AutoTopologyBuilder(10);

	builder.addSpout(AutoSpout.create("pre-spout"));

	/*Star Topology construction*/
	List<String> spouts = new ArrayList<String>();
	for(int i = 0; i < 3; i++) {
		String spoutName = "spout_"+String.valueOf(i);
		builder.addBolt(AutoBolt.create(spoutName, 0, 1));
		spouts.add(spoutName);
	}

	String midBoltName = "mid-bolt";
	AutoBolt midBolt = AutoBolt.create(midBoltName, 1, 10);
	for(String spout : spouts) {
		midBolt.addParent(spout);
	}
	builder.addBolt(midBolt, 1);

	for(int i = 0; i < 3; i++) {
		String boltName = "out_"+String.valueOf(i);
		builder.addBolt(AutoBolt.create(boltName, 2, 100)
						.addParent(midBoltName), 1);
	}
	return builder.createTopology();
  }

  private static StormTopology createDiamond() {
	AutoTopologyBuilder builder = new AutoTopologyBuilder(10);

	/* Diamond Topology construction*/
	builder.addSpout(AutoSpout.create("a"));
	AutoBolt output = AutoBolt.create("output", 1, 100);
	for(int i = 0; i <= 5; i++) {
		String boltName = "bolt_"+String.valueOf(i);
		builder.addBolt(AutoBolt.create(boltName, 2, 100)
						.addParent("a"), 1);
		output.addParent(boltName);
	}
	builder.addBolt(output, 1);
	return builder.createTopology();
  }

  private static StormTopology createLinear() {
	AutoTopologyBuilder builder = new AutoTopologyBuilder(10);

	/* Linear Topology construction*/
	builder.addSpout(AutoSpout.create("a"));
	String prevBoltName = "a";
	for(int i = 0; i <= 5; i++) {
		String boltName = "bolt_"+String.valueOf(i);
		builder.addBolt(AutoBolt.create(boltName, 2, 100)
						.addParent(prevBoltName), 1);
		prevBoltName = boltName;
	}
	return builder.createTopology();
  }

  private static StormTopology createReach() {
	int numTasks = 10;
    LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder("reach");
    builder.addBolt(new ReachTopology.GetTweeters(), 4).setNumTasks(numTasks);
    builder.addBolt(new ReachTopology.GetFollowers(), 1).shuffleGrouping().setNumTasks(numTasks);
    builder.addBolt(new ReachTopology.PartialUniquer(), 1).fieldsGrouping(
	  new Fields("id", "follower")).setNumTasks(numTasks);
    builder.addBolt(new ReachTopology.CountAggregator(), 1).fieldsGrouping(
	  new Fields("id")).setNumTasks(numTasks);
	return builder.createRemoteTopology();
  }

  // private static StormTopology createRolling() {
  //   TopologyBuilder builder = new TopologyBuilder();
  //   String spoutId = "wordGenerator";
  //   String counterId = "counter";
  //   String intermediateRankerId = "intermediateRanker";
  //   String totalRankerId = "finalRanker";
  //   builder.setSpout(spoutId, new TestWordSpout(), 5);
  //   builder.setBolt(counterId, new RollingCountBolt(9, 3), 4).fieldsGrouping(spoutId, new Fields("word"));
  //   builder.setBolt(intermediateRankerId, new IntermediateRankingsBolt(TOP_N), 4).fieldsGrouping(counterId, new Fields(
  //       "obj"));
  //   builder.setBolt(totalRankerId, new TotalRankingsBolt(TOP_N)).globalGrouping(intermediateRankerId);
  // }

  private static Map<String, StormTopology> getTopologies() {
	Map<String, StormTopology> tops = new HashMap<String, StormTopology>();
	tops.put("wordcount", createWordCount());
	tops.put("custom0", createCustom0());
	tops.put("star", createStar());
	tops.put("diamond", createDiamond());
	tops.put("linear", createLinear());
	tops.put("reach", createReach());
	return tops;
  }

  public static void main(String[] args) throws Exception {
	Map<String, StormTopology> tops = getTopologies();
	String topologyName = args[0];
	String topologyType = args[1];
	String algorithm = args[2];
	int iterations = Integer.parseInt(args[3]);
	boolean local = (args[4].equals("1"));

	StormTopology topology = tops.get(topologyType);
	if (topology == null) {
	  System.out.format("Topology type %s not found\n", topologyType);
	  System.exit(1);
	}

    Config conf = new Config();
	conf.setNumAckers(3);
	conf.put("FEEDBACK_ALGORITHM", algorithm);
	conf.put("FEEDBACK_ITERATIONS", iterations);
	conf.setNumWorkers(6);

	if (local) {
	  LocalCluster cluster = new LocalCluster();
	  FeedbackMetricsConsumer.register(conf, topologyName, topology, cluster);
	  cluster.submitTopology(topologyName, conf, topology);
	  Thread.sleep(45 * 60 * 1000);
	  cluster.shutdown();
	} else {
	  FeedbackMetricsConsumer.register(conf, topologyName, topology);
	  StormSubmitter.submitTopologyWithProgressBar(topologyName, conf, topology);
	}
  }
}
