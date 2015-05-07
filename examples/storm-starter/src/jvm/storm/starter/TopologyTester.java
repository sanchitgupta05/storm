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

import java.util.HashMap;
import java.util.Map;

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
    TopologyBuilder builder = new TopologyBuilder();
	int numTasks = 10;
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

  private static Map<String, StormTopology> getTopologies() {
	Map<String, StormTopology> tops = new HashMap<String, StormTopology>();
	tops.put("wordcount", createWordCount());
	tops.put("custom0", createCustom0());
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
