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
import storm.feedback.FeedbackMetricsConsumer;
import storm.feedback.RoundRobin;
import storm.feedback.CombinatorialAlgorithm;

import backtype.storm.generated.StormTopology;

import java.util.HashMap;
import java.util.Map;

/**
 * This topology demonstrates Storm's stream groupings and multilang capabilities.
 */
public class WordCountTopology {
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

  public static void main(String[] args) throws Exception {

    TopologyBuilder builder = new TopologyBuilder();

	int numTasks = 5;

	// 5, 8, 12
    builder.setSpout("spout", new RandomSentenceSpout(), 1).setNumTasks(numTasks);
    builder.setBolt("split", new SplitSentence(), 1).setNumTasks(numTasks).shuffleGrouping("spout");
    builder.setBolt("count", new WordCount(), 1).setNumTasks(numTasks).fieldsGrouping("split", new Fields("word"));
    // builder.setBolt("split2", new SplitSentence(), 1).shuffleGrouping("spout");
    // builder.setBolt("count2", new WordCount(), 2).fieldsGrouping("split2", new Fields("word"));

    Config conf = new Config();
    // conf.setDebug(true);
	conf.setStatsSampleRate(1);
	conf.put(Config.TOPOLOGY_BUILTIN_METRICS_BUCKET_SIZE_SECS, 1);
   conf.registerMetricsConsumer(FeedbackMetricsConsumer.class, 1);
	conf.setNumAckers(3);
	conf.setMaxTaskParallelism(10);
	conf.setMaxSpoutPending(64);
	conf.put(Config.NIMBUS_SUPERVISOR_TIMEOUT_SECS, 100);

    if (args != null && args.length > 0) {
      conf.setNumWorkers(3);

      StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
    }
    else {
      conf.setMaxTaskParallelism(10);

      LocalCluster cluster = new LocalCluster();
	  String topologyName = "word-count";
	  StormTopology topology = builder.createTopology();

	  //RoundRobin algorithm = new RoundRobin();
	  CombinatorialAlgorithm algorithm = new CombinatorialAlgorithm();
	  algorithm.initialize(cluster, topologyName, topology);
	  FeedbackMetricsConsumer.algorithm = algorithm;

      cluster.submitTopology(topologyName, conf, topology);

		while(1<2)
			Thread.sleep(10 * 20 * 60 * 1000);
		
      //cluster.shutdown();
    }
  }
}
