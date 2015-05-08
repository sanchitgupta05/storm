/* Diamond Topology Testing for Dynamic Scaling of Topology
 *
 * - Sanchit Gupta, Xander Masotto, Rohit Sanbhadti
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
import backtype.storm.generated.StormTopology;

import java.util.HashMap;
import java.util.Map;

/**
 * This topology demonstrates Storm's stream groupings and multilang capabilities.
 */
public class DiamondTopology {

  public static void main(String[] args) throws Exception {

	AutoTopologyBuilder builder = new AutoTopologyBuilder(5);

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

   /* Config setup */
	Config conf = new Config();
	conf.setStatsSampleRate(1);
	conf.put(Config.TOPOLOGY_BUILTIN_METRICS_BUCKET_SIZE_SECS, 1);
	conf.setNumAckers(1);
	conf.setMaxTaskParallelism(10);
	conf.setMaxSpoutPending(10);

	StormTopology topology = builder.createTopology();
	if (args != null && args.length > 0) {
		conf.setNumWorkers(4);

		String topologyName = args[0];
		FeedbackMetricsConsumer.register(conf, topologyName, topology);
		StormSubmitter.submitTopologyWithProgressBar(topologyName, conf,
																	builder.createTopology());
	}
    else {
	  LocalCluster cluster = new LocalCluster();
	  String topologyName = "diamond-topology";
	  FeedbackMetricsConsumer.register(conf, topologyName, topology, cluster);
      cluster.submitTopology(topologyName, conf, topology);

	  while(1<2)
		  Thread.sleep(60 * 1000);

      // cluster.shutdown();
    }
  }
}
