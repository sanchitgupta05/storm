/* Star Topology Testing for Dynamic Scaling of Topology
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
import java.util.List;
import java.util.ArrayList;


/**
 * This topology demonstrates Storm's stream groupings and multilang capabilities.
 */
public class StarTopology {

  public static void main(String[] args) throws Exception {

	AutoTopologyBuilder builder = new AutoTopologyBuilder(5);

	/*Star Topology construction*/
	List<String> spouts = new ArrayList<String>();
	for(int i = 0; i < 3; i++) {
		String spoutName = "spout_"+String.valueOf(i);
		builder.addSpout(AutoSpout.create(spoutName));
		spouts.add(spoutName);
	}

	String midBoltName = "mid-bolt";
	AutoBolt midBolt = AutoBolt.create(midBoltName, 1, 100);
	for(String spout : spouts) {
		midBolt.addParent(spout);
	}
	builder.addBolt(midBolt, 1);

	for(int i = 0; i < 3; i++) {
		String boltName = "out_"+String.valueOf(i);
		builder.addBolt(AutoBolt.create(boltName, 2, 100)
						.addParent(midBoltName), 1);
	}

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
	  String topologyName = "star-topology";
	  FeedbackMetricsConsumer.register(conf, topologyName, topology, cluster);
      cluster.submitTopology(topologyName, conf, topology);

	  while(1<2)
		  Thread.sleep(60 * 1000);

      // cluster.shutdown();
    }
  }
}
