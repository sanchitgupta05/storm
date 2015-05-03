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
package storm.auto;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.SpoutDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.generated.StormTopology;

public class AutoTopologyBuilder {
	private TopologyBuilder builder;
	private int numTasks;

	public AutoTopologyBuilder(int numTasks) {
		this.builder = new TopologyBuilder();
		this.numTasks = numTasks;
	}

	public void addSpout(AutoSpout spout) {
		addSpout(spout, 1);
	}

	public void addSpout(AutoSpout spout, int phint) {
		SpoutDeclarer d = builder.setSpout(spout.name, spout, phint);
		d.setNumTasks(numTasks);
	}

	public void addBolt(AutoBolt bolt) {
		addBolt(bolt, 1);
	}

	public void addBolt(AutoBolt bolt, int phint) {
		BoltDeclarer d = builder.setBolt(bolt.name, bolt, phint);
		d.setNumTasks(numTasks);
		for (String parent : bolt.parents) {
			d.shuffleGrouping(parent);
		}
	}

	public StormTopology createTopology() {
		return builder.createTopology();
	}
}
