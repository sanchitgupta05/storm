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

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.MessageId;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Random;

import java.math.BigInteger;
import java.security.MessageDigest;

public class AutoBolt extends BaseRichBolt {
	public String name;
	public List<String> parents;

	private int work;
	private int rate;
	private Random rn;

	private OutputCollector collector;
	private Map<MessageId, List<Tuple>> seen;

	public AutoBolt(String name, int work, int rate) {
		this.name = name;
		this.parents = new ArrayList<String>();

		this.work = work;
		this.rate = rate;
		this.rn = new Random();
	}

	public static AutoBolt create(String name, int work, int rate) {
		return new AutoBolt(name, work, rate);
	}

	public AutoBolt addParent(String name) {
		parents.add(name);
		return this;
	}

	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.seen = new HashMap<MessageId, List<Tuple>>();
	}

	@Override
	public void execute(Tuple tuple) {
		// proof of work
		for (int i=0; i<work; i++) {
			try {
				MessageDigest md = MessageDigest.getInstance("SHA-256");
				md.update((new BigInteger(1024, rn)).toString(2).getBytes());
				byte byteData[] = md.digest();
			} catch (Exception e) {
				System.out.println("exception caught " + e);
			}
		}

		MessageId id = tuple.getMessageId();

		if (!seen.containsKey(id)) {
			seen.put(id, new ArrayList<Tuple>());
		}
		if (tuple.getString(0).equals("fin")) {
			seen.get(id).add(tuple);
		} else {
			collector.ack(tuple);
		}

		// System.out.format("%s received, %d total\n", name, seen.get(id).size());

		// int numFinished = 0;
		// for (Tuple t : seen.get(id)) {
		// 	if (t.getString(0).equals("fin")) {
		// 		numFinished += 1;
		// 	}
		// }

		if (seen.get(id).size() >= parents.size()) {
			for (Tuple t : seen.get(id)) {
				collector.ack(t);
			}
			seen.remove(id);

			// System.out.println(name + " sending");

			for (int i=0; i<rate; i++) {
				String data = (new BigInteger(10, rn)).toString(2);
				collector.emit(new Values(data));
			}
			collector.emit(new Values("fin"));
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("data"));
	}
}
