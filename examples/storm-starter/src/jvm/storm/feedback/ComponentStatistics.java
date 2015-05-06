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

import java.io.Serializable;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

public class ComponentStatistics implements Serializable {
	public long counter;
	public boolean isSpout;

	// spout stats
	public double ackCount;
	public double completeLatency;

	// bolt stats
	public Map<String, Double> ackLatency;
	public double executeLatency;
	public double executeCount;
	public double emitCount;
	public double receiveQueueLength;
	public double sendQueueLength;
	public List<Long> sendQueueRead;

	public double receiveLatency;
	public double sendLatency;

	public double outputRate;

	public ComponentStatistics() {
		counter = 0;
		isSpout = false;

		ackCount = 0;
		completeLatency = 0;

		ackLatency = new HashMap<String, Double>();
		executeLatency = 0;
		executeCount = 0;
		emitCount = 0;
		receiveQueueLength = 0;
		sendQueueLength = 0;
		sendQueueRead = new ArrayList<Long>();

		receiveLatency = 0;
		sendLatency = 0;
		outputRate = 0;
	}

	private void processBolt(Map<String, Object> dpMap) {
		Map<String, Double> ackLatencyMap = (Map<String, Double>)dpMap.get("__process-latency");
		for (String id : ackLatencyMap.keySet()) {
			int pos = id.indexOf(":default");
			if (pos >= 0) {
				String component = id.substring(0, pos);
				double val = ackLatency.containsKey(component)
					? ackLatency.get(component) : 0;
				ackLatency.put(component, val + ackLatencyMap.get(id));
			}
		}

		Map<String, Double> executeLatencyMap = (Map<String, Double>)dpMap.get("__execute-latency");
		for (Double val : executeLatencyMap.values()) {
			executeLatency += val;
		}

		Map<String, Long> executeCountMap = (Map<String, Long>)dpMap.get("__execute-count");
		for (Long val : executeCountMap.values()) {
			executeCount += val;
		}

		Map<String, Long> emitCountMap = (Map<String, Long>)dpMap.get("__emit-count");
		if (emitCountMap != null && emitCountMap.containsKey("default")) {
			emitCount += emitCountMap.get("default");
		}

		Map<String, Long> receive = (Map<String, Long>)dpMap.get("__receive");
		if (receive != null) {
			receiveQueueLength += receive.get("population");
		}

		Map<String, Long> send = (Map<String, Long>)dpMap.get("__sendqueue");
		if (send != null) {
			sendQueueLength += send.get("population");
			sendQueueRead.add(send.get("read_pos"));
		}
	}

	private void processSpout(Map<String, Object> dpMap) {
		Map<String, Long> ackCounts = (Map<String, Long>)dpMap.get("__ack-count");
		if (ackCounts.containsKey("default")) {
			ackCount += ackCounts.get("default");
		}

		Map<String, Long> emitCountMap = (Map<String, Long>)dpMap.get("__emit-count");
		if (emitCountMap != null && emitCountMap.containsKey("default")) {
			emitCount += emitCountMap.get("default");
		}

		Map<String, Double> completeLatencies = (Map<String, Double>)dpMap.get("__complete-latency");
		if (completeLatencies.containsKey("default")) {
			completeLatency += completeLatencies.get("default");
		}
	}

	public void finish(int windowSize) {
		// counts should be divided by windowSize (since we sum over tasks)
		// latencies should be divided by counter (since we average over tasks)

		if (counter <= 0)
			return;

		ackCount /= windowSize;
		completeLatency /= counter;

		for (String component : ackLatency.keySet()) {
			ackLatency.put(component, ackLatency.get(component) / counter);
		}
		executeLatency /= counter;
		executeCount /= windowSize;
		emitCount /= windowSize;
		receiveQueueLength /= windowSize;
		sendQueueLength /= windowSize;

		// Add some special metrics
		if (executeCount > 0)
			receiveLatency = receiveQueueLength * (1000 / executeCount);
		if (emitCount > 0)
			sendLatency = sendQueueLength * (1000 / emitCount);

		// Figure out the total output rate of all the tasks combined
		long minRead = -1;
		long maxRead = -1;
		for (Long r : sendQueueRead) {
			if (minRead < 0 || r < minRead) {
				minRead = r;
			}
			if (maxRead < 0 || r > maxRead) {
				maxRead = r;
			}
		}
		outputRate = (counter / windowSize) *
			(double)(maxRead - minRead) / (windowSize - 1);
	}

	public void processDataPoints(Map<String, Object> dpMap) {
		// System.out.println("hello world " + dpMap);

		counter++;
		if (dpMap.containsKey("__ack-count")) {
			if (dpMap.containsKey("__execute-latency")) {
				processBolt(dpMap);
			} else {
				isSpout = true;
				processSpout(dpMap);
			}
		}
	}
}
