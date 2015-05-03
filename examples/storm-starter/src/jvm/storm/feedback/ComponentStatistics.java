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

import java.util.Map;

public class ComponentStatistics {
	public long counter;
	private boolean isSpout;

	// spout stats
	private long ackCount;
	private double completeLatency;

	// bolt stats
	private double executeLatency;
	public long executeCount;
	public long receiveQueueLength;
	private long sendQueueLength;

	// so it compiles
	public double congestion = 0;

	public ComponentStatistics() {
		counter = 0;
		isSpout = false;

		ackCount = 0;
		completeLatency = 0;

		executeLatency = 0;
		executeCount = 0;
		receiveQueueLength = 0;
		sendQueueLength = 0;
	}

	public boolean isSpout() {
		return isSpout;
	}

	public long ackCount() {
		return ackCount;
	}

	public double completeLatency() {
		return completeLatency / counter;
	}

	public double executeLatency() {
		if (executeCount == 0)
			return 0;
		return (executeLatency / 1000) / counter;
	}

	public double receiveLatency() {
		if (executeCount == 0)
			return 0;
		return (double)receiveQueueLength / executeCount;
	}

	public double sendLatency() {
		if (executeCount == 0)
			return 0;
		return (double)sendQueueLength / executeCount;
	}

	private void processBolt(Map<String, Object> dpMap) {
		double ackLatency = 0;
		Map<String, Double> ackLatencyMap = (Map<String, Double>)dpMap.get("__execute-latency");
		for (Double val : ackLatencyMap.values()) {
			executeLatency += val;
		}

		Map<String, Long> ackCountMap = (Map<String, Long>)dpMap.get("__execute-count");
		for (Long val : ackCountMap.values()) {
			executeCount += val;
		}

		if (dpMap.containsKey("__receive")) {
			Map<String, Long> receive =
				(Map<String, Long>)dpMap.get("__receive");
			receiveQueueLength += receive.get("population");
		}
		if (dpMap.containsKey("__sendqueue")) {
			Map<String, Long> send =
				(Map<String, Long>)dpMap.get("__sendqueue");
			sendQueueLength += send.get("population");
		}
	}

	private void processSpout(Map<String, Object> dpMap) {
		Map<String, Long> ackCounts = (Map<String, Long>)dpMap.get("__ack-count");
		if (ackCounts.containsKey("default")) {
			ackCount += ackCounts.get("default");
		}

		Map<String, Double> completeLatencies = (Map<String, Double>)dpMap.get("__complete-latency");
		if (completeLatencies.containsKey("default")) {
			completeLatency += completeLatencies.get("default");
		}
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
