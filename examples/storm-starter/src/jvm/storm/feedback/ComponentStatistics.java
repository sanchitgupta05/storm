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

class ComponentStatistics {
	public boolean isSpout;

	public long ackCount;
	public double ackLatency;
	public long receiveQueueLength;
	public long sendQueueLength;
	public double congestion;
	public long counter;

	public ComponentStatistics() {
		isSpout = false;
		ackCount = 0;
		ackLatency = 0;
		receiveQueueLength = 0;
		sendQueueLength = 0;
		congestion = 0;
		counter = 0;
	}

	private void processBolt(Map<String, Object> dpMap) {
		long ackCount = 0;
		Map<String, Long> ackCountMap = (Map<String, Long>)dpMap.get("__ack-count");
		for (Long val : ackCountMap.values()) {
			ackCount += val;
		}

		double ackLatency = 0;
		Map<String, Double> ackLatencyMap = (Map<String, Double>)dpMap.get("__process-latency");
		for (Double val : ackLatencyMap.values()) {
			ackLatency += val;
		}

		if (ackCount > 0) {
			this.ackLatency = (ackLatency * ackCount + this.ackLatency * this.ackCount) / (ackCount + this.ackCount);
			this.ackCount += ackCount;
		}
	}

	private void processSpout(Map<String, Object> dpMap) {
		isSpout = true;

		Map<String, Long> ackCounts = (Map<String, Long>)dpMap.get("__ack-count");
		if (ackCounts.containsKey("default")) {
			ackCount += ackCounts.get("default");
		}
	}

	public void processDataPoints(Map<String, Object> dpMap) {
		counter++;
		if (dpMap.containsKey("__ack-count")) {
			if (dpMap.containsKey("__execute-latency")) {
				processBolt(dpMap);
			} else {
				processSpout(dpMap);
			}
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
}
