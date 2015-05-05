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
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.ArrayList;

import backtype.storm.task.TopologyContext;
import backtype.storm.generated.GlobalStreamId;

import storm.feedback.ComponentStatistics;

public class ThroughputModel {

	public static boolean isCpuBound(
		double throughput,
		TopologyContext context,
		Map<String, ComponentStatistics> statistics,
		Map<String, Integer> parallelism,
		int numWorkers,
		double penalty) {

		double cpuEstimate = cpuBoundThroughput(
			context, statistics, parallelism, numWorkers, penalty);
		double ioEstimate = ioBoundThroughput(
			context, statistics, parallelism, numWorkers, penalty);

		System.out.println("ACTUAL: " + throughput);
		System.out.println("CPU ESTIMATE: " + cpuEstimate);
		System.out.println("IO ESTIMATE: " + ioEstimate);

		return cpuEstimate <= throughput * 1.25;
	}

	public static double predict(
		boolean isCpuBound,
		TopologyContext context,
		Map<String, ComponentStatistics> statistics,
		Map<String, Integer> parallelism,
		int numWorkers,
		double penalty) {

		if (isCpuBound) {
			return ThroughputModel.cpuBoundThroughput(
				context,
				statistics,
				parallelism,
				numWorkers,
				penalty);
		} else {
			return ThroughputModel.ioBoundThroughput(
				context,
				statistics,
				parallelism,
				numWorkers,
				penalty);
		}
	}

	public static double getCapacity(
		TopologyContext context,
		Map<String, ComponentStatistics> statistics) {
		double result = 0;
		Set<String> components = context.getComponentIds();
		for (String component : components) {
			ComponentStatistics stats = statistics.get(component);
			if (stats.emitCount > result) {
				result = stats.emitCount;
			}
		}
		return result;
	}

	public static double cpuBoundThroughput(
		TopologyContext context,
		Map<String, ComponentStatistics> statistics,
		Map<String, Integer> parallelism,
		int numWorkers,
		double penalty) {

		Set<String> components = context.getComponentIds();

		// Find the spout's emit rate
		// We're assuming there's only one spout in the topology.
		double spoutEmitCount = 0;
		for (String component : components) {
			ComponentStatistics stats = statistics.get(component);
			if (stats.isSpout) {
				spoutEmitCount = stats.ackCount;
				break;
			}
		}

		double maxLatency = 0;
		List<Double> latencies = new ArrayList<Double>();
		for (int i=0; i<numWorkers; i++) {
			latencies.add(0.0);
		}
		for (String component : components) {
			ComponentStatistics stats = statistics.get(component);
			double executeRate = stats.executeCount / spoutEmitCount;
			int p = parallelism.get(component);
			for (int i=0; i<p; i++) {
				addToMin(latencies, executeRate * stats.executeLatency / p);
			}
		}
		for (int i=0; i<latencies.size(); i++) {
			if (latencies.get(i) > maxLatency) {
				maxLatency = latencies.get(i);
			}
		}
		return 1000 / maxLatency * executorPenalty(
			context, parallelism, numWorkers, penalty);
	}

	public static double ioBoundThroughput(
		TopologyContext context,
		Map<String, ComponentStatistics> statistics,
		Map<String, Integer> parallelism,
		int numWorkers,
		double penalty) {

		Set<String> components = context.getComponentIds();

		// Find the spout's emit rate.
		// We're assuming there's only one spout in the topology.
		double spoutEmitCount = 0;
		for (String component : components) {
			ComponentStatistics stats = statistics.get(component);
			if (stats.isSpout) {
				spoutEmitCount = stats.ackCount;
				break;
			}
		}

		double maxTupleRate = 0;
		List<Double> tupleRates = new ArrayList<Double>();
		for (int i=0; i<numWorkers; i++) {
			tupleRates.add(0.0);
		}
		for (String component : components) {
			ComponentStatistics stats = statistics.get(component);
			double tupleRate = stats.emitCount / spoutEmitCount;
			int p = parallelism.get(component);
			for (int i=0; i<p; i++) {
				addToMin(tupleRates, tupleRate / p);
			}
		}
		for (int i=0; i<tupleRates.size(); i++) {
			if (tupleRates.get(i) > maxTupleRate) {
				maxTupleRate = tupleRates.get(i);
			}
		}

		System.out.println(tupleRates);

		double capacity = getCapacity(context, statistics);
		return capacity / maxTupleRate * executorPenalty(
			context, parallelism, numWorkers, penalty);
	}

	private static double executorPenalty(
		TopologyContext context,
		Map<String, Integer> parallelism,
		int numWorkers,
		double penalty) {

		Set<String> components = context.getComponentIds();

        // penalize too many executors
		int totalNumExecutors = 0;
		for (String component : components) {
			totalNumExecutors += parallelism.get(component);
		}
		return (1 - penalty * Math.max(0, totalNumExecutors - numWorkers));
	}

	public static void addToMin(List<Double> list, double value) {
		int j = -1;
		double min = Double.MAX_VALUE;
		for (int i=0; i<list.size(); i++) {
			if (list.get(i) < min) {
				min = list.get(i);
				j = i;
			}
		}
		list.set(j, list.get(j) + value);
	}

}
