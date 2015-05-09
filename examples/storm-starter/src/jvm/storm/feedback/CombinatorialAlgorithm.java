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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.Math;
import java.util.Collection;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.LinkedList;
import java.util.Queue;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Set;
import java.util.HashSet;

import org.apache.thrift7.TException;
import org.apache.commons.math3.distribution.NormalDistribution;

import backtype.storm.ILocalCluster;
import backtype.storm.metric.api.IMetricsConsumer;
import backtype.storm.task.IErrorReporter;
import backtype.storm.task.TopologyContext;
import backtype.storm.generated.*;
import backtype.storm.utils.BufferFileInputStream;
import backtype.storm.utils.Utils;
import backtype.storm.utils.NimbusClient;

import storm.feedback.ranking.IRanker;

public class CombinatorialAlgorithm extends IterativeFeedbackAlgorithm {
	private IRanker ranker;

	public CombinatorialAlgorithm(int iterations, IRanker ranker) {
		super(iterations);
		this.ranker = ranker;
	}

	public List<Set<String>> getActions(Map<String, ComponentStatistics> statistics) {
		List<String> ranking = ranker.rankComponents(
			state.topologyContext,
			statistics,
			state.parallelism);

		// truncate list to reduce possible combinations
		int k = Math.min(3, ranking.size());
		if (ranking.size() > k)
			ranking = ranking.subList(0, k);

		// return all combinations of the ranked components
		// starts with subsets of size 1, then size 2, etc
		List<Set<String>> result = new ArrayList<Set<String>>();
		for (int i=1; i<=k; i++) {
			CombinationGenerator gen = new CombinationGenerator(i, ranking);
			result.add(gen.get());
			while (gen.next()) {
				result.add(gen.get());
			}
		}

		System.out.println("Actions: " + result);
		return result;
	}

	public class CombinationGenerator {
		public int k;
		private List<String> components;
		private int[] toIncrease;

		public CombinationGenerator(int k, List<String> components) {
			this.k = k;
			this.components = components;
			toIncrease = new int[k];
			for (int i=0; i<k; i++) {
				toIncrease[i] = i;
			}
		}

		private boolean tryIncrement(int i) {
			if (i < 0) {
				return false;
			}

			int next = toIncrease[i] + 1;
			if (next >= components.size()
				|| (i + 1 < toIncrease.length
					&& next >= toIncrease[i+1])) {
				return tryIncrement(i-1);
			} else {
				for (int j=i; j<toIncrease.length; j++) {
					toIncrease[j] = next + (j - i);
				}
				return true;
			}
		}

		public boolean next() {
			return tryIncrement(toIncrease.length - 1);
		}

		public Set<String> get() {
			Set<String> result = new HashSet<String>();
			for (int i=0; i<toIncrease.length; i++) {
				result.add(components.get(toIncrease[i]));
			}
			return result;
		}
	}
}
