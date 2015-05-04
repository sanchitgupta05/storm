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
package storm.feedback.ranking;

import java.util.Map;
import java.util.List;
import java.util.Set;
import java.util.ArrayList;

import backtype.storm.task.TopologyContext;

import storm.feedback.ComponentStatistics;

public class CongestionRanker implements IRanker {
	public List<String> rankComponents(TopologyContext context,
									   Map<String, ComponentStatistics> statistics,
									   Map<String, Integer> parallelism) {
		Set<String> components = context.getComponentIds();
		List<String> result = new ArrayList<String>();
		int k = components.size();
		for (int i=0; i<k; i++) {
			double max = 0;
			String maxComponent = null;
			for (String component : components) {
				ComponentStatistics stats = statistics.get(component);
				double congestion =
					stats.receiveLatency +
					stats.executeLatency +
					stats.sendLatency;
				if (congestion > max) {
					max = congestion;
					maxComponent = component;
				}
			}
			if (maxComponent != null) {
				components.remove(maxComponent);
				result.add(maxComponent);
			} else {
				break;
			}
		}
		return result;
	}
}
