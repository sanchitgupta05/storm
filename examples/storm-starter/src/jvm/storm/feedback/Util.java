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

// public class Util {
// 	public static double getProcessCpuLoad() {
// 		try {
// 			MBeanServer mbs    = ManagementFactory.getPlatformMBeanServer();
//       ObjectName name    = ObjectName.getInstance("java.lang:type=OperatingSystem");
//       AttributeList list = mbs.getAttributes(name, new String[]{ "ProcessCpuLoad" });

//       if (list.isEmpty())   return Double.NaN;

//       Attribute att = (Attribute)list.get(0);
//       Double value  = (Double)att.getValue();

//       if (value == -1.0)    return Double.NaN;  // usually takes a couple of seconds before we get real values

//       return ((int)(value * 1000) / 10.0);    // returns a percentage value with 1 decimal point precision
//     } catch (Exception e) {
//       return -1;
//     }
//   }
// }
