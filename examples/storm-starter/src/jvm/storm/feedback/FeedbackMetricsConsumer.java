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

import java.util.Collection;
import java.util.Map;

//import org.apache.thrift.TException;
import org.apache.thrift7.TException;

import backtype.storm.metric.api.IMetricsConsumer;
import backtype.storm.task.IErrorReporter;
import backtype.storm.task.TopologyContext;
import backtype.storm.generated.Nimbus;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.ClusterSummary;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.Nimbus;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.SubmitOptions;
import backtype.storm.generated.TopologySummary;
import backtype.storm.utils.BufferFileInputStream;
import backtype.storm.utils.Utils;
import backtype.storm.utils.NimbusClient;

/**
* conf:{"topology.builtin.metrics.bucket.size.secs" 1,
 "nimbus.childopts" "-Xmx1024m",
 "ui.filter.params" nil,
 "storm.cluster.mode" "local",
 "storm.messaging.netty.client_worker_threads" 1,
 "supervisor.run.worker.as.user" false,
 "topology.max.task.parallelism" 3,
 "zmq.threads" 1,
 "storm.group.mapping.service" "backtype.storm.security.auth.ShellBasedGroupsMapping",
 "transactional.zookeeper.root" "/transactional",
 "topology.sleep.spout.wait.strategy.time.ms" 1,
 "drpc.invocations.port" 3773,
 "topology.multilang.serializer" "backtype.storm.multilang.JsonSerializer",
 "storm.messaging.netty.server_worker_threads" 1,
 "topology.max.error.report.per.interval" 5,
 "storm.thrift.transport" "backtype.storm.security.auth.SimpleTransportPlugin",
 "zmq.hwm" 0,
 "storm.principal.tolocal" "backtype.storm.security.auth.DefaultPrincipalToLocal",
 "storm.zookeeper.retry.times" 5,
 "ui.actions.enabled" true,
 "zmq.linger.millis" 0,
 "supervisor.enable" true,
 "topology.stats.sample.rate" 1.0,
 "storm.messaging.netty.min_wait_ms" 100,
 "storm.zookeeper.port" 2000,
 "supervisor.heartbeat.frequency.secs" 5,
 "topology.enable.message.timeouts" true,
 "drpc.worker.threads" 64,
 "drpc.queue.size" 128,
 "drpc.https.keystore.password" "",
 "logviewer.port" 8000,
 "nimbus.reassign" true,
 "topology.executor.send.buffer.size" 1024,
 "topology.spout.wait.strategy" "backtype.storm.spout.SleepSpoutWaitStrategy",
 "ui.host" "0.0.0.0",
 "topology.submitter.principal" "",
 "storm.nimbus.retry.interval.millis" 2000,
 "nimbus.inbox.jar.expiration.secs" 3600,
 "dev.zookeeper.path" "/tmp/dev-storm-zookeeper",
 "topology.acker.executors" nil,
 "topology.fall.back.on.java.serialization" true,
 "storm.zookeeper.servers" ["localhost"],
 "nimbus.thrift.threads" 64,
 "logviewer.cleanup.age.mins" 10080,
 "topology.worker.childopts" nil,
 "topology.classpath" nil,
 "supervisor.monitor.frequency.secs" 3,
 "nimbus.credential.renewers.freq.secs" 600,
 "topology.skip.missing.kryo.registrations" true,
 "drpc.authorizer.acl.filename" "drpc-auth-acl.yaml",
 "storm.group.mapping.service.cache.duration.secs" 120,
 "topology.testing.always.try.serialize" false,
 "nimbus.monitor.freq.secs" 10,
 "supervisor.supervisors" [],
 "topology.tasks" nil,
 "topology.bolts.outgoing.overflow.buffer.enable" false,
 "storm.messaging.netty.socket.backlog" 500,
 "topology.workers" 1,
 "storm.local.dir" "/var/folders/57/p62brkx94fd_sy961lwv88rr0000gn/T//8fdcceb7-ed82-401f-94e9-6bb0579d823b",
 "worker.childopts" "-Xmx768m",
 "storm.auth.simple-white-list.users" [],
 "topology.message.timeout.secs" 30,
 "topology.state.synchronization.timeout.secs" 60,
 "topology.tuple.serializer" "backtype.storm.serialization.types.ListDelegateSerializer",
 "supervisor.supervisors.commands" [],
 "logviewer.childopts" "-Xmx128m",
 "topology.environment" nil,
 "topology.debug" false,
 "storm.messaging.netty.max_retries" 300,
 "ui.childopts" "-Xmx768m",
 "storm.zookeeper.session.timeout" 20000,
 "drpc.childopts" "-Xmx768m",
 "drpc.http.creds.plugin" "backtype.storm.security.auth.DefaultHttpCredentialsPlugin",
 "storm.zookeeper.connection.timeout" 15000,
 "storm.zookeeper.auth.user" nil,
 "storm.meta.serialization.delegate" "backtype.storm.serialization.DefaultSerializationDelegate",
 "topology.max.spout.pending" nil,
 "nimbus.supervisor.timeout.secs" 60,
 "nimbus.task.timeout.secs" 30,
 "storm.zookeeper.superACL" nil,
 "drpc.port" 3772,
 "storm.zookeeper.retry.intervalceiling.millis" 30000,
 "nimbus.thrift.port" 6627,
 "storm.auth.simple-acl.admins" [],
 "storm.nimbus.retry.times" 5,
 "supervisor.worker.start.timeout.secs" 120,
 "storm.zookeeper.retry.interval" 1000,
 "logs.users" nil,
 "transactional.zookeeper.port" nil,
 "drpc.max_buffer_size" 1048576,
 "task.credentials.poll.secs" 30,
 "drpc.https.keystore.type" "JKS",
 "topology.worker.receiver.thread.count" 1,
 "supervisor.slots.ports" (1027 1028 1029),
 "topology.transfer.buffer.size" 1024,
 "topology.worker.shared.thread.pool.size" 4,
 "drpc.authorizer.acl.strict" false,
 "nimbus.file.copy.expiration.secs" 600,
 "topology.executor.receive.buffer.size" 1024,
 "topology.users" [],
 "nimbus.task.launch.secs" 120,
 "storm.local.mode.zmq" false,
 "storm.messaging.netty.buffer_size" 5242880,
 "worker.heartbeat.frequency.secs" 1,
 "ui.http.creds.plugin" "backtype.storm.security.auth.DefaultHttpCredentialsPlugin",
 "storm.zookeeper.root" "/storm",
 "topology.submitter.user" "",
 "topology.tick.tuple.freq.secs" nil,
 "drpc.https.port" -1,
 "task.refresh.poll.secs" 10,
 "topology.metrics.consumer.register" [{"argument" nil,
 "parallelism.hint" 1,
 "class" "storm.feedback.FeedbackMetricsConsumer"}],
 "task.heartbeat.frequency.secs" 3,
 "storm.messaging.netty.max_wait_ms" 1000,
 "drpc.http.port" 3774,
 "topology.error.throttle.interval.secs" 10,
 "storm.messaging.transport" "backtype.storm.messaging.netty.Context",
 "storm.messaging.netty.authentication" false,
 "topology.kryo.factory" "backtype.storm.serialization.DefaultKryoFactory",
 "topology.kryo.register" nil,
 "worker.gc.childopts" "",
 "nimbus.topology.validator" "backtype.storm.nimbus.DefaultTopologyValidator",
 "nimbus.cleanup.inbox.freq.secs" 600,
 "ui.users" nil,
 "transactional.zookeeper.servers" nil,
 "supervisor.worker.timeout.secs" 30,
 "storm.zookeeper.auth.password" nil,
 "supervisor.childopts" "-Xmx256m",
 "ui.filter" nil,
 "topology.receiver.buffer.size" 8,
 "ui.header.buffer.bytes" 4096,
 "storm.messaging.netty.flush.check.interval.ms" 10,
 "storm.nimbus.retry.intervalceiling.millis" 60000,
 "topology.trident.batch.emit.interval.millis" 50,
 "topology.disruptor.wait.strategy" "com.lmax.disruptor.BlockingWaitStrategy",
 "storm.auth.simple-acl.users" [],
 "drpc.invocations.threads" 64,
 "java.library.path" "/usr/local/lib:/opt/local/lib:/usr/lib",
 "ui.port" 8080,
 "topology.kryo.decorators" [],
 "storm.id" "word-count-1-1427856673",
 "topology.name" "word-count",
 "storm.messaging.netty.transfer.batch.size" 262144,
 "logviewer.appender.name" "A1",
 "nimbus.thrift.max_buffer_size" 1048576,
 "nimbus.host" "localhost",
 "storm.auth.simple-acl.users.commands" [],
 "drpc.request.timeout.secs" 600}
*/

public class FeedbackMetricsConsumer implements IMetricsConsumer {
	private Map localStormConf;

    public static final Logger LOG = LoggerFactory.getLogger(FeedbackMetricsConsumer.class);

    @Override
    public void prepare(Map stormConf, Object registrationArgument, TopologyContext context, IErrorReporter errorReporter) {
		
		  this.localStormConf = stormConf;
		  System.out.println("FEEDBACK_CONF: " + this.localStormConf);
        NimbusClient client = NimbusClient.getConfiguredClient(stormConf);
        // Nimbus.Iface nimbusInterface = null;
        // client.getClient().getClusterInfo();
        try {
        //     // LOG.info("");
            client.getClient().getClusterInfo();
        } catch(AuthorizationException e) {
            LOG.warn("exception: "+e.get_msg());
            // throw e;
        } catch(TException msg) {
            LOG.warn("exception: "+ msg);
        }finally {
            client.close();
        }
	
	}
	
	// public void contactNimbus() {
	// 	getClusterInfo();
	// }

    static private String padding = "                       ";

    @Override
    public void handleDataPoints(TaskInfo taskInfo, Collection<DataPoint> dataPoints) {
		// taskInfo.timestamp
		// taskInfo.srcWorkerHost, taskInfo.srcWorkerPort
		// taskInfo.srcTaskId
		// taskInfo.srcComponentId

		// dataPoint.name dataPoint.value

        StringBuilder sb = new StringBuilder();
        String header = String.format("FEEDBACK %d\t%15s:%-4d\t%3d:%-11s\t",
            taskInfo.timestamp,
            taskInfo.srcWorkerHost, taskInfo.srcWorkerPort,
            taskInfo.srcTaskId,
            taskInfo.srcComponentId);
        sb.append(header);
        for (DataPoint p : dataPoints) {
            sb.delete(header.length(), sb.length());
            sb.append(p.name)
                .append(padding).delete(header.length()+23,sb.length()).append("\t")
                .append(p.value);
			System.out.println(sb.toString());
        }
    }

    @Override
    public void cleanup() { }


}
