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
package org.apache.storm.redis.util.config;

import redis.clients.jedis.Protocol;

import java.io.Serializable;

public class JedisPoolConfig implements Serializable {
    public static final String DEFAULT_HOST = "127.0.0.1";

    private String host;
    private int port;
    private int timeout;
    private int database;
    private String password;

    public JedisPoolConfig(String host, int port, int timeout, String password, int database) {
        this.host = host;
        this.port = port;
        this.timeout = timeout;
        this.database = database;
        this.password = password;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public int getTimeout() {
        return timeout;
    }

    public int getDatabase() {
        return database;
    }

    public String getPassword() {
        return password;
    }

    public static class Builder {
        private String host = DEFAULT_HOST;
        private int port = Protocol.DEFAULT_PORT;
        private int timeout = Protocol.DEFAULT_TIMEOUT;
        private int database = Protocol.DEFAULT_DATABASE;
        private String password;

        public Builder setHost(String host) {
            this.host = host;
            return this;
        }

        public Builder setPort(int port) {
            this.port = port;
            return this;
        }

        public Builder setTimeout(int timeout) {
            this.timeout = timeout;
            return this;
        }

        public Builder setDatabase(int database) {
            this.database = database;
            return this;
        }

        public Builder setPassword(String password) {
            this.password = password;
            return this;
        }

        public JedisPoolConfig build() {
            return new JedisPoolConfig(host, port, timeout, password, database);
        }
    }
}
