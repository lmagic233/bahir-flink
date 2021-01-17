/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.connectors.redis.common;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisClusterConfig;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisSentinelConfig;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.streaming.connectors.redis.dynamic.RedisDynamicTableFactory.*;

public class Util {
    public static void checkArgument(boolean condition, String message) {
        if(!condition) {
            throw new IllegalArgumentException(message);
        }
    }

    public static FlinkJedisConfigBase getFlinkJedisConfig(ReadableConfig options) {
        FlinkJedisConfigBase config = null;

        String mode = options.get(MODE);
        String singleHost = options.get(SINGLE_HOST);
        int singlePort = options.get(SINGLE_PORT);
        String clusterNodes = options.get(CLUSTER_NODES);
        String sentinelMaster = options.get(SENTINEL_MASTER);
        String sentinelNodes = options.get(SENTINEL_NODES);
        String password = options.get(PASSWORD);
        int dbNum = options.get(DB_NUM);

        int timeout = options.get(CONNECTION_TIMEOUT_MS);
        int maxTotal = options.get(CONNECTION_MAX_TOTAL);
        int maxIdle = options.get(CONNECTION_MAX_IDLE);
        boolean testOnBorrow = options.get(CONNECTION_TEST_ON_BORROW);
        boolean testOnReturn = options.get(CONNECTION_TEST_ON_RETURN);
        boolean testWhileIdle = options.get(CONNECTION_TEST_WHILE_IDLE);

        if (mode.equals("single")) {
            config = new FlinkJedisPoolConfig.Builder()
              .setHost(singleHost)
              .setPort(singlePort)
              .setPassword(password)
              .setDatabase(dbNum)
              .setTimeout(timeout)
              .setMaxTotal(maxTotal)
              .setMaxIdle(maxIdle)
              .setTestOnBorrow(testOnBorrow)
              .setTestOnReturn(testOnReturn)
              .setTestWhileIdle(testWhileIdle)
              .build();
        } else if (mode.equals("sentinel")) {
            Set<String> sentinels = new HashSet<>();
            Collections.addAll(sentinels, StringUtils.split(sentinelNodes, ','));

            config = new FlinkJedisSentinelConfig.Builder()
              .setMasterName(sentinelMaster)
              .setSentinels(sentinels)
              .setPassword(password)
              .setDatabase(dbNum)
              .setConnectionTimeout(timeout)
              .setSoTimeout(timeout)
              .setMaxTotal(maxTotal)
              .setMaxIdle(maxIdle)
              .setTestOnBorrow(testOnBorrow)
              .setTestOnReturn(testOnReturn)
              .setTestWhileIdle(testWhileIdle)
              .build();
        } else if (mode.equals("cluster")) {
            Set<InetSocketAddress> nodes = new HashSet<>();
            for (String node : StringUtils.split(clusterNodes, ',')) {
                String[] strs = StringUtils.split(node, ':');
                if (strs != null && strs.length == 2) {
                    nodes.add(new InetSocketAddress(strs[0], NumberUtils.createInteger(strs[1])));
                }
            }

            config = new FlinkJedisClusterConfig.Builder()
              .setNodes(nodes)
              .setPassword(password)
              .setTimeout(timeout)
              .setMaxTotal(maxTotal)
              .setMaxIdle(maxIdle)
              .setTestOnBorrow(testOnBorrow)
              .setTestOnReturn(testOnReturn)
              .setTestWhileIdle(testWhileIdle)
              .build();
        }

        return config;
    }
}
