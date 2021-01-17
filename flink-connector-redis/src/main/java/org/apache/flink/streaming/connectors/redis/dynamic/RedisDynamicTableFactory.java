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
package org.apache.flink.streaming.connectors.redis.dynamic;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.Protocol;

import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.table.factories.FactoryUtil.*;

public class RedisDynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {
  public static final ConfigOption<String> MODE = ConfigOptions
    .key("mode")
    .stringType()
    .defaultValue("single");

  public static final ConfigOption<String> SINGLE_HOST = ConfigOptions
    .key("single.host")
    .stringType()
    .defaultValue(Protocol.DEFAULT_HOST);

  public static final ConfigOption<Integer> SINGLE_PORT = ConfigOptions
    .key("single.port")
    .intType()
    .defaultValue(Protocol.DEFAULT_PORT);

  public static final ConfigOption<String> CLUSTER_NODES = ConfigOptions
    .key("cluster.nodes")
    .stringType()
    .noDefaultValue();

  public static final ConfigOption<String> SENTINEL_NODES = ConfigOptions
    .key("sentinel.nodes")
    .stringType()
    .noDefaultValue();

  public static final ConfigOption<String> SENTINEL_MASTER = ConfigOptions
    .key("sentinel.master")
    .stringType()
    .noDefaultValue();

  public static final ConfigOption<String> PASSWORD = ConfigOptions
    .key("password")
    .stringType()
    .noDefaultValue();

  public static final ConfigOption<String> COMMAND = ConfigOptions
    .key("command")
    .stringType()
    .noDefaultValue();

  public static final ConfigOption<Integer> DB_NUM = ConfigOptions
    .key("db.num")
    .intType()
    .defaultValue(Protocol.DEFAULT_DATABASE);

  public static final ConfigOption<Integer> KEY_TTL_SEC = ConfigOptions
    .key("key.ttl-sec")
    .intType()
    .noDefaultValue();

  public static final ConfigOption<Integer> CONNECTION_TIMEOUT_MS = ConfigOptions
    .key("connection.timeout-ms")
    .intType()
    .defaultValue(Protocol.DEFAULT_TIMEOUT);

  public static final ConfigOption<Integer> CONNECTION_MAX_TOTAL = ConfigOptions
    .key("connection.max-total")
    .intType()
    .defaultValue(GenericObjectPoolConfig.DEFAULT_MAX_TOTAL);

  public static final ConfigOption<Integer> CONNECTION_MAX_IDLE = ConfigOptions
    .key("connection.max-idle")
    .intType()
    .defaultValue(GenericObjectPoolConfig.DEFAULT_MAX_IDLE);

  public static final ConfigOption<Boolean> CONNECTION_TEST_ON_BORROW = ConfigOptions
    .key("connection.test-on-borrow")
    .booleanType()
    .defaultValue(GenericObjectPoolConfig.DEFAULT_TEST_ON_BORROW);

  public static final ConfigOption<Boolean> CONNECTION_TEST_ON_RETURN = ConfigOptions
    .key("connection.test-on-return")
    .booleanType()
    .defaultValue(GenericObjectPoolConfig.DEFAULT_TEST_ON_RETURN);

  public static final ConfigOption<Boolean> CONNECTION_TEST_WHILE_IDLE = ConfigOptions
    .key("connection.test-while-idle")
    .booleanType()
    .defaultValue(GenericObjectPoolConfig.DEFAULT_TEST_WHILE_IDLE);

  public static final ConfigOption<Boolean> LOOKUP_CACHE_ENABLE = ConfigOptions
    .key("lookup.cache.enable")
    .booleanType()
    .defaultValue(Boolean.FALSE);

  public static final ConfigOption<Integer> LOOKUP_CACHE_MAX_ROWS = ConfigOptions
    .key("lookup.cache.max-rows")
    .intType()
    .defaultValue(10000);

  public static final ConfigOption<Integer> LOOKUP_CACHE_TTL_SEC = ConfigOptions
    .key("lookup.cache.ttl-sec")
    .intType()
    .defaultValue(60);

  public static final ConfigOption<Boolean> LOOKUP_CACHE_CACHE_EMPTY = ConfigOptions
    .key("lookup.cache.cache-empty")
    .booleanType()
    .defaultValue(Boolean.TRUE);

  @Override
  public DynamicTableSource createDynamicTableSource(Context context) {
    return null;
  }

  @Override
  public DynamicTableSink createDynamicTableSink(Context context) {
    FactoryUtil.TableFactoryHelper helper = createTableFactoryHelper(this, context);
    helper.validate();

    ReadableConfig options = helper.getOptions();
    return new RedisDynamicTableSink(options);
  }

  @Override
  public String factoryIdentifier() {
    return "redis";
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    Set<ConfigOption<?>> requiredOptions = new HashSet<>();
    requiredOptions.add(MODE);
    return requiredOptions;
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    Set<ConfigOption<?>> optionalOptions = new HashSet<>();
    optionalOptions.add(SINGLE_HOST);
    optionalOptions.add(SINGLE_PORT);
    optionalOptions.add(CLUSTER_NODES);
    optionalOptions.add(SENTINEL_NODES);
    optionalOptions.add(SENTINEL_MASTER);
    optionalOptions.add(PASSWORD);
    optionalOptions.add(COMMAND);
    optionalOptions.add(DB_NUM);
    optionalOptions.add(KEY_TTL_SEC);
    optionalOptions.add(CONNECTION_TIMEOUT_MS);
    optionalOptions.add(CONNECTION_MAX_TOTAL);
    optionalOptions.add(CONNECTION_MAX_IDLE);
    optionalOptions.add(CONNECTION_TEST_ON_BORROW);
    optionalOptions.add(CONNECTION_TEST_ON_RETURN);
    optionalOptions.add(CONNECTION_TEST_WHILE_IDLE);
    optionalOptions.add(LOOKUP_CACHE_ENABLE);
    optionalOptions.add(LOOKUP_CACHE_MAX_ROWS);
    optionalOptions.add(LOOKUP_CACHE_TTL_SEC);
    optionalOptions.add(LOOKUP_CACHE_CACHE_EMPTY);
    return optionalOptions;
  }
}
