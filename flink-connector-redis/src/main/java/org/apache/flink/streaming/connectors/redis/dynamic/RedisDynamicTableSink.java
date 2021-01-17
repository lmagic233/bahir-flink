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

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.Util;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisDataType;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

import java.util.Optional;

import static org.apache.flink.streaming.connectors.redis.dynamic.RedisDynamicTableFactory.*;

public class RedisDynamicTableSink implements DynamicTableSink {
  private ReadableConfig options;

  public RedisDynamicTableSink(ReadableConfig options) {
    this.options = options;
  }

  @Override
  public ChangelogMode getChangelogMode(ChangelogMode changelogMode) {
    return ChangelogMode.newBuilder()
      .addContainedKind(RowKind.INSERT)
      .addContainedKind(RowKind.UPDATE_BEFORE)
      .addContainedKind(RowKind.UPDATE_AFTER)
      .addContainedKind(RowKind.DELETE)
      .build();
  }

  @Override
  public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
    FlinkJedisConfigBase config = Util.getFlinkJedisConfig(options);

    RedisCommand command = RedisCommand.valueOf(options.get(COMMAND).toUpperCase());
    RedisMapper<RowData> mapper = new RowDataRedisMapper(options, command);
    RedisSink<RowData> redisSink = new RedisSink<>(config, mapper);

    return SinkFunctionProvider.of(redisSink);
  }

  @Override
  public DynamicTableSink copy() {
    return new RedisDynamicTableSink(options);
  }

  @Override
  public String asSummaryString() {
    return "redis";
  }


  public static final class RowDataRedisMapper implements RedisMapper<RowData> {
    private static final long serialVersionUID = 1L;

    private ReadableConfig options;
    private RedisCommand command;

    public RowDataRedisMapper(ReadableConfig options, RedisCommand command) {
      this.options = options;
      this.command = command;
    }

    @Override
    public RedisCommandDescription getCommandDescription() {
      return new RedisCommandDescription(command);
    }

    @Override
    public String getKeyFromData(RowData data) {
      return data.getString(needAdditionalKey() ? 1 : 0).toString();
    }

    @Override
    public String getValueFromData(RowData data) {
      return data.getString(needAdditionalKey() ? 2 : 1).toString();
    }

    @Override
    public Optional<String> getAdditionalKey(RowData data) {
      return needAdditionalKey() ? Optional.of(data.getString(0).toString()) : Optional.empty();
    }

    @Override
    public Optional<Integer> getAdditionalTTL(RowData data) {
      return options.getOptional(KEY_TTL_SEC);
    }

    private boolean needAdditionalKey() {
      return command.getRedisDataType() == RedisDataType.HASH || command.getRedisDataType() == RedisDataType.SORTED_SET;
    }
  }
}
