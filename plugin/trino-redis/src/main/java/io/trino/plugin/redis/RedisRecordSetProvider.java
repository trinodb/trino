/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.redis;

import com.google.inject.Inject;
import io.trino.decoder.DispatchingRowDecoderFactory;
import io.trino.decoder.RowDecoder;
import io.trino.decoder.RowDecoderSpec;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.RecordSet;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;

/**
 * Factory for Redis specific {@link RecordSet} instances.
 */
public class RedisRecordSetProvider
        implements ConnectorRecordSetProvider
{
    private final DispatchingRowDecoderFactory decoderFactory;
    private final RedisJedisManager jedisManager;

    @Inject
    public RedisRecordSetProvider(DispatchingRowDecoderFactory decoderFactory, RedisJedisManager jedisManager)
    {
        this.decoderFactory = requireNonNull(decoderFactory, "decoderFactory is null");
        this.jedisManager = requireNonNull(jedisManager, "jedisManager is null");
    }

    @Override
    public RecordSet getRecordSet(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorSplit split, ConnectorTableHandle table, List<? extends ColumnHandle> columns)
    {
        RedisSplit redisSplit = (RedisSplit) split;

        List<RedisColumnHandle> redisColumns = columns.stream()
                .map(RedisColumnHandle.class::cast)
                .collect(toImmutableList());

        RowDecoder keyDecoder = decoderFactory.create(
                session,
                new RowDecoderSpec(
                        redisSplit.getKeyDataFormat(),
                        emptyMap(),
                        redisColumns.stream()
                                .filter(col -> !col.isInternal())
                                .filter(RedisColumnHandle::isKeyDecoder)
                                .collect(toImmutableSet())));

        RowDecoder valueDecoder = decoderFactory.create(
                session,
                new RowDecoderSpec(
                        redisSplit.getValueDataFormat(),
                        emptyMap(),
                        redisColumns.stream()
                                .filter(col -> !col.isInternal())
                                .filter(col -> !col.isKeyDecoder())
                                .collect(toImmutableSet())));

        return new RedisRecordSet(redisSplit, jedisManager, redisColumns, keyDecoder, valueDecoder);
    }
}
