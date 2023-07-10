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
package io.trino.plugin.kinesis;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.units.Duration;
import io.trino.decoder.DispatchingRowDecoderFactory;
import io.trino.decoder.RowDecoder;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.RecordSet;

import java.util.HashMap;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

public class KinesisRecordSetProvider
        implements ConnectorRecordSetProvider
{
    private final KinesisClientProvider clientManager;
    private final long dynamoReadCapacity;
    private final long dynamoWriteCapacity;
    private final boolean isLogBatches;
    private final int fetchAttempts;
    private final Duration sleepTime;
    private final DispatchingRowDecoderFactory decoderFactory;

    @Inject
    public KinesisRecordSetProvider(
            DispatchingRowDecoderFactory decoderFactory,
            KinesisClientProvider clientManager,
            KinesisConfig kinesisConfig)
    {
        this.decoderFactory = requireNonNull(decoderFactory, "decoderFactory is null");
        this.clientManager = requireNonNull(clientManager, "clientManager is null");
        requireNonNull(kinesisConfig, "kinesisConfig is null");
        this.dynamoReadCapacity = kinesisConfig.getDynamoReadCapacity();
        this.dynamoWriteCapacity = kinesisConfig.getDynamoWriteCapacity();
        this.isLogBatches = kinesisConfig.isLogBatches();
        this.fetchAttempts = kinesisConfig.getFetchAttempts();
        this.sleepTime = kinesisConfig.getSleepTime();
    }

    @Override
    public RecordSet getRecordSet(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            List<? extends ColumnHandle> columns)
    {
        KinesisSplit kinesisSplit = (KinesisSplit) split;
        List<KinesisColumnHandle> kinesisColumns = columns.stream()
                .map(x -> (KinesisColumnHandle) x)
                .collect(toImmutableList());

        ImmutableList.Builder<KinesisColumnHandle> handleBuilder = ImmutableList.builder();

        RowDecoder messageDecoder = decoderFactory.create(
                kinesisSplit.getMessageDataFormat(),
                new HashMap<>(),
                kinesisColumns.stream()
                        .filter(column -> !column.isInternal())
                        .collect(toImmutableSet()));

        for (ColumnHandle handle : columns) {
            KinesisColumnHandle columnHandle = (KinesisColumnHandle) handle;
            handleBuilder.add(columnHandle);
        }
        return new KinesisRecordSet(
                kinesisSplit,
                session,
                clientManager,
                handleBuilder.build(),
                messageDecoder,
                dynamoReadCapacity,
                dynamoWriteCapacity,
                isLogBatches,
                fetchAttempts,
                sleepTime);
    }
}
