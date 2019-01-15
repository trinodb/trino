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
package com.qubole.presto.kinesis;

import com.facebook.presto.decoder.DispatchingRowDecoderFactory;
import com.facebook.presto.decoder.RowDecoder;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;

import java.util.HashMap;
import java.util.List;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

public class KinesisRecordSetProvider
        implements ConnectorRecordSetProvider
{
    private final KinesisHandleResolver handleResolver;
    private final KinesisClientProvider clientManager;
    private final KinesisConnectorConfig kinesisConnectorConfig;
    private DispatchingRowDecoderFactory decoderFactory;

    @Inject
    public KinesisRecordSetProvider(DispatchingRowDecoderFactory decoderFactory,
            KinesisHandleResolver handleResolver,
            KinesisClientProvider clientManager,
            KinesisConnectorConfig kinesisConnectorConfig)
    {
        this.decoderFactory = requireNonNull(decoderFactory, "decoderFactory is null");
        this.handleResolver = requireNonNull(handleResolver, "handleResolver is null");
        this.clientManager = requireNonNull(clientManager, "clientManager is null");
        this.kinesisConnectorConfig = requireNonNull(kinesisConnectorConfig, "kinesisConnectorConfig is null");
    }

    @Override
    public RecordSet getRecordSet(ConnectorTransactionHandle transactionHandle, ConnectorSession session,
            ConnectorSplit split, List<? extends ColumnHandle> columns)
    {
        KinesisSplit kinesisSplit = handleResolver.convertSplit(split);
        List<KinesisColumnHandle> kinesisColumns = columns.stream()
                .map(KinesisHandleResolver::convertColumnHandle)
                .collect(ImmutableList.toImmutableList());

        ImmutableList.Builder<KinesisColumnHandle> handleBuilder = ImmutableList.builder();

        RowDecoder messageDecoder = decoderFactory.create(
                kinesisSplit.getMessageDataFormat(),
                new HashMap<>(),
                kinesisColumns.stream()
                        .filter(col -> !col.isInternal())
                        .collect(toImmutableSet()));

        for (ColumnHandle handle : columns) {
            KinesisColumnHandle columnHandle = handleResolver.convertColumnHandle(handle);
            handleBuilder.add(columnHandle);
        }

        ImmutableList<KinesisColumnHandle> handles = handleBuilder.build();
        return new KinesisRecordSet(kinesisSplit, session, clientManager, handles, messageDecoder, kinesisConnectorConfig);
    }
}
