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
package io.prestosql.plugin.kafka;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.decoder.DispatchingRowDecoderFactory;
import io.prestosql.decoder.RowDecoder;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorRecordSetProvider;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.RecordSet;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.prestosql.plugin.kafka.KafkaHandleResolver.convertSplit;
import static java.util.Objects.requireNonNull;

/**
 * Factory for Kafka specific {@link RecordSet} instances.
 */
public class KafkaRecordSetProvider
        implements ConnectorRecordSetProvider
{
    private DispatchingRowDecoderFactory decoderFactory;
    private final KafkaSimpleConsumerManager consumerManager;

    @Inject
    public KafkaRecordSetProvider(DispatchingRowDecoderFactory decoderFactory, KafkaSimpleConsumerManager consumerManager)
    {
        this.decoderFactory = requireNonNull(decoderFactory, "decoderFactory is null");
        this.consumerManager = requireNonNull(consumerManager, "consumerManager is null");
    }

    @Override
    public RecordSet getRecordSet(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorSplit split, ConnectorTableHandle table, List<? extends ColumnHandle> columns)
    {
        KafkaSplit kafkaSplit = convertSplit(split);

        List<KafkaColumnHandle> kafkaColumns = columns.stream()
                .map(KafkaHandleResolver::convertColumnHandle)
                .collect(ImmutableList.toImmutableList());

        RowDecoder keyDecoder = decoderFactory.create(
                kafkaSplit.getKeyDataFormat(),
                getKeyDecoderParameters(kafkaSplit),
                kafkaColumns.stream()
                        .filter(col -> !col.isInternal())
                        .filter(KafkaColumnHandle::isKeyDecoder)
                        .collect(toImmutableSet()));

        RowDecoder messageDecoder = decoderFactory.create(
                kafkaSplit.getMessageDataFormat(),
                getMessageDecoderParameters(kafkaSplit),
                kafkaColumns.stream()
                        .filter(col -> !col.isInternal())
                        .filter(col -> !col.isKeyDecoder())
                        .collect(toImmutableSet()));

        return new KafkaRecordSet(kafkaSplit, consumerManager, kafkaColumns, keyDecoder, messageDecoder);
    }

    private Map<String, String> getKeyDecoderParameters(KafkaSplit kafkaSplit)
    {
        ImmutableMap.Builder<String, String> parameters = ImmutableMap.builder();
        kafkaSplit.getKeyDataSchemaContents().ifPresent(schema -> parameters.put("dataSchema", schema));
        kafkaSplit.getKeyDataReaderProvider().ifPresent(provider -> parameters.put("dataReaderProvider", provider));
        kafkaSplit.getConfluentSchemaRegistryUrl().ifPresent(url -> parameters.put("confluentSchemaRegistryUrl", url));
        return parameters.build();
    }

    private Map<String, String> getMessageDecoderParameters(KafkaSplit kafkaSplit)
    {
        ImmutableMap.Builder<String, String> parameters = ImmutableMap.builder();
        kafkaSplit.getMessageDataSchemaContents().ifPresent(schema -> parameters.put("dataSchema", schema));
        kafkaSplit.getMessageDataReaderProvider().ifPresent(provider -> parameters.put("dataReaderProvider", provider));
        kafkaSplit.getConfluentSchemaRegistryUrl().ifPresent(url -> parameters.put("confluentSchemaRegistryUrl", url));
        return parameters.build();
    }
}
