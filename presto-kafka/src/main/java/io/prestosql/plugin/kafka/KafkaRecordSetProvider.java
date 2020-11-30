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

import com.google.common.collect.ImmutableMap;
import io.prestosql.decoder.DispatchingRowDecoderFactory;
import io.prestosql.decoder.RowDecoder;
import io.prestosql.decoder.RowDecoderFactory;
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
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.prestosql.plugin.kafka.KafkaHandleResolver.convertSplit;
import static java.util.Objects.requireNonNull;

public class KafkaRecordSetProvider
        implements ConnectorRecordSetProvider
{
    private final DispatchingRowDecoderFactory decoderFactory;
    private final KafkaConsumerFactory consumerFactory;
    private final Optional<RowDecoderFactory> internalKeyFieldDecoderFactory;
    private final Optional<RowDecoderFactory> internalMessageFieldDecoderFactory;
    private final KafkaInternalFieldManager internalFieldManager;

    @Inject
    public KafkaRecordSetProvider(DispatchingRowDecoderFactory decoderFactory, KafkaConsumerFactory consumerFactory, @ForInternalKeyField Optional<RowDecoderFactory> internalKeyFieldDecoderFactory, @ForInternalMessageField Optional<RowDecoderFactory> internalMessageFieldDecoderFactory, KafkaInternalFieldManager internalFieldManager)
    {
        this.decoderFactory = requireNonNull(decoderFactory, "decoderFactory is null");
        this.consumerFactory = requireNonNull(consumerFactory, "consumerManager is null");
        this.internalKeyFieldDecoderFactory = requireNonNull(internalKeyFieldDecoderFactory, "internalKeyFieldDecoderFactory is null");
        this.internalMessageFieldDecoderFactory = requireNonNull(internalMessageFieldDecoderFactory, "internalMessageFieldDecoderFactory is null");
        this.internalFieldManager = requireNonNull(internalFieldManager, "internalFieldManager is null");
    }

    @Override
    public RecordSet getRecordSet(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorSplit split, ConnectorTableHandle table, List<? extends ColumnHandle> columns)
    {
        KafkaSplit kafkaSplit = convertSplit(split);

        List<KafkaColumnHandle> kafkaColumns = columns.stream()
                .map(KafkaHandleResolver::convertColumnHandle)
                .collect(toImmutableList());

        RowDecoder keyDecoder = decoderFactory.create(
                kafkaSplit.getKeyDataFormat(),
                getDecoderParameters(kafkaSplit.getKeyDataSchemaContents()),
                kafkaColumns.stream()
                        .filter(col -> !col.isInternal())
                        .filter(KafkaColumnHandle::isKeyCodec)
                        .collect(toImmutableSet()));

        RowDecoder messageDecoder = decoderFactory.create(
                kafkaSplit.getMessageDataFormat(),
                getDecoderParameters(kafkaSplit.getMessageDataSchemaContents()),
                kafkaColumns.stream()
                        .filter(col -> !col.isInternal())
                        .filter(col -> !col.isKeyCodec())
                        .collect(toImmutableSet()));
        Optional<RowDecoder> internalKeyFieldDecoder = internalKeyFieldDecoderFactory.map(factory -> factory.create(getInternalDecoderParameters(kafkaSplit.getKeyDataFormat()), kafkaColumns.stream()
                .filter(col -> col.isInternal())
                .collect(toImmutableSet())));
        Optional<RowDecoder> internalMessageFieldDecoder = internalMessageFieldDecoderFactory.map(factory -> factory.create(getInternalDecoderParameters(kafkaSplit.getMessageDataFormat()), kafkaColumns.stream()
                .filter(col -> col.isInternal())
                .collect(toImmutableSet())));

        return new KafkaRecordSet(kafkaSplit, consumerFactory, kafkaColumns, keyDecoder, messageDecoder, internalKeyFieldDecoder, internalMessageFieldDecoder, internalFieldManager);
    }

    private Map<String, String> getDecoderParameters(Optional<String> dataSchema)
    {
        ImmutableMap.Builder<String, String> parameters = ImmutableMap.builder();
        dataSchema.ifPresent(schema -> parameters.put("dataSchema", schema));
        return parameters.build();
    }

    private Map<String, String> getInternalDecoderParameters(String dataFormat)
    {
        return ImmutableMap.<String, String>builder()
                .put("dataFormat", dataFormat)
                .build();
    }
}
