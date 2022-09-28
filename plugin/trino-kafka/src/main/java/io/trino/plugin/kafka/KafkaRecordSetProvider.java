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
package io.trino.plugin.kafka;

import com.google.common.collect.ImmutableMap;
import io.trino.decoder.DispatchingRowDecoderFactory;
import io.trino.decoder.RowDecoder;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.RecordSet;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

public class KafkaRecordSetProvider
        implements ConnectorRecordSetProvider
{
    private final DispatchingRowDecoderFactory decoderFactory;
    private final KafkaConsumerFactory consumerFactory;

    @Inject
    public KafkaRecordSetProvider(DispatchingRowDecoderFactory decoderFactory, KafkaConsumerFactory consumerFactory)
    {
        this.decoderFactory = requireNonNull(decoderFactory, "decoderFactory is null");
        this.consumerFactory = requireNonNull(consumerFactory, "consumerFactory is null");
    }

    @Override
    public RecordSet getRecordSet(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorSplit split, ConnectorTableHandle table, List<? extends ColumnHandle> columns)
    {
        KafkaSplit kafkaSplit = (KafkaSplit) split;

        List<KafkaColumnHandle> kafkaColumns = columns.stream()
                .map(KafkaColumnHandle.class::cast)
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

        return new KafkaRecordSet(kafkaSplit, consumerFactory, session, kafkaColumns, keyDecoder, messageDecoder);
    }

    private static Map<String, String> getDecoderParameters(Optional<String> dataSchema)
    {
        ImmutableMap.Builder<String, String> parameters = ImmutableMap.builder();
        dataSchema.ifPresent(schema -> parameters.put("dataSchema", schema));
        return parameters.buildOrThrow();
    }
}
