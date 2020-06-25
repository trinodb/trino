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
import com.google.common.collect.ImmutableSet;
import io.prestosql.plugin.kafka.encoder.DispatchingRowEncoderFactory;
import io.prestosql.plugin.kafka.encoder.EncoderColumnHandle;
import io.prestosql.plugin.kafka.encoder.RowEncoder;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorInsertTableHandle;
import io.prestosql.spi.connector.ConnectorOutputTableHandle;
import io.prestosql.spi.connector.ConnectorPageSink;
import io.prestosql.spi.connector.ConnectorPageSinkProvider;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTransactionHandle;

import javax.inject.Inject;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class KafkaPageSinkProvider
        implements ConnectorPageSinkProvider
{
    private final DispatchingRowEncoderFactory encoderFactory;
    private final PlainTextKafkaProducerFactory producerFactory;

    @Inject
    public KafkaPageSinkProvider(DispatchingRowEncoderFactory encoderFactory, PlainTextKafkaProducerFactory producerFactory)
    {
        this.encoderFactory = requireNonNull(encoderFactory, "encoderFactory is null");
        this.producerFactory = requireNonNull(producerFactory, "producerFactory is null");
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle tableHandle)
    {
        throw new UnsupportedOperationException("Table creation is not supported by the kafka connector");
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle tableHandle)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        checkArgument(tableHandle instanceof KafkaTableHandle, "tableHandle is not an instance of KafkaTableHandle");
        KafkaTableHandle handle = (KafkaTableHandle) tableHandle;

        ImmutableSet.Builder<EncoderColumnHandle> keyColumns = ImmutableSet.builder();
        ImmutableSet.Builder<EncoderColumnHandle> messageColumns = ImmutableSet.builder();
        handle.getColumns().forEach(col -> {
            if (col.isInternal()) {
                throw new PrestoException(GENERIC_USER_ERROR, "unexpected internal column");
            }
            if (col.isKeyDecoder()) {
                keyColumns.add(col);
            }
            else {
                messageColumns.add(col);
            }
        });

        RowEncoder keyEncoder = encoderFactory.create(
                handle.getKeyDataFormat(),
                ImmutableMap.of("dataSchema", getDataSchema(handle.getKeyDataSchemaLocation())),
                keyColumns.build());

        RowEncoder messageEncoder = encoderFactory.create(
                handle.getMessageDataFormat(),
                ImmutableMap.of("dataSchema", getDataSchema(handle.getMessageDataSchemaLocation())),
                messageColumns.build());

        return new KafkaPageSink(
                handle.getTopicName(),
                handle.getColumns(),
                keyEncoder,
                messageEncoder,
                session,
                producerFactory);
    }

    private String getDataSchema(Optional<String> dataSchemaLocation)
    {
        String dataSchema = "";
        try {
            if (dataSchemaLocation.isPresent()) {
                dataSchema = Files.readString(Paths.get(dataSchemaLocation.get()));
            }
        }
        catch (IOException e) {
            throw new PrestoException(GENERIC_USER_ERROR, format("Unable to read data schema at '%s'", dataSchemaLocation.get()), e);
        }
        return dataSchema;
    }
}
