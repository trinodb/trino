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
package io.trino.plugin.kafka.encoder.protobuf;

import com.google.inject.Inject;
import com.google.protobuf.Descriptors.Descriptor;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.trino.plugin.kafka.encoder.RowEncoder;
import io.trino.plugin.kafka.encoder.RowEncoderFactory;
import io.trino.plugin.kafka.encoder.RowEncoderSpec;
import io.trino.plugin.kafka.schema.confluent.ConfluentProtobufSchema;
import io.trino.plugin.kafka.schema.confluent.ConfluentProtobufWireFormat;
import io.trino.plugin.kafka.schema.confluent.ConfluentSchemaRegistryDynamicMessageProvider.ReaderSchema;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorSession;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static io.trino.plugin.kafka.KafkaErrorCode.KAFKA_SCHEMA_ERROR;
import static io.trino.plugin.kafka.KafkaErrorCode.KAFKA_SCHEMA_REGISTRY_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.String.format;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

/**
 * Encodes rows with the schema the table columns were derived from, or the latest schema
 * registered for the table's subject when the table does not name one, and frames each
 * message in the Confluent wire format, so records written by Trino are readable by any
 * Confluent protobuf consumer.
 */
public class ConfluentProtobufRowEncoderFactory
        implements RowEncoderFactory
{
    private final SchemaRegistryClient schemaRegistryClient;

    @Inject
    public ConfluentProtobufRowEncoderFactory(SchemaRegistryClient schemaRegistryClient)
    {
        this.schemaRegistryClient = requireNonNull(schemaRegistryClient, "schemaRegistryClient is null");
    }

    @Override
    public RowEncoder create(ConnectorSession session, RowEncoderSpec rowEncoderSpec)
    {
        String subject = rowEncoderSpec.subject()
                .orElseThrow(() -> new TrinoException(NOT_SUPPORTED, format("Inserting into the %s of table '%s' requires a schema registry subject", rowEncoderSpec.kafkaFieldType().name().toLowerCase(ENGLISH), rowEncoderSpec.topic())));
        // rows are written with the schema the table columns were derived from, which may not be the latest one by now
        Optional<ReaderSchema> tableSchema = rowEncoderSpec.dataSchema().map(ReaderSchema::parse);
        int schemaId = tableSchema.map(ReaderSchema::schemaId).orElseGet(() -> getLatestSchemaId(subject));
        ConfluentProtobufSchema schema = getSchema(subject, schemaId);
        Descriptor descriptor = tableSchema
                .map(reader -> schema.findMessage(reader.messageName())
                        .orElseThrow(() -> new TrinoException(KAFKA_SCHEMA_ERROR, format("Schema id %s of subject '%s' does not declare message %s", schemaId, subject, reader.messageName()))))
                .orElseGet(() -> schema.getMessageDescriptor(subject));
        return new FramingRowEncoder(schemaId, ConfluentProtobufWireFormat.messageIndexes(descriptor), new ProtobufRowEncoder(descriptor, session, rowEncoderSpec.columnHandles()));
    }

    private int getLatestSchemaId(String subject)
    {
        try {
            SchemaMetadata metadata = schemaRegistryClient.getLatestSchemaMetadata(subject);
            return metadata.getId();
        }
        catch (RestClientException e) {
            if (e.getStatus() == HTTP_NOT_FOUND) {
                throw new TrinoException(KAFKA_SCHEMA_ERROR, format("Subject '%s' does not exist in the schema registry", subject), e);
            }
            throw new TrinoException(KAFKA_SCHEMA_REGISTRY_ERROR, format("Failed to look up the latest schema of subject '%s' in the schema registry: %s", subject, e.getMessage()), e);
        }
        catch (IOException e) {
            throw new TrinoException(KAFKA_SCHEMA_REGISTRY_ERROR, format("Failed to look up the latest schema of subject '%s' in the schema registry: %s", subject, e.getMessage()), e);
        }
    }

    private ConfluentProtobufSchema getSchema(String subject, int schemaId)
    {
        ParsedSchema schema;
        try {
            schema = schemaRegistryClient.getSchemaBySubjectAndId(subject, schemaId);
        }
        catch (RestClientException e) {
            if (e.getStatus() == HTTP_NOT_FOUND) {
                throw new TrinoException(KAFKA_SCHEMA_ERROR, format("Schema id %s of subject '%s' does not exist in the schema registry", schemaId, subject), e);
            }
            throw new TrinoException(KAFKA_SCHEMA_REGISTRY_ERROR, format("Failed to fetch schema id %s of subject '%s' from the schema registry: %s", schemaId, subject, e.getMessage()), e);
        }
        catch (IOException e) {
            throw new TrinoException(KAFKA_SCHEMA_REGISTRY_ERROR, format("Failed to fetch schema id %s of subject '%s' from the schema registry: %s", schemaId, subject, e.getMessage()), e);
        }
        if (!(schema instanceof ConfluentProtobufSchema protobufSchema)) {
            throw new TrinoException(KAFKA_SCHEMA_ERROR, format("Schema id %s of subject '%s' is a %s schema, not a PROTOBUF schema", schemaId, subject, schema.schemaType()));
        }
        return protobufSchema;
    }

    private static class FramingRowEncoder
            implements RowEncoder
    {
        private final int schemaId;
        private final List<Integer> messageIndexes;
        private final RowEncoder delegate;

        private FramingRowEncoder(int schemaId, List<Integer> messageIndexes, RowEncoder delegate)
        {
            this.schemaId = schemaId;
            this.messageIndexes = requireNonNull(messageIndexes, "messageIndexes is null");
            this.delegate = requireNonNull(delegate, "delegate is null");
        }

        @Override
        public void appendColumnValue(Block block, int position)
        {
            delegate.appendColumnValue(block, position);
        }

        @Override
        public byte[] toByteArray()
        {
            return ConfluentProtobufWireFormat.frame(schemaId, messageIndexes, delegate.toByteArray());
        }

        @Override
        public void close()
                throws IOException
        {
            delegate.close();
        }
    }
}
