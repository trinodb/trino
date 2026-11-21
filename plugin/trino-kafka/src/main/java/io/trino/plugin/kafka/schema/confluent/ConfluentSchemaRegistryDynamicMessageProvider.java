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
package io.trino.plugin.kafka.schema.confluent;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.inject.Inject;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.trino.cache.NonEvictableLoadingCache;
import io.trino.decoder.protobuf.DynamicMessageProvider;
import io.trino.plugin.kafka.schema.confluent.ConfluentProtobufWireFormat.Frame;
import io.trino.spi.TrinoException;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static io.trino.cache.SafeCaches.buildNonEvictableCache;
import static io.trino.decoder.protobuf.ProtobufErrorCode.INVALID_PROTOBUF_MESSAGE;
import static io.trino.plugin.kafka.KafkaErrorCode.KAFKA_SCHEMA_ERROR;
import static io.trino.plugin.kafka.KafkaErrorCode.KAFKA_SCHEMA_REGISTRY_ERROR;
import static java.lang.String.format;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.util.Objects.requireNonNull;

/**
 * Decodes records in the Confluent wire format with the schema they were written with: the
 * schema id and message indexes in the frame select the message type, which is fetched from
 * the schema registry and cached. Ids are immutable in the registry, so the cache has no TTL.
 * <p>
 * The reader schema, which the table columns were derived from, is the message the data schema
 * parameter names within a registry schema; see {@link #readerSchemaReference(int, Descriptor)}.
 */
public class ConfluentSchemaRegistryDynamicMessageProvider
        implements DynamicMessageProvider
{
    private static final Pattern READER_SCHEMA_REFERENCE = Pattern.compile("schema-registry-id:(\\d+);message=([^;]+)");

    private final SchemaRegistryClient schemaRegistryClient;
    private final Optional<ReaderSchema> readerSchema;
    private final NonEvictableLoadingCache<MessageKey, Descriptor> descriptorCache;

    public ConfluentSchemaRegistryDynamicMessageProvider(SchemaRegistryClient schemaRegistryClient)
    {
        this(schemaRegistryClient, Optional.empty());
    }

    public ConfluentSchemaRegistryDynamicMessageProvider(SchemaRegistryClient schemaRegistryClient, Optional<String> readerSchemaReference)
    {
        this.schemaRegistryClient = requireNonNull(schemaRegistryClient, "schemaRegistryClient is null");
        this.readerSchema = requireNonNull(readerSchemaReference, "readerSchemaReference is null").map(ReaderSchema::parse);
        descriptorCache = buildNonEvictableCache(
                CacheBuilder.newBuilder().maximumSize(1000),
                CacheLoader.from(this::lookupDescriptor));
    }

    /**
     * The data schema of a table whose columns were derived from the given message of a registry
     * schema. A protobuf schema is compiled together with its references, which only the registry
     * has, so the table carries the schema's id and the message's name rather than the schema text.
     */
    public static String readerSchemaReference(int schemaId, Descriptor message)
    {
        return format("schema-registry-id:%s;message=%s", schemaId, message.getFullName());
    }

    @Override
    public Optional<Descriptor> getReaderDescriptor()
    {
        return readerSchema.map(reader -> getSchema(reader.schemaId()).findMessage(reader.messageName())
                .orElseThrow(() -> new TrinoException(KAFKA_SCHEMA_ERROR, format("Schema id %s does not declare message %s", reader.schemaId(), reader.messageName()))));
    }

    @Override
    public DynamicMessage parseDynamicMessage(byte[] data)
    {
        Frame frame = ConfluentProtobufWireFormat.readFrame(data);
        Descriptor descriptor = getDescriptor(new MessageKey(frame.schemaId(), frame.messageIndexes()));
        try {
            return DynamicMessage.parseFrom(descriptor, ByteString.copyFrom(data, frame.payloadOffset(), data.length - frame.payloadOffset()));
        }
        catch (InvalidProtocolBufferException e) {
            throw new TrinoException(INVALID_PROTOBUF_MESSAGE, "Decoding Protobuf record failed.", e);
        }
    }

    private Descriptor getDescriptor(MessageKey key)
    {
        try {
            return descriptorCache.getUnchecked(key);
        }
        catch (UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), TrinoException.class);
            throw e;
        }
    }

    private Descriptor lookupDescriptor(MessageKey key)
    {
        return ConfluentProtobufWireFormat.findMessage(getSchema(key.schemaId()).getFileDescriptor(), key.messageIndexes())
                .orElseThrow(() -> new TrinoException(INVALID_PROTOBUF_MESSAGE, format("Message indexes %s do not exist in schema id %s", key.messageIndexes(), key.schemaId())));
    }

    // the registry client caches schemas by id, and a schema compiles its descriptors once
    private ConfluentProtobufSchema getSchema(int schemaId)
    {
        ParsedSchema schema;
        try {
            schema = schemaRegistryClient.getSchemaById(schemaId);
        }
        catch (RestClientException e) {
            if (e.getStatus() == HTTP_NOT_FOUND) {
                throw new TrinoException(KAFKA_SCHEMA_ERROR, format("Schema id %s does not exist in the schema registry", schemaId), e);
            }
            throw new TrinoException(KAFKA_SCHEMA_REGISTRY_ERROR, format("Failed to look up schema id %s in the schema registry: %s", schemaId, e.getMessage()), e);
        }
        catch (IOException e) {
            throw new TrinoException(KAFKA_SCHEMA_REGISTRY_ERROR, format("Failed to look up schema id %s in the schema registry: %s", schemaId, e.getMessage()), e);
        }
        if (!(schema instanceof ConfluentProtobufSchema protobufSchema)) {
            throw new TrinoException(KAFKA_SCHEMA_ERROR, format("Schema id %s is a %s schema, not a PROTOBUF schema", schemaId, schema.schemaType()));
        }
        return protobufSchema;
    }

    private record MessageKey(int schemaId, List<Integer> messageIndexes)
    {
        private MessageKey
        {
            messageIndexes = ImmutableList.copyOf(requireNonNull(messageIndexes, "messageIndexes is null"));
        }
    }

    /**
     * The registry schema id and the full name of the message a table's columns were derived from.
     */
    public record ReaderSchema(int schemaId, String messageName)
    {
        public ReaderSchema
        {
            requireNonNull(messageName, "messageName is null");
        }

        /**
         * Parses a reference created by {@link #readerSchemaReference(int, Descriptor)}.
         */
        public static ReaderSchema parse(String reference)
        {
            Matcher matcher = READER_SCHEMA_REFERENCE.matcher(reference);
            checkArgument(matcher.matches(), "Data schema is not a schema registry reference: %s", reference);
            try {
                return new ReaderSchema(Integer.parseInt(matcher.group(1)), matcher.group(2));
            }
            catch (NumberFormatException e) {
                throw new IllegalArgumentException(format("Data schema is not a schema registry reference: %s", reference), e);
            }
        }
    }

    public static class Factory
            implements DynamicMessageProvider.Factory
    {
        private final SchemaRegistryClient schemaRegistryClient;

        @Inject
        public Factory(SchemaRegistryClient schemaRegistryClient)
        {
            this.schemaRegistryClient = requireNonNull(schemaRegistryClient, "schemaRegistryClient is null");
        }

        @Override
        public DynamicMessageProvider create(Optional<String> protoFile)
        {
            return new ConfluentSchemaRegistryDynamicMessageProvider(schemaRegistryClient, protoFile);
        }
    }
}
