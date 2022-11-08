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
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.protobuf.MessageIndexes;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.trino.collect.cache.NonEvictableLoadingCache;
import io.trino.decoder.protobuf.DynamicMessageProvider;
import io.trino.spi.TrinoException;

import javax.inject.Inject;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.collect.cache.SafeCaches.buildNonEvictableCache;
import static io.trino.decoder.protobuf.ProtobufErrorCode.INVALID_PROTOBUF_MESSAGE;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ConfluentSchemaRegistryDynamicMessageProvider
        implements DynamicMessageProvider
{
    private static final int MAGIC_BYTE = 0;
    private final SchemaRegistryClient schemaRegistryClient;
    private final NonEvictableLoadingCache<Integer, Descriptor> descriptorCache;

    public ConfluentSchemaRegistryDynamicMessageProvider(SchemaRegistryClient schemaRegistryClient)
    {
        this.schemaRegistryClient = requireNonNull(schemaRegistryClient, "schemaRegistryClient is null");
        descriptorCache = buildNonEvictableCache(
                CacheBuilder.newBuilder().maximumSize(1000),
                CacheLoader.from(this::lookupDescriptor));
    }

    @Override
    public DynamicMessage parseDynamicMessage(byte[] data)
    {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        byte magicByte = buffer.get();
        checkArgument(magicByte == MAGIC_BYTE, "Invalid MagicByte");
        int schemaId = buffer.getInt();
        MessageIndexes.readFrom(buffer);
        try {
            return DynamicMessage.parseFrom(
                    descriptorCache.getUnchecked(schemaId),
                    wrappedBuffer(buffer).getInput());
        }
        catch (IOException e) {
            throw new TrinoException(INVALID_PROTOBUF_MESSAGE, "Decoding Protobuf record failed.", e);
        }
    }

    private Descriptor lookupDescriptor(int schemaId)
    {
        try {
            ParsedSchema schema = schemaRegistryClient.getSchemaById(schemaId);
            checkArgument(schema instanceof ProtobufSchema, "schema should be an instance of ProtobufSchema");
            return ((ProtobufSchema) schema).toDescriptor();
        }
        catch (IOException | RestClientException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Looking up schemaId '%s'from confluent schema registry failed", schemaId), e);
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
            return new ConfluentSchemaRegistryDynamicMessageProvider(schemaRegistryClient);
        }
    }
}
