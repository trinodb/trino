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
import com.google.inject.Inject;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.trino.collect.cache.NonEvictableLoadingCache;
import io.trino.decoder.avro.AvroReaderSupplier;
import io.trino.spi.TrinoException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;

import java.io.IOException;
import java.nio.ByteBuffer;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.collect.cache.SafeCaches.buildNonEvictableCache;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ConfluentAvroReaderSupplier<T>
        implements AvroReaderSupplier<T>
{
    private final Schema targetSchema;
    private final SchemaRegistryClient schemaRegistryClient;
    private final NonEvictableLoadingCache<Integer, GenericDatumReader<T>> avroRecordReaderCache;

    private ConfluentAvroReaderSupplier(Schema targetSchema, SchemaRegistryClient schemaRegistryClient)
    {
        this.targetSchema = requireNonNull(targetSchema, "targetSchema is null");
        this.schemaRegistryClient = requireNonNull(schemaRegistryClient, "schemaRegistryClient is null");
        avroRecordReaderCache = buildNonEvictableCache(
                CacheBuilder.newBuilder().maximumSize(1000),
                CacheLoader.from(this::lookupReader));
    }

    private GenericDatumReader<T> lookupReader(int id)
    {
        try {
            Schema sourceSchema = requireNonNull(schemaRegistryClient.getById(id), "Schema is null");
            return new GenericDatumReader<>(sourceSchema, targetSchema);
        }
        catch (IOException | RestClientException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Looking up schemaId '%s'from confluent schema registry failed", id), e);
        }
    }

    @Override
    public DatumReader<T> get(ByteBuffer buffer)
    {
        checkState(buffer.get() == 0, "Unexpected format");
        int schemaId = buffer.getInt();
        return avroRecordReaderCache.getUnchecked(schemaId);
    }

    public static class Factory
            implements AvroReaderSupplier.Factory
    {
        private final SchemaRegistryClient schemaRegistryClient;

        @Inject
        public Factory(SchemaRegistryClient schemaRegistryClient)
        {
            this.schemaRegistryClient = requireNonNull(schemaRegistryClient, "schemaRegistryClient is null");
        }

        @Override
        public <T> AvroReaderSupplier<T> create(Schema schema)
        {
            return new ConfluentAvroReaderSupplier<>(schema, schemaRegistryClient);
        }
    }
}
