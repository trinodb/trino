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

import com.google.common.collect.ImmutableList;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.client.SchemaVersionFetcher;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;

import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/**
 * Parses {@code PROTOBUF} schemas fetched by the schema registry client into {@link ConfluentProtobufSchema}s.
 * The client hands each instance the {@link SchemaVersionFetcher} used to resolve schema references,
 * so one instance belongs to exactly one client.
 */
public class ConfluentProtobufSchemaProvider
        implements SchemaProvider
{
    private SchemaVersionFetcher schemaVersionFetcher;

    @Override
    public String schemaType()
    {
        return ConfluentProtobufSchema.TYPE;
    }

    @Override
    public synchronized void configure(Map<String, ?> configs)
    {
        checkState(schemaVersionFetcher == null, "%s is already configured", getClass().getSimpleName());
        Object fetcher = configs.get(SCHEMA_VERSION_FETCHER_CONFIG);
        checkArgument(fetcher instanceof SchemaVersionFetcher, "%s is not a SchemaVersionFetcher: %s", SCHEMA_VERSION_FETCHER_CONFIG, fetcher);
        schemaVersionFetcher = (SchemaVersionFetcher) fetcher;
    }

    @Override
    public ParsedSchema parseSchemaOrElseThrow(Schema schema, boolean isNew, boolean normalize)
    {
        requireNonNull(schema, "schema is null");
        checkArgument(schema.getSchema() != null, "schema text is null");
        return new ConfluentProtobufSchema(
                schema.getSchema(),
                Optional.ofNullable(schema.getReferences()).orElse(ImmutableList.of()),
                schema.getVersion(),
                schema.getMetadata(),
                schema.getRuleSet(),
                getSchemaVersionFetcher());
    }

    private synchronized SchemaVersionFetcher getSchemaVersionFetcher()
    {
        return schemaVersionFetcher;
    }
}
