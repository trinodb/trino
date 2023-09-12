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

import com.google.inject.Inject;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.trino.plugin.kafka.schema.AbstractContentSchemaProvider;
import io.trino.spi.TrinoException;

import java.io.IOException;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class AvroConfluentContentSchemaProvider
        extends AbstractContentSchemaProvider
{
    private final SchemaRegistryClient schemaRegistryClient;

    @Inject
    public AvroConfluentContentSchemaProvider(SchemaRegistryClient schemaRegistryClient)
    {
        this.schemaRegistryClient = requireNonNull(schemaRegistryClient, "schemaRegistryClient is null");
    }

    @Override
    protected Optional<String> readSchema(Optional<String> dataSchemaLocation, Optional<String> subject)
    {
        if (subject.isEmpty()) {
            return Optional.empty();
        }
        checkState(dataSchemaLocation.isEmpty(), "Unexpected parameter: dataSchemaLocation");
        try {
            SchemaMetadata schemaMetadata = schemaRegistryClient.getLatestSchemaMetadata(subject.get());
            ParsedSchema schema = schemaRegistryClient.getSchemaBySubjectAndId(subject.get(), schemaMetadata.getId());
            return Optional.ofNullable(schema.rawSchema())
                    .map(Object::toString);
        }
        catch (IOException | RestClientException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Could not resolve schema for the '%s' subject", subject.get()), e);
        }
    }
}
