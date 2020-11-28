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
package io.prestosql.plugin.kafka.schema.confluent;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.prestosql.plugin.kafka.schema.AbstractContentSchemaReader;
import io.prestosql.spi.PrestoException;

import javax.inject.Inject;

import java.io.IOException;
import java.util.Optional;

import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class AvroConfluentContentSchemaReader
        extends AbstractContentSchemaReader
{
    private final SchemaRegistryClient schemaRegistryClient;

    @Inject
    public AvroConfluentContentSchemaReader(SchemaRegistryClient schemaRegistryClient)
    {
        this.schemaRegistryClient = requireNonNull(schemaRegistryClient, "schemaRegistryClient is null");
    }

    @Override
    protected Optional<String> readSchema(Optional<String> dataSchemaLocation, Optional<String> subject)
    {
        if (subject.isPresent()) {
            try {
                return Optional.of(schemaRegistryClient.getLatestSchemaMetadata(subject.get()).getSchema());
            }
            catch (IOException | RestClientException e) {
                throw new PrestoException(GENERIC_INTERNAL_ERROR, format("Could not resolve schema for the '%s' subject", subject.get()), e);
            }
        }
        return Optional.empty();
    }
}
