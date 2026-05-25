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
package io.trino.plugin.kafka.schema.rest;

import com.google.common.cache.Cache;
import com.google.common.io.CharStreams;
import com.google.inject.Inject;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.trino.cache.EvictableCacheBuilder;
import io.trino.plugin.kafka.schema.AbstractContentSchemaProvider;
import io.trino.spi.TrinoException;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URI;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

/**
 * Resolves Avro schema for the REST table description supplier. When dataSchemaLocation is set,
 * reads from file or URL. When only subject is set (e.g. Schema Registry), fetches schema from
 * the registry. Used for both read (decoder) and write (encoder) paths so "dataSchema cannot be null"
 * is avoided when reading with subject-only table definitions.
 */
public class RestContentSchemaProvider
        extends AbstractContentSchemaProvider
{
    private static final int SCHEMA_BY_SUBJECT_CACHE_MAX_SIZE = 1000;

    private final SchemaRegistryClient schemaRegistryClient;
    private final Cache<String, Optional<String>> schemaBySubjectCache = EvictableCacheBuilder.newBuilder()
            .maximumSize(SCHEMA_BY_SUBJECT_CACHE_MAX_SIZE)
            .build();

    @Inject
    public RestContentSchemaProvider(SchemaRegistryClient schemaRegistryClient)
    {
        this.schemaRegistryClient = requireNonNull(schemaRegistryClient, "schemaRegistryClient is null");
    }

    @Override
    protected Optional<String> readSchema(Optional<String> dataSchemaLocation, Optional<String> subject)
    {
        if (dataSchemaLocation.isPresent()) {
            return readFromLocation(dataSchemaLocation.get());
        }
        if (subject.isPresent()) {
            try {
                return schemaBySubjectCache.get(subject.get(), () -> fetchFromSchemaRegistry(subject.get()));
            }
            catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof TrinoException) {
                    throw (TrinoException) cause;
                }
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Could not resolve schema for subject: " + subject.get(), cause);
            }
        }
        return Optional.empty();
    }

    private Optional<String> readFromLocation(String location)
    {
        try (InputStream inputStream = openSchemaLocation(location)) {
            return Optional.of(CharStreams.toString(new InputStreamReader(inputStream, UTF_8)));
        }
        catch (IOException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Could not read schema from: " + location, e);
        }
    }

    private Optional<String> fetchFromSchemaRegistry(String subject)
    {
        try {
            SchemaMetadata metadata = schemaRegistryClient.getLatestSchemaMetadata(subject);
            ParsedSchema schema = schemaRegistryClient.getSchemaBySubjectAndId(subject, metadata.getId());
            return Optional.ofNullable(schema.rawSchema()).map(Object::toString);
        }
        catch (IOException | RestClientException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Could not resolve schema for subject: " + subject, e);
        }
    }

    private static InputStream openSchemaLocation(String dataSchemaLocation)
            throws IOException
    {
        if (isUrl(dataSchemaLocation.trim().toLowerCase(ENGLISH))) {
            try {
                return URI.create(dataSchemaLocation).toURL().openStream();
            }
            catch (MalformedURLException ignored) {
                // fall through to file
            }
        }
        return new FileInputStream(dataSchemaLocation);
    }

    private static boolean isUrl(String location)
    {
        try {
            URI.create(location).toURL();
            return true;
        }
        catch (Exception e) {
            return false;
        }
    }
}
