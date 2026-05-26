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
import com.google.common.collect.ImmutableList;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.trino.cache.EvictableCacheBuilder;
import io.trino.plugin.kafka.KafkaTopicDescription;
import io.trino.plugin.kafka.KafkaTopicFieldDescription;
import io.trino.plugin.kafka.KafkaTopicFieldGroup;
import io.trino.plugin.kafka.schema.confluent.AvroSchemaConverter;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import org.apache.avro.Schema;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.plugin.kafka.schema.confluent.EmptyFieldStrategy.IGNORE;
import static java.util.Objects.requireNonNull;

/**
 * Enriches KafkaTopicDescription by fetching Avro schemas from Schema Registry when
 * key or message has dataFormat=avro and subject (or topic-derived subject) but empty fields.
 * Makes "fields" optional when Schema Registry is configured.
 */
public class RestTableDescriptionEnricher
{
    private static final String KEY_SUFFIX = "-key";
    private static final String VALUE_SUFFIX = "-value";
    private static final int ENRICHED_FIELDS_CACHE_MAX_SIZE = 500;

    private final SchemaRegistryClient schemaRegistryClient;
    private final TypeManager typeManager;
    private final AvroSchemaConverter schemaConverter;
    private final Cache<String, List<KafkaTopicFieldDescription>> fieldsBySubjectCache = EvictableCacheBuilder.newBuilder()
            .maximumSize(ENRICHED_FIELDS_CACHE_MAX_SIZE)
            .build();

    public RestTableDescriptionEnricher(SchemaRegistryClient schemaRegistryClient, TypeManager typeManager)
    {
        this.schemaRegistryClient = requireNonNull(schemaRegistryClient, "schemaRegistryClient is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.schemaConverter = new AvroSchemaConverter(typeManager, IGNORE);
    }

    public KafkaTopicDescription enrich(ConnectorSession session, KafkaTopicDescription description)
    {
        Optional<KafkaTopicFieldGroup> key = description.key();
        Optional<KafkaTopicFieldGroup> message = description.message();
        String topic = description.topicName();

        if (key.isPresent() && needsEnrichment(key.get())) {
            key = Optional.of(enrichFieldGroup(key.get(), topic + KEY_SUFFIX, key.get().subject().orElse(topic + KEY_SUFFIX)));
        }
        if (message.isPresent() && needsEnrichment(message.get())) {
            message = Optional.of(enrichFieldGroup(message.get(), topic + VALUE_SUFFIX, message.get().subject().orElse(topic + VALUE_SUFFIX)));
        }

        if (key.equals(description.key()) && message.equals(description.message())) {
            return description;
        }
        return new KafkaTopicDescription(
                description.tableName(),
                description.schemaName(),
                description.topicName(),
                key,
                message);
    }

    private static boolean needsEnrichment(KafkaTopicFieldGroup group)
    {
        return "avro".equalsIgnoreCase(group.dataFormat()) && group.fields().isEmpty();
    }

    private KafkaTopicFieldGroup enrichFieldGroup(KafkaTopicFieldGroup group, String defaultSubject, String subject)
    {
        List<KafkaTopicFieldDescription> fields;
        try {
            fields = fieldsBySubjectCache.get(subject, () -> loadFieldsForSubject(subject));
        }
        catch (ExecutionException e) {
            return group;
        }
        return new KafkaTopicFieldGroup(
                group.dataFormat(),
                Optional.empty(),
                Optional.of(subject),
                fields);
    }

    private List<KafkaTopicFieldDescription> loadFieldsForSubject(String subject)
            throws IOException, RestClientException
    {
        SchemaMetadata metadata = schemaRegistryClient.getLatestSchemaMetadata(subject);
        ParsedSchema parsed = schemaRegistryClient.getSchemaBySubjectAndId(subject, metadata.getId());
        if (!(parsed instanceof AvroSchema)) {
            throw new IOException("Subject does not have an Avro schema: " + subject);
        }
        Schema schema = ((AvroSchema) parsed).rawSchema();
        return buildFieldsFromAvroSchema(schema);
    }

    private List<KafkaTopicFieldDescription> buildFieldsFromAvroSchema(Schema schema)
    {
        List<Type> types = schemaConverter.convertAvroSchema(schema);
        ImmutableList.Builder<KafkaTopicFieldDescription> builder = ImmutableList.builder();
        if (schema.getType() != Schema.Type.RECORD) {
            builder.add(new KafkaTopicFieldDescription(
                    schema.getFullName(),
                    getOnlyElement(types),
                    schema.getFullName(),
                    null,
                    null,
                    null,
                    false));
        }
        else {
            List<Schema.Field> avroFields = schema.getFields();
            for (int i = 0; i < avroFields.size(); i++) {
                Schema.Field field = avroFields.get(i);
                builder.add(new KafkaTopicFieldDescription(
                        field.name(),
                        types.get(i),
                        field.name(),
                        null,
                        null,
                        null,
                        false));
            }
        }
        return builder.build();
    }
}
