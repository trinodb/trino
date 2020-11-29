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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.Duration;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.prestosql.decoder.avro.AvroRowDecoderFactory;
import io.prestosql.plugin.kafka.KafkaConfig;
import io.prestosql.plugin.kafka.KafkaTopicDescription;
import io.prestosql.plugin.kafka.KafkaTopicFieldDescription;
import io.prestosql.plugin.kafka.KafkaTopicFieldGroup;
import io.prestosql.plugin.kafka.schema.TableDescriptionSupplier;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import org.apache.avro.Schema;

import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Provider;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Suppliers.memoizeWithExpiration;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.prestosql.plugin.kafka.schema.confluent.ConfluentSessionProperties.getEmptyFieldStrategy;
import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class ConfluentSchemaRegistryTableDescriptionSupplier
        implements TableDescriptionSupplier
{
    public static final String NAME = "confluent";

    private static final String DELIMITER = "&";
    private static final String SEPARATOR = "=";

    @VisibleForTesting
    static final String KEY_SUBJECT = "key-subject";

    @VisibleForTesting
    static final String MESSAGE_SUBJECT = "message-subject";

    private static final String KEY_SUFFIX = "-key";
    private static final String VALUE_SUFFIX = "-value";

    private final SchemaRegistryClient schemaRegistryClient;
    private final String defaultSchema;
    private final ExecutorService subjectsCacheExecutor;
    private final Supplier<Map<String, TopicAndSubjects>> topicAndSubjectsSupplier;
    private final TypeManager typeManager;

    public ConfluentSchemaRegistryTableDescriptionSupplier(SchemaRegistryClient schemaRegistryClient, String defaultSchema, Duration subjectsCacheRefreshInterval, TypeManager typeManager)
    {
        requireNonNull(typeManager, "typeManager is null");
        this.schemaRegistryClient = requireNonNull(schemaRegistryClient, "schemaRegistryClient is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.defaultSchema = requireNonNull(defaultSchema, "defaultSchema is null");
        subjectsCacheExecutor = newCachedThreadPool(daemonThreadsNamed("kafka-confluent-subjects-cache-refresher"));
        topicAndSubjectsSupplier = memoizeWithExpiration(this::refreshSubjects, subjectsCacheRefreshInterval.toMillis(), MILLISECONDS);
    }

    @PreDestroy
    public void stop()
    {
        subjectsCacheExecutor.shutdownNow();
    }

    public static class Factory
            implements Provider<TableDescriptionSupplier>
    {
        private final String defaultSchema;
        private final Duration subjectsCacheRefreshInterval;
        private final TypeManager typeManager;
        private final SchemaRegistryClient schemaRegistryClient;

        @Inject
        public Factory(KafkaConfig kafkaConfig, ConfluentSchemaRegistryConfig confluentConfig, TypeManager typeManager, SchemaRegistryClient schemaRegistryClient)
        {
            requireNonNull(kafkaConfig, "kafkaConfig is null");
            requireNonNull(typeManager, "typeManager is null");
            this.schemaRegistryClient = requireNonNull(schemaRegistryClient, "schemaRegistryClient is null");
            this.defaultSchema = kafkaConfig.getDefaultSchema();
            this.subjectsCacheRefreshInterval = confluentConfig.getConfluentSubjectsCacheRefreshInterval();
            this.typeManager = typeManager;
        }

        @Override
        public TableDescriptionSupplier get()
        {
            return new ConfluentSchemaRegistryTableDescriptionSupplier(schemaRegistryClient, defaultSchema, subjectsCacheRefreshInterval, typeManager);
        }
    }

    // Refresh mapping of topic to subjects.
    // Note that this mapping only supports subjects that use the SubjectNameStrategy of TopicName.
    // If another SubjectNameStrategy is used then the key and message subject must be encoded
    // into the table name as follows:
    // <table name>[&key-subject=<key subject>][&message-subject=<message subject]
    // ex. kafka.default."my-topic&key-subject=foo&message-subject=bar"
    private Map<String, TopicAndSubjects> refreshSubjects()
    {
        try {
            Map<String, Set<String>> topicToSubjects = new HashMap<>();
            for (String subject : schemaRegistryClient.getAllSubjects()) {
                if (isValidSubject(subject)) {
                    topicToSubjects.computeIfAbsent(extractTopicFromSubject(subject), k -> new HashSet<>()).add(subject);
                }
            }
            ImmutableMap.Builder<String, TopicAndSubjects> topicSubjectsCacheBuilder = ImmutableMap.builder();
            for (Map.Entry<String, Set<String>> entry : topicToSubjects.entrySet()) {
                String topic = entry.getKey();
                TopicAndSubjects topicAndSubjects = new TopicAndSubjects(topic,
                        getKeySubjectFromTopic(topic, entry.getValue()),
                        getValueSubjectFromTopic(topic, entry.getValue()));
                topicSubjectsCacheBuilder.put(topicAndSubjects.getTableName(), topicAndSubjects);
            }
            return topicSubjectsCacheBuilder.build();
        }
        catch (IOException | RestClientException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to retrieve subjects from schema registry", e);
        }
    }

    @Override
    public Optional<KafkaTopicDescription> getTopicDescription(ConnectorSession session, SchemaTableName schemaTableName)
    {
        requireNonNull(schemaTableName, "schemaTableName is null");
        TopicAndSubjects topicAndSubjects = parseTopicAndSubjects(schemaTableName);

        String tableName = topicAndSubjects.getTableName();
        String topic = topicAndSubjects.getTopic();
        Optional<String> keySubject = topicAndSubjects.getKeySubject();
        Optional<String> valueSubject = topicAndSubjects.getValueSubject();

        TopicAndSubjects topicAndSubjectsFromCache = topicAndSubjectsSupplier.get().get(tableName);

        if (topicAndSubjectsFromCache != null) {
            // Always use the topic from cache in case the topic is mixed case
            topic = topicAndSubjectsFromCache.getTopic();
            if (!keySubject.isPresent()) {
                keySubject = topicAndSubjectsFromCache.getKeySubject();
            }
            if (!valueSubject.isPresent()) {
                valueSubject = topicAndSubjectsFromCache.getValueSubject();
            }
        }
        if (!keySubject.isPresent() && !valueSubject.isPresent()) {
            return Optional.empty();
        }
        AvroSchemaConverter schemaConverter = new AvroSchemaConverter(typeManager, getEmptyFieldStrategy(session));
        Optional<KafkaTopicFieldGroup> key = keySubject.map(subject -> getFieldGroup(schemaConverter, subject));
        Optional<KafkaTopicFieldGroup> message = valueSubject.map(subject -> getFieldGroup(schemaConverter, subject));
        return Optional.of(new KafkaTopicDescription(tableName, Optional.of(schemaTableName.getSchemaName()), topic, key, message));
    }

    private KafkaTopicFieldGroup getFieldGroup(AvroSchemaConverter avroSchemaConverter, String subject)
    {
        try {
            Schema schema = new Schema.Parser().parse(schemaRegistryClient.getLatestSchemaMetadata(subject).getSchema());
            List<Type> types = avroSchemaConverter.convertAvroSchema(schema);
            ImmutableList.Builder<KafkaTopicFieldDescription> fieldsBuilder = ImmutableList.builder();
            if (schema.getType() != Schema.Type.RECORD) {
                checkState(types.size() == 1, "incompatible schema");
                fieldsBuilder.add(new KafkaTopicFieldDescription(
                        subject,
                        getOnlyElement(types),
                        subject,
                        null,
                        null,
                        null,
                        false));
            }
            else {
                List<Schema.Field> avroFields = schema.getFields();
                checkState(avroFields.size() == types.size(), "incompatible schema");

                for (int i = 0; i < types.size(); i++) {
                    Schema.Field field = avroFields.get(i);
                    fieldsBuilder.add(new KafkaTopicFieldDescription(
                            field.name(),
                            types.get(i),
                            field.name(),
                            null,
                            null,
                            null,
                            false));
                }
            }
            return new KafkaTopicFieldGroup(AvroRowDecoderFactory.NAME, Optional.empty(), Optional.of(subject), fieldsBuilder.build());
        }
        catch (IOException | RestClientException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, format("Unable to get field group for '%s' subject", subject), e);
        }
    }

    // If the default subject naming strategy is not used, the key and message subjects
    // can be specified by adding them to the table name as follows:
    //  <tablename>&key-subject=<key subject>&message-subject=<value subject>
    // ex. kafka.default."mytable&key-subject=foo&message-subject=bar"
    @VisibleForTesting
    static TopicAndSubjects parseTopicAndSubjects(SchemaTableName encodedSchemaTableName)
    {
        String encodedTableName = encodedSchemaTableName.getTableName();
        List<String> parts = Splitter.on(DELIMITER).trimResults().splitToList(encodedTableName);
        checkState(!parts.isEmpty() && parts.size() <= 3, "Unexpected format for encodedTableName. Expected format is <tableName>[&keySubject=<key subject>][&messageSubject=<message subject>]");
        String tableName = parts.get(0);
        Optional<String> keySubject = Optional.empty();
        Optional<String> valueSubject = Optional.empty();
        for (int part = 1; part < parts.size(); part++) {
            List<String> subjectKeyValue = Splitter.on(SEPARATOR).trimResults().splitToList(parts.get(part));
            checkState(subjectKeyValue.size() == 2 && (subjectKeyValue.get(0).equals(KEY_SUBJECT) || subjectKeyValue.get(0).equals(MESSAGE_SUBJECT)), "Unexpected parameter '%s', should be %s=<key subject>' or %s=<message subject>", parts.get(part), KEY_SUBJECT, MESSAGE_SUBJECT);
            if (subjectKeyValue.get(0).equals(KEY_SUBJECT)) {
                checkState(!keySubject.isPresent(), "Key subject already defined");
                keySubject = Optional.of(subjectKeyValue.get(1));
            }
            else {
                checkState(!valueSubject.isPresent(), "Value subject already defined");
                valueSubject = Optional.of(subjectKeyValue.get(1));
            }
        }
        return new TopicAndSubjects(tableName, keySubject, valueSubject);
    }

    @Override
    public Set<SchemaTableName> listTables()
    {
        try {
            ImmutableSet.Builder<SchemaTableName> schemaTableNameBuilder = ImmutableSet.builder();
            schemaRegistryClient.getAllSubjects().stream()
                    .filter(ConfluentSchemaRegistryTableDescriptionSupplier::isValidSubject)
                    .forEach(subject -> schemaTableNameBuilder.add(new SchemaTableName(defaultSchema, extractTopicFromSubject(subject))));
            return schemaTableNameBuilder.build();
        }
        catch (IOException | RestClientException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Unable to list tables", e);
        }
    }

    @VisibleForTesting
    static boolean isValidSubject(String subject)
    {
        requireNonNull(subject, "subject is null");
        return subject.endsWith(VALUE_SUFFIX) || subject.endsWith(KEY_SUFFIX);
    }

    @VisibleForTesting
    static String extractTopicFromSubject(String subject)
    {
        requireNonNull(subject, "subject is null");
        if (subject.endsWith(VALUE_SUFFIX)) {
            return subject.substring(0, subject.length() - VALUE_SUFFIX.length());
        }
        checkState(subject.endsWith(KEY_SUFFIX), "Unexpected subject name %s", subject);
        return subject.substring(0, subject.length() - KEY_SUFFIX.length());
    }

    @VisibleForTesting
    static Optional<String> getKeySubjectFromTopic(String topic, Set<String> subjectsForTopic)
    {
        String keySubject = topic + KEY_SUFFIX;
        if (subjectsForTopic.contains(keySubject)) {
            return Optional.of(keySubject);
        }
        return Optional.empty();
    }

    @VisibleForTesting
    static Optional<String> getValueSubjectFromTopic(String topic, Set<String> subjectsForTopic)
    {
        String valueSubject = topic + VALUE_SUFFIX;
        if (subjectsForTopic.contains(valueSubject)) {
            return Optional.of(valueSubject);
        }
        return Optional.empty();
    }

    @VisibleForTesting
    static class TopicAndSubjects
    {
        private final Optional<String> keySubject;
        private final Optional<String> valueSubject;
        private final String topic;

        public TopicAndSubjects(String topic, Optional<String> keySubject, Optional<String> valueSubject)
        {
            this.topic = requireNonNull(topic, "topic is null");
            this.keySubject = requireNonNull(keySubject, "keySubject is null");
            this.valueSubject = requireNonNull(valueSubject, "valueSubject is null");
        }

        public String getTableName()
        {
            return topic.toLowerCase(ENGLISH);
        }

        public String getTopic()
        {
            return topic;
        }

        public Optional<String> getKeySubject()
        {
            return keySubject;
        }

        public Optional<String> getValueSubject()
        {
            return valueSubject;
        }

        @Override
        public boolean equals(Object other)
        {
            if (this == other) {
                return true;
            }
            if (!(other instanceof TopicAndSubjects)) {
                return false;
            }
            TopicAndSubjects that = (TopicAndSubjects) other;
            return topic.equals(that.topic) &&
                    keySubject.equals(that.keySubject) &&
                    valueSubject.equals(that.valueSubject);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(topic, keySubject, valueSubject);
        }
    }
}
