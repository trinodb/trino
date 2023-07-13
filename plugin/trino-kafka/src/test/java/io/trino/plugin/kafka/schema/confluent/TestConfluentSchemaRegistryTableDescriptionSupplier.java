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
import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.trino.plugin.kafka.KafkaTopicDescription;
import io.trino.plugin.kafka.KafkaTopicFieldDescription;
import io.trino.plugin.kafka.KafkaTopicFieldGroup;
import io.trino.plugin.kafka.schema.TableDescriptionSupplier;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.TestingTypeManager;
import io.trino.spi.type.Type;
import io.trino.testing.TestingConnectorSession;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertTrue;

public class TestConfluentSchemaRegistryTableDescriptionSupplier
{
    private static final String DEFAULT_NAME = "tests";
    private static final SchemaRegistryClient SCHEMA_REGISTRY_CLIENT = new MockSchemaRegistryClient();

    @Test
    public void testTopicDescription()
            throws Exception
    {
        TableDescriptionSupplier tableDescriptionSupplier = createTableDescriptionSupplier();
        String topicName = "simple_topic";
        SchemaTableName schemaTableName = new SchemaTableName(DEFAULT_NAME, topicName);
        String subject = topicName + "-value";
        SCHEMA_REGISTRY_CLIENT.register(subject, getAvroSchema(schemaTableName.getTableName(), ""));

        assertTrue(tableDescriptionSupplier.listTables().contains(schemaTableName));

        assertThat(getKafkaTopicDescription(tableDescriptionSupplier, schemaTableName))
                .isEqualTo(
                        new KafkaTopicDescription(
                                schemaTableName.getTableName(),
                                Optional.of(schemaTableName.getSchemaName()),
                                schemaTableName.getTableName(),
                                Optional.empty(),
                                Optional.of(getTopicFieldGroup(
                                        subject,
                                        ImmutableList.of(getFieldDescription("col1", INTEGER), getFieldDescription("col2", createUnboundedVarcharType()))))));
    }

    @Test
    public void testCaseInsensitiveSubjectMapping()
            throws Exception
    {
        TableDescriptionSupplier tableDescriptionSupplier = createTableDescriptionSupplier();
        String topicName = "camelCase_Topic";
        SchemaTableName schemaTableName = new SchemaTableName(DEFAULT_NAME, topicName);
        String subject = topicName + "-key";

        SCHEMA_REGISTRY_CLIENT.register(subject, getAvroSchema(schemaTableName.getTableName(), ""));

        assertTrue(tableDescriptionSupplier.listTables().contains(schemaTableName));

        assertThat(getKafkaTopicDescription(tableDescriptionSupplier, schemaTableName))
                .isEqualTo(
                        new KafkaTopicDescription(
                                schemaTableName.getTableName(),
                                Optional.of(schemaTableName.getSchemaName()),
                                topicName,
                                Optional.of(getTopicFieldGroup(
                                        subject,
                                        ImmutableList.of(getFieldDescription("col1", INTEGER), getFieldDescription("col2", createUnboundedVarcharType())))),
                                Optional.empty()));
    }

    @Test
    public void testAmbiguousSubject()
            throws Exception
    {
        TableDescriptionSupplier tableDescriptionSupplier = createTableDescriptionSupplier();
        String topicName = "topic_one";
        SchemaTableName schemaTableName = new SchemaTableName(DEFAULT_NAME, topicName);

        SCHEMA_REGISTRY_CLIENT.register(topicName + "-key", getAvroSchema(schemaTableName.getTableName(), ""));
        SCHEMA_REGISTRY_CLIENT.register(topicName.toUpperCase(ENGLISH) + "-key", getAvroSchema(schemaTableName.getTableName(), ""));

        assertTrue(tableDescriptionSupplier.listTables().contains(schemaTableName));

        assertThatThrownBy(() ->
                tableDescriptionSupplier.getTopicDescription(
                        TestingConnectorSession.builder()
                                .setPropertyMetadata(new ConfluentSessionProperties(new ConfluentSchemaRegistryConfig()).getSessionProperties())
                                .build(),
                        schemaTableName))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Unable to access 'topic_one' table. Subject is ambiguous, and may refer to one of the following: TOPIC_ONE, topic_one");
    }

    @Test
    public void testOverriddenSubject()
            throws Exception
    {
        TableDescriptionSupplier tableDescriptionSupplier = createTableDescriptionSupplier();
        String topicName = "base_Topic";
        SchemaTableName schemaTableName = new SchemaTableName(DEFAULT_NAME, topicName);
        String subject = topicName + "-value";
        SCHEMA_REGISTRY_CLIENT.register(subject, getAvroSchema(schemaTableName.getTableName(), ""));
        String overriddenSubject = "overriddenSubject";
        SCHEMA_REGISTRY_CLIENT.register(overriddenSubject, getAvroSchema(schemaTableName.getTableName(), "overridden_"));

        assertTrue(tableDescriptionSupplier.listTables().contains(schemaTableName));

        assertThat(getKafkaTopicDescription(tableDescriptionSupplier, schemaTableName))
                .isEqualTo(
                        new KafkaTopicDescription(
                                schemaTableName.getTableName(),
                                Optional.of(schemaTableName.getSchemaName()),
                                topicName,
                                Optional.empty(),
                                Optional.of(getTopicFieldGroup(
                                        subject,
                                        ImmutableList.of(getFieldDescription("col1", INTEGER), getFieldDescription("col2", createUnboundedVarcharType()))))));

        assertThat(getKafkaTopicDescription(
                tableDescriptionSupplier,
                new SchemaTableName(DEFAULT_NAME, format("%s&value-subject=%s", topicName, overriddenSubject))))
                .isEqualTo(
                        new KafkaTopicDescription(
                                schemaTableName.getTableName(),
                                Optional.of(schemaTableName.getSchemaName()),
                                topicName,
                                Optional.empty(),
                                Optional.of(getTopicFieldGroup(
                                        overriddenSubject,
                                        ImmutableList.of(getFieldDescription("overridden_col1", INTEGER), getFieldDescription("overridden_col2", createUnboundedVarcharType()))))));
    }

    @Test
    public void testAmbiguousOverriddenSubject()
            throws Exception
    {
        TableDescriptionSupplier tableDescriptionSupplier = createTableDescriptionSupplier();
        String topicName = "base_Topic";
        SchemaTableName schemaTableName = new SchemaTableName(DEFAULT_NAME, topicName);
        String overriddenSubject = "ambiguousOverriddenSubject";
        SCHEMA_REGISTRY_CLIENT.register(overriddenSubject, getAvroSchema(schemaTableName.getTableName(), "overridden_"));
        SCHEMA_REGISTRY_CLIENT.register(overriddenSubject.toUpperCase(ENGLISH), getAvroSchema(schemaTableName.getTableName(), "overridden_"));

        assertThatThrownBy(() ->
                tableDescriptionSupplier.getTopicDescription(
                        TestingConnectorSession.builder()
                                .setPropertyMetadata(new ConfluentSessionProperties(new ConfluentSchemaRegistryConfig()).getSessionProperties())
                                .build(),
                        new SchemaTableName(DEFAULT_NAME, format("%s&value-subject=%s", topicName, overriddenSubject))))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Subject 'ambiguousoverriddensubject' is ambiguous, and may refer to one of the following: AMBIGUOUSOVERRIDDENSUBJECT, ambiguousOverriddenSubject");
    }

    private KafkaTopicDescription getKafkaTopicDescription(TableDescriptionSupplier tableDescriptionSupplier, SchemaTableName schemaTableName)
    {
        return tableDescriptionSupplier.getTopicDescription(
                TestingConnectorSession.builder()
                        .setPropertyMetadata(new ConfluentSessionProperties(new ConfluentSchemaRegistryConfig()).getSessionProperties())
                        .build(),
                schemaTableName)
                .orElseThrow();
    }

    private TableDescriptionSupplier createTableDescriptionSupplier()
    {
        return new ConfluentSchemaRegistryTableDescriptionSupplier(
                SCHEMA_REGISTRY_CLIENT,
                ImmutableMap.of("AVRO", new AvroSchemaParser(new TestingTypeManager())),
                DEFAULT_NAME,
                new Duration(1, SECONDS));
    }

    private static Schema getAvroSchema(String topicName, String columnNamePrefix)
    {
        return SchemaBuilder.record(topicName)
                .fields()
                .name(columnNamePrefix + "col1").type().intType().noDefault()
                .name(columnNamePrefix + "col2").type().stringType().noDefault()
                .endRecord();
    }

    private static KafkaTopicFieldGroup getTopicFieldGroup(String topicName, List<KafkaTopicFieldDescription> fieldDescriptions)
    {
        return new KafkaTopicFieldGroup("avro", Optional.empty(), Optional.of(topicName), fieldDescriptions);
    }

    private static KafkaTopicFieldDescription getFieldDescription(String name, Type type)
    {
        return new KafkaTopicFieldDescription(name, type, name, null, null, null, false);
    }
}
