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
package io.prestosql.plugin.kafka.confluent;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.Duration;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.prestosql.plugin.kafka.KafkaConfig;
import io.prestosql.plugin.kafka.KafkaSessionProperties;
import io.prestosql.plugin.kafka.KafkaTopicDescription;
import io.prestosql.plugin.kafka.KafkaTopicFieldDescription;
import io.prestosql.plugin.kafka.confluent.ConfluentSchemaRegistryTableDescriptionSupplier.TopicAndSubjects;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.session.PropertyMetadata;
import io.prestosql.spi.type.TestingTypeManager;
import io.prestosql.testing.TestingConnectorSession;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.prestosql.plugin.kafka.KafkaSessionProperties.EMPTY_FIELD_STRATEGY;
import static io.prestosql.plugin.kafka.confluent.ConfluentSchemaRegistryTableDescriptionSupplier.KEY_SUBJECT;
import static io.prestosql.plugin.kafka.confluent.ConfluentSchemaRegistryTableDescriptionSupplier.MESSAGE_SUBJECT;
import static io.prestosql.plugin.kafka.confluent.ConfluentSchemaRegistryTableDescriptionSupplier.extractTopicFromSubject;
import static io.prestosql.plugin.kafka.confluent.ConfluentSchemaRegistryTableDescriptionSupplier.getKeySubjectFromTopic;
import static io.prestosql.plugin.kafka.confluent.ConfluentSchemaRegistryTableDescriptionSupplier.getValueSubjectFromTopic;
import static io.prestosql.plugin.kafka.confluent.ConfluentSchemaRegistryTableDescriptionSupplier.isValidSubject;
import static io.prestosql.plugin.kafka.confluent.ConfluentSchemaRegistryTableDescriptionSupplier.parseTopicAndSubjects;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestConfluentSchemaRegistryTableDescriptionSupplier
{
    private static final Map<String, List<PropertyMetadata<?>>> CONFLUENT_PROPERITES = ImmutableMap.of(ConfluentAvroReaderSupplier.NAME, new ConfluentSessionProperties(new ConfluentSchemaRegistryConfig()).get());

    @Test
    public void testParseTopicAndSubjects()
    {
        assertEquals(parseTopicAndSubjects(new SchemaTableName("default", "my-table")), new TopicAndSubjects("my-table", Optional.empty(), Optional.empty()));
        assertEquals(parseTopicAndSubjects(new SchemaTableName("default", "my-table&key-subject=foo")), new TopicAndSubjects("my-table", Optional.of("foo"), Optional.empty()));
        assertEquals(parseTopicAndSubjects(new SchemaTableName("default", "my-table&message-subject=bar")), new TopicAndSubjects("my-table", Optional.empty(), Optional.of("bar")));
        assertEquals(parseTopicAndSubjects(new SchemaTableName("default", "my-table&key-subject=foo&message-subject=bar")), new TopicAndSubjects("my-table", Optional.of("foo"), Optional.of("bar")));
        assertEquals(parseTopicAndSubjects(new SchemaTableName("default", "my-table&message-subject=bar&key-subject=foo")), new TopicAndSubjects("my-table", Optional.of("foo"), Optional.of("bar")));

        assertThatThrownBy(() -> parseTopicAndSubjects(new SchemaTableName("default", "my-table&bad-key=baz")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Unexpected parameter '%s', should be %s=<key subject>' or %s=<message subject>", "bad-key=baz", KEY_SUBJECT, MESSAGE_SUBJECT);

        assertThatThrownBy(() -> parseTopicAndSubjects(new SchemaTableName("default", "my-table&&key-subject=foo")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Unexpected parameter '%s', should be %s=<key subject>' or %s=<message subject>", "", KEY_SUBJECT, MESSAGE_SUBJECT);

        assertThatThrownBy(() -> parseTopicAndSubjects(new SchemaTableName("default", "my-table&novalue")))
                .isInstanceOf(IllegalStateException.class).isInstanceOf(IllegalStateException.class)
                .hasMessage("Unexpected parameter '%s', should be %s=<key subject>' or %s=<message subject>", "novalue", KEY_SUBJECT, MESSAGE_SUBJECT);

        assertThatThrownBy(() -> parseTopicAndSubjects(new SchemaTableName("default", "my-table&=nokey")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Unexpected parameter '%s', should be %s=<key subject>' or %s=<message subject>", "=nokey", KEY_SUBJECT, MESSAGE_SUBJECT);

        assertThatThrownBy(() -> parseTopicAndSubjects(new SchemaTableName("default", "my-table&key-subject=foo&bad-key=baz")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Unexpected parameter '%s', should be %s=<key subject>' or %s=<message subject>", "bad-key=baz", KEY_SUBJECT, MESSAGE_SUBJECT);

        assertThatThrownBy(() -> parseTopicAndSubjects(new SchemaTableName("default", "my-table&key-subject=foo&message-subject=bar&bad-key=baz")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Unexpected format for encodedTableName. Expected format is <tableName>[&keySubject=<key subject>][&messageSubject=<message subject>]");
    }

    @Test
    public void testIsValidSubject()
    {
        assertTrue(isValidSubject("my-topic-key"));
        assertTrue(isValidSubject("my-topic-value"));
        assertFalse(isValidSubject("my-topic"));
    }

    @Test
    public void testExtractTopicFromSubject()
    {
        assertEquals("my-topic", extractTopicFromSubject("my-topic-key"));
        assertEquals("my-topic", extractTopicFromSubject("my-topic-value"));
        assertThatThrownBy(() -> extractTopicFromSubject("my-topic"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Unexpected subject name %s", "my-topic");
    }

    @Test
    public void testGetKeySubjectFromTopic()
    {
        assertEquals(Optional.of("my-topic-key"), getKeySubjectFromTopic("my-topic", ImmutableSet.of("my-topic-key")));
        assertEquals(Optional.empty(), getKeySubjectFromTopic("my-topic", ImmutableSet.of("my-topic-value")));
        assertEquals(Optional.empty(), getKeySubjectFromTopic("my-topic", ImmutableSet.of("my-topic-record-name")));
    }

    @Test
    public void testGetValueSubjectFromTopic()
    {
        assertEquals(Optional.of("my-topic-value"), getValueSubjectFromTopic("my-topic", ImmutableSet.of("my-topic-value")));
        assertEquals(Optional.empty(), getValueSubjectFromTopic("my-topic", ImmutableSet.of("my-topic-key")));
        assertEquals(Optional.empty(), getValueSubjectFromTopic("my-topic", ImmutableSet.of("my-topic-record-name")));
    }

    @Test
    public void testMixedCaseTopic()
            throws Exception
    {
        ConfluentSchemaRegistryTableDescriptionSupplier tableDescriptionSupplier = null;
        try {
            String testTopic = "testTopic";
            MockSchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();
            Schema keySchema = Schema.create(Schema.Type.LONG);
            Schema valueSchema = SchemaBuilder.record(testTopic)
                    .fields()
                    .name("col1").type().longType().noDefault()
                    .endRecord();
            String keySubject = format("%s-key", testTopic);
            schemaRegistryClient.register(keySubject, keySchema);
            schemaRegistryClient.register(format("%s-value", testTopic), valueSchema);
            tableDescriptionSupplier = new ConfluentSchemaRegistryTableDescriptionSupplier(schemaRegistryClient, "default", new Duration(1, SECONDS), new TestingTypeManager());
            Optional<KafkaTopicDescription> topicDescription = tableDescriptionSupplier.getTopicDescription(TestingConnectorSession.builder()
                    .setPropertyMetadata(new KafkaSessionProperties(new KafkaConfig(), CONFLUENT_PROPERITES).getSessionProperties())
                    .setPropertyValues(ImmutableMap.of(EMPTY_FIELD_STRATEGY, "ADD_DUMMY"))
                    .build(), new SchemaTableName("default", testTopic));
            assertTrue(topicDescription.isPresent());
            assertEquals(topicDescription.get().getTopicName(), testTopic);
            assertEquals(topicDescription.get().getTableName(), testTopic.toLowerCase(ENGLISH));
            assertTrue(topicDescription.get().getKey().isPresent());
            KafkaTopicFieldDescription keyDescription = getOnlyElement(topicDescription.get().getKey().get().getFields());
            assertEquals(keyDescription.getName(), keySubject);
            assertEquals(keyDescription.getType(), BIGINT);
            assertTrue(topicDescription.get().getMessage().isPresent());
            KafkaTopicFieldDescription messageDescription = getOnlyElement(topicDescription.get().getMessage().get().getFields());
            assertEquals(messageDescription.getName(), "col1");
            assertEquals(messageDescription.getType(), BIGINT);
        }
        finally {
            if (tableDescriptionSupplier != null) {
                tableDescriptionSupplier.stop();
            }
        }
    }
}
