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
package io.trino.plugin.kafka.util;

import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;
import io.airlift.json.JsonCodec;
import io.trino.plugin.kafka.KafkaTopicDescription;
import io.trino.plugin.kafka.KafkaTopicFieldDescription;
import io.trino.plugin.kafka.KafkaTopicFieldGroup;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.Type;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.lang.String.format;

public final class TestUtils
{
    private TestUtils() {}

    public static Map.Entry<SchemaTableName, KafkaTopicDescription> loadTpchTopicDescription(JsonCodec<KafkaTopicDescription> topicDescriptionJsonCodec, String topicName, SchemaTableName schemaTableName)
            throws IOException
    {
        KafkaTopicDescription tpchTemplate = topicDescriptionJsonCodec.fromJson(ByteStreams.toByteArray(TestUtils.class.getResourceAsStream(format("/tpch/%s.json", schemaTableName.getTableName()))));

        return new AbstractMap.SimpleImmutableEntry<>(
                schemaTableName,
                new KafkaTopicDescription(schemaTableName.getTableName(), Optional.of(schemaTableName.getSchemaName()), topicName, tpchTemplate.getKey(), tpchTemplate.getMessage()));
    }

    public static Map.Entry<SchemaTableName, KafkaTopicDescription> createEmptyTopicDescription(String topicName, SchemaTableName schemaTableName)
    {
        return new AbstractMap.SimpleImmutableEntry<>(
                schemaTableName,
                new KafkaTopicDescription(schemaTableName.getTableName(), Optional.of(schemaTableName.getSchemaName()), topicName, Optional.empty(), Optional.empty()));
    }

    public static KafkaTopicDescription createDescription(SchemaTableName schemaTableName, KafkaTopicFieldDescription key, List<KafkaTopicFieldDescription> fields)
    {
        return new KafkaTopicDescription(
                schemaTableName.getTableName(),
                Optional.of(schemaTableName.getSchemaName()),
                schemaTableName.getTableName(),
                Optional.of(new KafkaTopicFieldGroup("json", Optional.empty(), Optional.empty(), ImmutableList.of(key))),
                Optional.of(new KafkaTopicFieldGroup("json", Optional.empty(), Optional.empty(), fields)));
    }

    public static KafkaTopicDescription createDescription(String name, String schema, String topic, Optional<KafkaTopicFieldGroup> message)
    {
        return new KafkaTopicDescription(name, Optional.of(schema), topic, Optional.empty(), message);
    }

    public static Optional<KafkaTopicFieldGroup> createFieldGroup(String dataFormat, List<KafkaTopicFieldDescription> fields)
    {
        return Optional.of(new KafkaTopicFieldGroup(dataFormat, Optional.empty(), Optional.empty(), fields));
    }

    public static KafkaTopicFieldDescription createOneFieldDescription(String name, Type type)
    {
        return new KafkaTopicFieldDescription(name, type, name, null, null, null, false);
    }

    public static KafkaTopicFieldDescription createOneFieldDescription(String name, Type type, String dataFormat)
    {
        return new KafkaTopicFieldDescription(name, type, name, null, dataFormat, null, false);
    }

    public static KafkaTopicFieldDescription createOneFieldDescription(String name, Type type, String dataFormat, Optional<String> formatHint)
    {
        return formatHint.map(s -> new KafkaTopicFieldDescription(name, type, name, null, dataFormat, s, false))
                .orElseGet(() -> new KafkaTopicFieldDescription(name, type, name, null, dataFormat, null, false));
    }

    public static KafkaTopicFieldDescription createOneFieldDescription(String name, Type type, String mapping, String dataFormat)
    {
        return new KafkaTopicFieldDescription(name, type, mapping, null, dataFormat, null, false);
    }
}
