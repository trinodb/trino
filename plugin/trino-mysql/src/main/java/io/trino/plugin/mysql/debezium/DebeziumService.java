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
package io.trino.plugin.mysql.debezium;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.plugin.jdbc.JdbcNamedRelationHandle;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.mysql.VersioningService;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.json.JSONObject;

import javax.inject.Inject;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

public class DebeziumService
        implements VersioningService
{
    private final DebeziumConfig config;

    @Inject
    public DebeziumService(DebeziumConfig config)
    {
        this.config = requireNonNull(config, "config is null");
    }

    @Override
    public Optional<Long> getCurrentTableVersion(JdbcTableHandle handle)
    {
        if (handle.isSynthetic()
                || handle.getColumns().isPresent()
                || !handle.getConstraint().isAll()
                || !handle.getConstraintExpressions().isEmpty()
                || handle.getLimit().isPresent()
                || handle.getSortOrder().isPresent()) {
            // only plain tables are supported
            return Optional.empty();
        }

        KafkaConsumer<String, String> consumer = getConsumer(handle);
        if (!consumer.listTopics().containsKey(getTopicName(handle))) {
            return Optional.empty();
        }

        TopicPartition topic = getTopic(handle);
        long offset = requireNonNull(consumer.endOffsets(ImmutableSet.of(topic)).get(topic));
        return Optional.of(offset);
    }

    @Override
    public Iterable<? extends List<?>> getRecords(JdbcTableHandle handle, List<Type> types, List<String> columnNames)
    {
        checkArgument(handle.isVersioned());
        checkArgument(types.size() == columnNames.size());
        KafkaConsumer<String, String> consumer = getConsumer(handle);
        TopicPartition topic = getTopic(handle);
        consumer.seek(topic, handle.getStartVersion().orElse(0L));
        Map<Long, List<?>> rows = new HashMap<>();
        long endVersion = handle.getEndVersion().orElseThrow();
        boolean returnDeleted = handle.isDeletedRows();
        while (consumer.position(topic) < endVersion) {
            ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
            for (ConsumerRecord<String, String> record : records) {
                if (record.offset() >= endVersion) {
                    break;
                }
                if (record.value() == null) {
                    // skip tombstone event
                    continue;
                }
                JSONObject payload = new JSONObject(record.value()).getJSONObject("payload");
                boolean deleted = payload.getString("op").equals("d");
                JSONObject current;
                if (deleted) {
                    current = payload.getJSONObject("before");
                }
                else {
                    current = payload.getJSONObject("after");
                }
                // TODO: getCurrentTableVersion should check that "id" exists
                ImmutableList.Builder<Object> row = ImmutableList.builder();
                for (int i = 0; i < columnNames.size(); ++i) {
                    String columnName = columnNames.get(i);
                    Type type = types.get(i);
                    if (type.equals(BIGINT)) {
                        long value = current.getLong(columnName);
                        if (columnName.equals("id") && returnDeleted) {
                            value = -value;
                        }
                        row.add(value);
                    }
                    else if (type.equals(INTEGER)) {
                        int value = current.getInt(columnName);
                        if (columnName.equals("id") && returnDeleted) {
                            value = -value;
                        }
                        row.add(value);
                    }
                    else if (type instanceof VarcharType) {
                        row.add(current.getString(columnName));
                    }
                    else {
                        throw new UnsupportedOperationException();
                    }
                }

                long id = current.getLong("id");
                if (!returnDeleted) {
                    if (deleted) {
                        rows.remove(id);
                    }
                    else {
                        rows.put(id, row.build());
                    }
                }
                else {
                    if (deleted) {
                        rows.put(id, row.build());
                    }
                    else {
                        rows.remove(id);
                    }
                }
            }
        }
        return ImmutableList.copyOf(rows.values());
    }

    private KafkaConsumer<String, String> getConsumer(JdbcTableHandle handle)
    {
        Properties properties = new Properties();
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, config.getNodes().stream()
                .map(HostAddress::toString)
                .collect(joining(",")));
        StringDeserializer x = new StringDeserializer();
        properties.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(GROUP_ID_CONFIG, "trino");
        properties.setProperty(ENABLE_AUTO_COMMIT_CONFIG, Boolean.toString(false));

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        TopicPartition topic = getTopic(handle);
        consumer.assign(ImmutableSet.of(topic));
        return consumer;
    }

    private TopicPartition getTopic(JdbcTableHandle handle)
    {
        return new TopicPartition(getTopicName(handle), 0);
    }

    private String getTopicName(JdbcTableHandle handle)
    {
        return getTopicName(getSchemaTableName(handle));
    }

    private String getTopicName(SchemaTableName tableName)
    {
        return config.getServerName() + "." + tableName.getSchemaName() + "." + tableName.getTableName();
    }

    private SchemaTableName getSchemaTableName(JdbcTableHandle handle)
    {
        checkArgument(handle.isNamedRelation());
        JdbcNamedRelationHandle relation = (JdbcNamedRelationHandle) handle.getRelationHandle();
        return relation.getSchemaTableName();
    }
}
