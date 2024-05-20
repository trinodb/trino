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
package io.trino.plugin.kafka;

import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * @param schemaName The schema name for this table. Is set through configuration and read
 * using {@link KafkaConfig#getDefaultSchema()}. Usually 'default'.
 * @param tableName The table name used by Trino.
 * @param topicName The topic name that is read from Kafka.
 */
public record KafkaTableHandle(
        String schemaName,
        String tableName,
        String topicName,
        String keyDataFormat,
        String messageDataFormat,
        Optional<String> keyDataSchemaLocation,
        Optional<String> messageDataSchemaLocation,
        Optional<String> keySubject,
        Optional<String> messageSubject,
        List<KafkaColumnHandle> columns,
        TupleDomain<ColumnHandle> constraint)
        implements ConnectorTableHandle, ConnectorInsertTableHandle
{
    public KafkaTableHandle
    {
        requireNonNull(schemaName, "schemaName is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(topicName, "topicName is null");
        requireNonNull(keyDataFormat, "keyDataFormat is null");
        requireNonNull(messageDataFormat, "messageDataFormat is null");
        requireNonNull(keyDataSchemaLocation, "keyDataSchemaLocation is null");
        requireNonNull(messageDataSchemaLocation, "messageDataSchemaLocation is null");
        requireNonNull(keySubject, "keySubject is null");
        requireNonNull(messageSubject, "messageSubject is null");
        columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
        requireNonNull(constraint, "constraint is null");
    }

    public SchemaTableName schemaTableName()
    {
        return new SchemaTableName(schemaName, tableName);
    }
}
