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
package io.trino.plugin.kinesis;

import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;

import static java.util.Objects.requireNonNull;

/**
 * Class maintains all the properties of Trino Table
 *
 * @param schemaName The schema name for this table. Is set through configuration and read
 * using {@link KinesisConfig#getDefaultSchema()}. Usually 'default'.
 * @param tableName The table name used by Trino.
 * @param streamName The stream name that is read from Kinesis
 */
public record KinesisTableHandle(
        String schemaName,
        String tableName,
        String streamName,
        String messageDataFormat,
        KinesisCompressionCodec compressionCodec)
        implements ConnectorTableHandle
{
    public KinesisTableHandle
    {
        requireNonNull(schemaName, "schemaName is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(streamName, "streamName is null");
        requireNonNull(messageDataFormat, "messageDataFormat is null");
        requireNonNull(compressionCodec, "compressionCodec is null");
    }

    public SchemaTableName schemaTableName()
    {
        return new SchemaTableName(schemaName, tableName);
    }
}
