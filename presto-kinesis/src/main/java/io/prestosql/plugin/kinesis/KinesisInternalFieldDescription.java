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
package io.prestosql.plugin.kinesis;

import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.BooleanType;
import io.prestosql.spi.type.TimestampType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarcharType;

import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Arrays.stream;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

public enum KinesisInternalFieldDescription
{
    SHARD_ID_FIELD("_shard_id", VarcharType.VARCHAR, "Shard Id"),
    SEGMENT_START_FIELD("_segment_start", VarcharType.VARCHAR, "Segment start sequence id"),
    SEGMENT_END_FIELD("_shard_sequence_id", VarcharType.VARCHAR, "Segment end sequence id"),
    SHARD_SEQUENCE_ID_FIELD("_shard_sequence_id_field", BigintType.BIGINT, "Segment start offset"),
    SEGMENT_COUNT_FIELD("_segment_count", BigintType.BIGINT, "Running message count per segment"),
    MESSAGE_VALID_FIELD("_message_valid", BooleanType.BOOLEAN, "Message data is valid"),
    MESSAGE_FIELD("_message", VarcharType.VARCHAR, "Message text"),
    MESSAGE_TIMESTAMP("_message_timestamp", TimestampType.TIMESTAMP_MILLIS, "Approximate message arrival timestamp"),
    MESSAGE_LENGTH_FIELD("_message_length", BigintType.BIGINT, "Total number of message bytes"),
    PARTITION_KEY_FIELD("_partition_key", VarcharType.VARCHAR, "Key text");

    private static final Map<String, KinesisInternalFieldDescription> BY_COLUMN_NAME = stream(KinesisInternalFieldDescription.values())
            .collect(toImmutableMap(KinesisInternalFieldDescription::getColumnName, identity()));

    public static KinesisInternalFieldDescription forColumnName(String columnName)
    {
        KinesisInternalFieldDescription description = BY_COLUMN_NAME.get(columnName);
        checkArgument(description != null, "Unknown internal column name %s", columnName);
        return description;
    }

    private final String columnName;
    private final Type type;
    private final String comment;

    KinesisInternalFieldDescription(
            String columnName,
            Type type,
            String comment)
    {
        checkArgument(!isNullOrEmpty(columnName), "name is null or is empty");
        this.columnName = columnName;
        this.type = requireNonNull(type, "type is null");
        this.comment = requireNonNull(comment, "comment is null");
    }

    public String getColumnName()
    {
        return columnName;
    }

    public Type getType()
    {
        return type;
    }

    KinesisColumnHandle getColumnHandle(int index, boolean hidden)
    {
        return new KinesisColumnHandle(
                index,
                getColumnName(),
                getType(),
                null,
                null,
                null,
                false,
                hidden);
    }

    ColumnMetadata getColumnMetadata(boolean hidden)
    {
        return ColumnMetadata.builder()
                .setName(columnName)
                .setType(type)
                .setComment(Optional.ofNullable(comment))
                .setHidden(hidden)
                .build();
    }
}
