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

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;

import java.util.Map;
import java.util.Optional;

import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TypeSignature.arrayType;
import static io.trino.spi.type.TypeSignature.mapType;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.util.Objects.requireNonNull;

public class KafkaInternalFieldManager
{
    /**
     * <tt>_partition_id</tt> - Kafka partition id.
     */
    public static final String PARTITION_ID_FIELD = "_partition_id";

    /**
     * <tt>_partition_offset</tt> - The current offset of the message in the partition.
     */
    public static final String PARTITION_OFFSET_FIELD = "_partition_offset";

    /**
     * <tt>_message_corrupt</tt> - True if the row converter could not read the a message. May be null if the row converter does not set a value (e.g. the dummy row converter does not).
     */
    public static final String MESSAGE_CORRUPT_FIELD = "_message_corrupt";

    /**
     * <tt>_message</tt> - Represents the full topic as a text column. Format is UTF-8 which may be wrong for some topics. TODO: make charset configurable.
     */
    public static final String MESSAGE_FIELD = "_message";

    /**
     * <tt>_message_length</tt> - length in bytes of the message.
     */
    public static final String MESSAGE_LENGTH_FIELD = "_message_length";

    /**
     * <tt>_headers</tt> - The header fields of the Kafka message. Key is a UTF-8 String and values an array of byte[].
     */
    public static final String HEADERS_FIELD = "_headers";

    /**
     * <tt>_key_corrupt</tt> - True if the row converter could not read the a key. May be null if the row converter does not set a value (e.g. the dummy row converter does not).
     */
    public static final String KEY_CORRUPT_FIELD = "_key_corrupt";

    /**
     * <tt>_key</tt> - Represents the key as a text column. Format is UTF-8 which may be wrong for topics. TODO: make charset configurable.
     */
    public static final String KEY_FIELD = "_key";

    /**
     * <tt>_key_length</tt> - length in bytes of the key.
     */
    public static final String KEY_LENGTH_FIELD = "_key_length";

    /**
     * <tt>_timestamp</tt> - message timestamp
     */
    public static final String OFFSET_TIMESTAMP_FIELD = "_timestamp";

    public static class InternalField
    {
        private final String columnName;
        private final String comment;
        private final Type type;

        InternalField(String columnName, String comment, Type type)
        {
            this.columnName = requireNonNull(columnName, "columnName is null");
            this.comment = requireNonNull(comment, "comment is null");
            this.type = requireNonNull(type, "type is null");
        }

        public String getColumnName()
        {
            return columnName;
        }

        private Type getType()
        {
            return type;
        }

        KafkaColumnHandle getColumnHandle(int index, boolean hidden)
        {
            return new KafkaColumnHandle(
                    getColumnName(),
                    getType(),
                    null,
                    null,
                    null,
                    false,
                    hidden,
                    true);
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

    private final Map<String, InternalField> internalFields;

    @Inject
    public KafkaInternalFieldManager(TypeManager typeManager)
    {
        Type varcharMapType = typeManager.getType(mapType(VARCHAR.getTypeSignature(), arrayType(VARBINARY.getTypeSignature())));

        internalFields = new ImmutableMap.Builder<String, InternalField>()
                .put(PARTITION_ID_FIELD, new InternalField(
                        PARTITION_ID_FIELD,
                        "Partition Id",
                        BigintType.BIGINT))
                .put(PARTITION_OFFSET_FIELD, new InternalField(
                        PARTITION_OFFSET_FIELD,
                        "Offset for the message within the partition",
                        BigintType.BIGINT))
                .put(MESSAGE_CORRUPT_FIELD, new InternalField(
                        MESSAGE_CORRUPT_FIELD,
                        "Message data is corrupt",
                        BooleanType.BOOLEAN))
                .put(MESSAGE_FIELD, new InternalField(
                        MESSAGE_FIELD,
                        "Message text",
                        createUnboundedVarcharType()))
                .put(HEADERS_FIELD, new InternalField(
                        HEADERS_FIELD,
                        "Headers of the message as map",
                        varcharMapType))
                .put(MESSAGE_LENGTH_FIELD, new InternalField(
                        MESSAGE_LENGTH_FIELD,
                        "Total number of message bytes",
                        BigintType.BIGINT))
                .put(KEY_CORRUPT_FIELD, new InternalField(
                        KEY_CORRUPT_FIELD,
                        "Key data is corrupt",
                        BooleanType.BOOLEAN))
                .put(KEY_FIELD, new InternalField(
                        KEY_FIELD,
                        "Key text",
                        createUnboundedVarcharType()))
                .put(KEY_LENGTH_FIELD, new InternalField(
                        KEY_LENGTH_FIELD,
                        "Total number of key bytes",
                        BigintType.BIGINT))
                .put(OFFSET_TIMESTAMP_FIELD, new InternalField(
                        OFFSET_TIMESTAMP_FIELD,
                        "Message timestamp",
                        TIMESTAMP_MILLIS))
                .buildOrThrow();
    }

    /**
     * @return Map of {@link InternalField} for each internal field.
     */
    public Map<String, InternalField> getInternalFields()
    {
        return internalFields;
    }
}
