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

import com.google.inject.Inject;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.plugin.kafka.KafkaInternalFieldManager.InternalFieldId.HEADERS_FIELD;
import static io.trino.plugin.kafka.KafkaInternalFieldManager.InternalFieldId.KEY_CORRUPT_FIELD;
import static io.trino.plugin.kafka.KafkaInternalFieldManager.InternalFieldId.KEY_LENGTH_FIELD;
import static io.trino.plugin.kafka.KafkaInternalFieldManager.InternalFieldId.MESSAGE_CORRUPT_FIELD;
import static io.trino.plugin.kafka.KafkaInternalFieldManager.InternalFieldId.MESSAGE_FIELD;
import static io.trino.plugin.kafka.KafkaInternalFieldManager.InternalFieldId.MESSAGE_LENGTH_FIELD;
import static io.trino.plugin.kafka.KafkaInternalFieldManager.InternalFieldId.OFFSET_TIMESTAMP_FIELD;
import static io.trino.plugin.kafka.KafkaInternalFieldManager.InternalFieldId.PARTITION_ID_FIELD;
import static io.trino.plugin.kafka.KafkaInternalFieldManager.InternalFieldId.PARTITION_OFFSET_FIELD;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TypeSignature.arrayType;
import static io.trino.spi.type.TypeSignature.mapType;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

public class KafkaInternalFieldManager
{
    public enum InternalFieldId
    {
        /**
         * Kafka partition id.
         */
        PARTITION_ID_FIELD,

        /**
         * The current offset of the message in the partition.
         */
        PARTITION_OFFSET_FIELD,

        /**
         * Field is true if the row converter could not read a message. May be null if the row converter does not set a value (e.g. the dummy row converter does not).
         */
        MESSAGE_CORRUPT_FIELD,

        /**
         * Represents the full topic as a text column. Format is UTF-8 which may be wrong for some topics. TODO: make charset configurable.
         */
        MESSAGE_FIELD,

        /**
         * length in bytes of the message.
         */
        MESSAGE_LENGTH_FIELD,

        /**
         * The header fields of the Kafka message. Key is a UTF-8 String and values an array of byte[].
         */
        HEADERS_FIELD,

        /**
         * Field is true if the row converter could not read a key. May be null if the row converter does not set a value (e.g. the dummy row converter does not).
         */
        KEY_CORRUPT_FIELD,

        /**
         * Represents the key as a text column. Format is UTF-8 which may be wrong for topics. TODO: make charset configurable.
         */
        KEY_FIELD,

        /**
         * length in bytes of the key.
         */
        KEY_LENGTH_FIELD,

        /**
         * message timestamp
         */
        OFFSET_TIMESTAMP_FIELD,
    }

    public static class InternalField
    {
        private final InternalFieldId internalFieldId;
        private final String columnName;
        private final String comment;
        private final Type type;

        InternalField(InternalFieldId internalFieldId, String columnName, String comment, Type type)
        {
            this.internalFieldId = requireNonNull(internalFieldId, "internalFieldId is null");
            this.columnName = requireNonNull(columnName, "columnName is null");
            this.comment = requireNonNull(comment, "comment is null");
            this.type = requireNonNull(type, "type is null");
        }

        public InternalFieldId getInternalFieldId()
        {
            return internalFieldId;
        }

        public String getColumnName()
        {
            return columnName;
        }

        private Type getType()
        {
            return type;
        }

        KafkaColumnHandle getColumnHandle(boolean hidden)
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

    private final Map<String, InternalField> fieldsByNames;
    private final Map<InternalFieldId, InternalField> fieldsByIds;

    @Inject
    public KafkaInternalFieldManager(TypeManager typeManager, KafkaConfig kafkaConfig)
    {
        Type varcharMapType = typeManager.getType(mapType(VARCHAR.getTypeSignature(), arrayType(VARBINARY.getTypeSignature())));
        String prefix = kafkaConfig.getInternalFieldPrefix();
        List<InternalField> fields = Stream.of(
                        new InternalField(
                                PARTITION_ID_FIELD,
                                prefix + "partition_id",
                                "Partition Id",
                                BigintType.BIGINT),
                        new InternalField(
                                PARTITION_OFFSET_FIELD,
                                prefix + "partition_offset",
                                "Offset for the message within the partition",
                                BigintType.BIGINT),
                        new InternalField(
                                MESSAGE_CORRUPT_FIELD,
                                prefix + "message_corrupt",
                                "Message data is corrupt",
                                BooleanType.BOOLEAN),
                        new InternalField(
                                MESSAGE_FIELD,
                                prefix + "message",
                                "Message text",
                                createUnboundedVarcharType()),
                        new InternalField(
                                HEADERS_FIELD,
                                prefix + "headers",
                                "Headers of the message as map",
                                varcharMapType),
                        new InternalField(
                                MESSAGE_LENGTH_FIELD,
                                prefix + "message_length",
                                "Total number of message bytes",
                                BigintType.BIGINT),
                        new InternalField(
                                KEY_CORRUPT_FIELD,
                                prefix + "key_corrupt",
                                "Key data is corrupt",
                                BooleanType.BOOLEAN),
                        new InternalField(
                                InternalFieldId.KEY_FIELD,
                                prefix + "key",
                                "Key text",
                                createUnboundedVarcharType()),
                        new InternalField(
                                KEY_LENGTH_FIELD,
                                prefix + "key_length",
                                "Total number of key bytes",
                                BigintType.BIGINT),
                        new InternalField(
                                OFFSET_TIMESTAMP_FIELD,
                                prefix + "timestamp",
                                "Message timestamp",
                                TIMESTAMP_MILLIS))
                .collect(toImmutableList());
        fieldsByNames = fields.stream()
                .collect(toImmutableMap(InternalField::getColumnName, identity()));
        fieldsByIds = fields.stream()
                .collect(toImmutableMap(InternalField::getInternalFieldId, identity()));
    }

    /**
     * @return Map of {@link InternalField} for each internal field.
     */
    public Collection<InternalField> getInternalFields()
    {
        return fieldsByNames.values();
    }

    public InternalField getFieldByName(String name)
    {
        return fieldsByNames.get(name);
    }

    public InternalField getFieldById(InternalFieldId id)
    {
        return fieldsByIds.get(id);
    }
}
