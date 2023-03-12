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
package io.trino.plugin.kafka.encoder.protobuf;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.trino.decoder.protobuf.ProtobufRowDecoder;
import io.trino.plugin.kafka.KafkaTopicFieldDescription;
import io.trino.plugin.kafka.KafkaTopicFieldGroup;
import io.trino.plugin.kafka.schema.confluent.SchemaParser;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;

import javax.inject.Inject;

import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.util.Objects.requireNonNull;

public class ProtobufSchemaParser
        implements SchemaParser
{
    private static final String TIMESTAMP_TYPE_NAME = "google.protobuf.Timestamp";
    private final TypeManager typeManager;

    @Inject
    public ProtobufSchemaParser(TypeManager typeManager)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public KafkaTopicFieldGroup parse(ConnectorSession session, String subject, ParsedSchema parsedSchema)
    {
        ProtobufSchema protobufSchema = (ProtobufSchema) parsedSchema;
        return new KafkaTopicFieldGroup(
                ProtobufRowDecoder.NAME,
                Optional.empty(),
                Optional.of(subject),
                protobufSchema.toDescriptor().getFields().stream()
                        .map(field -> new KafkaTopicFieldDescription(
                                field.getName(),
                                getType(field),
                                field.getName(),
                                null,
                                null,
                                null,
                                false))
                        .collect(toImmutableList()));
    }

    private Type getType(FieldDescriptor fieldDescriptor)
    {
        Type baseType = switch (fieldDescriptor.getJavaType()) {
            case BOOLEAN -> BOOLEAN;
            case INT -> INTEGER;
            case LONG -> BIGINT;
            case FLOAT -> REAL;
            case DOUBLE -> DOUBLE;
            case BYTE_STRING -> VARBINARY;
            case STRING, ENUM -> createUnboundedVarcharType();
            case MESSAGE -> getTypeForMessage(fieldDescriptor);
        };

        // Protobuf does not support adding repeated label for map type but schema registry incorrecty adds it
        if (fieldDescriptor.isRepeated() && !fieldDescriptor.isMapField()) {
            return new ArrayType(baseType);
        }
        return baseType;
    }

    private Type getTypeForMessage(FieldDescriptor fieldDescriptor)
    {
        Descriptor descriptor = fieldDescriptor.getMessageType();
        if (fieldDescriptor.getMessageType().getFullName().equals(TIMESTAMP_TYPE_NAME)) {
            return createTimestampType(6);
        }
        if (fieldDescriptor.isMapField()) {
            return new MapType(
                    getType(descriptor.findFieldByNumber(1)),
                    getType(descriptor.findFieldByNumber(2)),
                    typeManager.getTypeOperators());
        }
        return RowType.from(
                fieldDescriptor.getMessageType().getFields().stream()
                        .map(field -> RowType.field(field.getName(), getType(field)))
                        .collect(toImmutableList()));
    }
}
