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
package io.trino.plugin.pulsar.decoder.json;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.decoder.DecoderColumnHandle;
import io.trino.plugin.pulsar.PulsarColumnHandle;
import io.trino.plugin.pulsar.PulsarColumnMetadata;
import io.trino.plugin.pulsar.PulsarRowDecoderFactory;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeSignature;
import io.trino.spi.type.TypeSignatureParameter;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.pulsar.client.impl.schema.generic.GenericJsonSchema;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.SchemaInfo;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.pulsar.PulsarColumnMetadata.PROPERTY_KEY_HANDLE_TYPE;
import static io.trino.plugin.pulsar.PulsarColumnMetadata.PROPERTY_KEY_INTERNAL;
import static io.trino.plugin.pulsar.PulsarColumnMetadata.PROPERTY_KEY_MAPPING;
import static io.trino.plugin.pulsar.PulsarColumnMetadata.PROPERTY_KEY_NAME_CASE_SENSITIVE;
import static io.trino.plugin.pulsar.PulsarErrorCode.PULSAR_SCHEMA_ERROR;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.TimeType.TIME_MILLIS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

/**
 * PulsarRowDecoderFactory for {@link org.apache.pulsar.shade.org.apache.pulsar.common.schema.SchemaType#JSON}.
 */
public class PulsarJsonRowDecoderFactory
        implements PulsarRowDecoderFactory
{
    private static final Logger log = Logger.get(PulsarJsonRowDecoderFactory.class);

    private TypeManager typeManager;

    public PulsarJsonRowDecoderFactory(TypeManager typeManager)
    {
        this.typeManager = typeManager;
    }

    @Override
    public PulsarJsonRowDecoder createRowDecoder(TopicName topicName, SchemaInfo schemaInfo, Set<DecoderColumnHandle> columns)
    {
        return new PulsarJsonRowDecoder((GenericJsonSchema) GenericJsonSchema.of(schemaInfo), columns);
    }

    @Override
    public List<ColumnMetadata> extractColumnMetadata(TopicName topicName, SchemaInfo schemaInfo, PulsarColumnHandle.HandleKeyValueType handleKeyValueType, boolean withInternalProperties)
    {
        List<ColumnMetadata> columnMetadata;
        String schemaJson = new String(schemaInfo.getSchema(), StandardCharsets.ISO_8859_1);
        if (Strings.nullToEmpty(schemaJson).trim().isEmpty()) {
            throw new TrinoException(PULSAR_SCHEMA_ERROR, "Topic " + topicName.toString() + " does not have a valid schema");
        }

        Schema schema;
        try {
            schema = GenericJsonSchema.of(schemaInfo).getAvroSchema();
        }
        catch (SchemaParseException ex) {
            throw new TrinoException(PULSAR_SCHEMA_ERROR, "Topic " + topicName.toString() + " does not have a valid schema");
        }

        try {
            columnMetadata = schema.getFields().stream()
                    .map(field -> {
                        ColumnMetadata.Builder metaBuilder = ColumnMetadata.builder()
                                    .setName(PulsarColumnMetadata.getColumnName(handleKeyValueType, field.name()))
                                    .setType(parseJsonTrinoType(field.name(), field.schema()))
                                    .setComment(Optional.of(field.schema().toString()))
                                    .setHidden(false);
                        if (withInternalProperties) {
                            metaBuilder.setProperties(ImmutableMap.of(
                                    PROPERTY_KEY_NAME_CASE_SENSITIVE, PulsarColumnMetadata.getColumnName(handleKeyValueType, field.name()),
                                    PROPERTY_KEY_INTERNAL, false,
                                    PROPERTY_KEY_HANDLE_TYPE, handleKeyValueType,
                                    PROPERTY_KEY_MAPPING, field.name()));
                        }
                        return metaBuilder.build();
                    }).collect(toList());
        }
        catch (StackOverflowError e) {
            log.warn(e, "Topic " + topicName.toString() + " extractColumnMetadata failed.");
            throw new TrinoException(PULSAR_SCHEMA_ERROR, "Topic " + topicName.toString() + " schema may contains cyclic definitions.", e);
        }
        return columnMetadata;
    }

    private Type parseJsonTrinoType(String fieldname, Schema schema)
    {
        Schema.Type type = schema.getType();
        LogicalType logicalType = schema.getLogicalType();
        switch (type) {
            case STRING:
            case ENUM:
                return createUnboundedVarcharType();
            case NULL:
                throw new UnsupportedOperationException(format("field '%s' NULL type code should not be reached ，please check the schema or report the bug.", fieldname));
            case FIXED:
            case BYTES:
                return VarbinaryType.VARBINARY;
            case INT:
                if (logicalType == LogicalTypes.date()) {
                    return DATE;
                }
                return IntegerType.INTEGER;
            case LONG:
                if (logicalType == LogicalTypes.timeMillis()) {
                    return TIME_MILLIS;
                }
                else if (logicalType == LogicalTypes.timestampMillis()) {
                    return TIMESTAMP_MILLIS;
                }
                return BigintType.BIGINT;
            case FLOAT:
                return RealType.REAL;
            case DOUBLE:
                return DoubleType.DOUBLE;
            case BOOLEAN:
                return BooleanType.BOOLEAN;
            case ARRAY:
                return new ArrayType(parseJsonTrinoType(fieldname, schema.getElementType()));
            case MAP:
                //The key for an avro map must be string.
                TypeSignature valueType = parseJsonTrinoType(fieldname, schema.getValueType()).getTypeSignature();
                return typeManager.getParameterizedType(StandardTypes.MAP, ImmutableList.of(TypeSignatureParameter
                                .typeParameter(VarcharType.VARCHAR.getTypeSignature()),
                                TypeSignatureParameter.typeParameter(valueType)));
            case RECORD:
                if (schema.getFields().size() > 0) {
                    return RowType.from(schema.getFields().stream()
                            .map(field -> new RowType.Field(Optional.of(field.name()),
                                    parseJsonTrinoType(field.name(), field.schema())))
                            .collect(toImmutableList()));
                }
                else {
                    throw new UnsupportedOperationException(format("field '%s' of record type has no fields, please check schema definition. ", fieldname));
                }
            case UNION:
                for (Schema nestType : schema.getTypes()) {
                    if (nestType.getType() != Schema.Type.NULL) {
                        return parseJsonTrinoType(fieldname, nestType);
                    }
                }
                throw new UnsupportedOperationException(format("field '%s' of UNION type must contains not NULL type.", fieldname));
            default:
                throw new UnsupportedOperationException(format("Can't convert from schema type '%s' (%s) to trino type.", schema.getType(), schema.getFullName()));
        }
    }
}
