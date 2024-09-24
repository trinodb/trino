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
package io.trino.plugin.pulsar.decoder.primitive;

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.decoder.DecoderColumnHandle;
import io.trino.plugin.pulsar.PulsarColumnHandle;
import io.trino.plugin.pulsar.PulsarColumnMetadata;
import io.trino.plugin.pulsar.PulsarRowDecoder;
import io.trino.plugin.pulsar.PulsarRowDecoderFactory;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.pulsar.client.impl.schema.AbstractSchema;
import org.apache.pulsar.client.impl.schema.AutoConsumeSchema;

import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static io.trino.plugin.pulsar.PulsarColumnMetadata.PROPERTY_KEY_HANDLE_TYPE;
import static io.trino.plugin.pulsar.PulsarColumnMetadata.PROPERTY_KEY_INTERNAL;
import static io.trino.plugin.pulsar.PulsarColumnMetadata.PROPERTY_KEY_MAPPING;
import static io.trino.plugin.pulsar.PulsarColumnMetadata.PROPERTY_KEY_NAME_CASE_SENSITIVE;
import static io.trino.spi.type.TimeType.TIME_MILLIS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;

public class PulsarPrimitiveRowDecoderFactory
        implements PulsarRowDecoderFactory
{
    private static final Logger log = Logger.get(PulsarPrimitiveRowDecoderFactory.class);

    public static final String PRIMITIVE_COLUMN_NAME = "__value__";

    @Override
    public PulsarRowDecoder createRowDecoder(TopicName topicName, SchemaInfo schemaInfo, Set<DecoderColumnHandle> columns)
    {
        if (columns.size() == 1) {
            return new PulsarPrimitiveRowDecoder((AbstractSchema<?>) AutoConsumeSchema.getSchema(schemaInfo), columns.iterator().next());
        }
        else {
            return new PulsarPrimitiveRowDecoder((AbstractSchema<?>) AutoConsumeSchema.getSchema(schemaInfo), null);
        }
    }

    @Override
    public List<ColumnMetadata> extractColumnMetadata(TopicName topicName, SchemaInfo schemaInfo, PulsarColumnHandle.HandleKeyValueType handleKeyValueType, boolean withInternalProperties)
    {
        ColumnMetadata.Builder metaBuilder =
                ColumnMetadata.builder()
                        .setName(PulsarColumnMetadata.getColumnName(handleKeyValueType, PRIMITIVE_COLUMN_NAME))
                        .setType(parsePrimitiveTrinoType(PRIMITIVE_COLUMN_NAME, schemaInfo.getType()))
                        .setComment(Optional.of("The value of the message with primitive type schema"))
                        .setHidden(false);
        if (withInternalProperties) {
            metaBuilder.setProperties(ImmutableMap.of(
                    PROPERTY_KEY_NAME_CASE_SENSITIVE, PulsarColumnMetadata.getColumnName(handleKeyValueType, PRIMITIVE_COLUMN_NAME),
                    PROPERTY_KEY_INTERNAL, false,
                    PROPERTY_KEY_HANDLE_TYPE, handleKeyValueType,
                    PROPERTY_KEY_MAPPING, PRIMITIVE_COLUMN_NAME));
        }
        return Arrays.asList(metaBuilder.build());
    }

    private Type parsePrimitiveTrinoType(String fieldName, SchemaType pulsarType)
    {
        switch (pulsarType) {
            case BOOLEAN:
                return BooleanType.BOOLEAN;
            case INT8:
                return TinyintType.TINYINT;
            case INT16:
                return SmallintType.SMALLINT;
            case INT32:
                return IntegerType.INTEGER;
            case INT64:
                return BigintType.BIGINT;
            case FLOAT:
                return RealType.REAL;
            case DOUBLE:
                return DoubleType.DOUBLE;
            case NONE:
            case BYTES:
                return VarbinaryType.VARBINARY;
            case STRING:
                return VarcharType.VARCHAR;
            case DATE:
                return DateType.DATE;
            case TIME:
                return TIME_MILLIS;
            case TIMESTAMP:
                return TIMESTAMP_MILLIS;
            default:
                log.error("Can't convert type: %s for %s", pulsarType, fieldName);
                return null;
        }
    }
}
