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

import io.airlift.log.Logger;
import io.trino.decoder.DecoderColumnHandle;
import io.trino.plugin.pulsar.PulsarColumnHandle;
import io.trino.plugin.pulsar.PulsarColumnMetadata;
import io.trino.plugin.pulsar.PulsarRowDecoder;
import io.trino.plugin.pulsar.PulsarRowDecoderFactory;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.*;
import org.apache.pulsar.client.impl.schema.AbstractSchema;
import org.apache.pulsar.client.impl.schema.AutoConsumeSchema;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

import java.util.List;
import java.util.Set;

/**
 * Primitive Schema PulsarRowDecoderFactory.
 */
public class PulsarPrimitiveRowDecoderFactory implements PulsarRowDecoderFactory {

    public static final String PRIMITIVE_COLUMN_NAME = "__value__";
    private static final Logger log = Logger.get(PulsarPrimitiveRowDecoderFactory.class);

    @Override
    public PulsarRowDecoder createRowDecoder(TopicName topicName, SchemaInfo schemaInfo,
                                             Set<DecoderColumnHandle> columns) {
        if (columns.size() == 1) {
            return new PulsarPrimitiveRowDecoder((AbstractSchema<?>) AutoConsumeSchema.getSchema(schemaInfo),
                    columns.iterator().next());
        } else {
            return new PulsarPrimitiveRowDecoder((AbstractSchema<?>) AutoConsumeSchema.getSchema(schemaInfo),
                    null);
        }
    }

    @Override
    public List<ColumnMetadata> extractColumnMetadata(TopicName topicName, SchemaInfo schemaInfo,
                                                      PulsarColumnHandle.HandleKeyValueType handleKeyValueType) {
        ColumnMetadata valueColumn = new PulsarColumnMetadata(
                PulsarColumnMetadata.getColumnName(handleKeyValueType, PRIMITIVE_COLUMN_NAME),
                parsePrimitivePrestoType(PRIMITIVE_COLUMN_NAME, schemaInfo.getType()),
                "The value of the message with primitive type schema", null, false, false,
                handleKeyValueType, new PulsarColumnMetadata.DecoderExtraInfo(PRIMITIVE_COLUMN_NAME,
                null, null), null);
        return List.of(valueColumn);
    }

    private Type parsePrimitivePrestoType(String fieldName, SchemaType pulsarType) {
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
                return TimeType.TIME_MILLIS;
            case TIMESTAMP:
                return TimestampType.TIMESTAMP_MILLIS;
            default:
                log.error("Can't convert type: %s for %s", pulsarType, fieldName);
                return null;
        }

    }
}
