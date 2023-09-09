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
package io.trino.plugin.kafka.schema;

import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.avro.Schema;

import static io.trino.spi.type.TimeType.TIME_MICROS;
import static io.trino.spi.type.TimeType.TIME_MILLIS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static java.lang.String.format;
import static org.apache.avro.LogicalTypes.date;
import static org.apache.avro.LogicalTypes.timeMicros;
import static org.apache.avro.LogicalTypes.timeMillis;
import static org.apache.avro.LogicalTypes.timestampMicros;
import static org.apache.avro.LogicalTypes.timestampMillis;

public class TrinoToAvroSchemaConverter
{
    private TrinoToAvroSchemaConverter() {}

    public static Schema fromTrinoType(Type trinoType)
    {
        if (trinoType instanceof BigintType) {
            return Schema.create(Schema.Type.LONG);
        }
        else if (trinoType instanceof IntegerType || trinoType instanceof SmallintType || trinoType instanceof TinyintType) {
            return Schema.create(Schema.Type.INT);
        }
        else if (trinoType instanceof VarcharType) {
            return Schema.create(Schema.Type.STRING);
        }
        else if (trinoType instanceof BooleanType) {
            return Schema.create(Schema.Type.BOOLEAN);
        }
        else if (trinoType instanceof DoubleType) {
            return Schema.create(Schema.Type.DOUBLE);
        }
        else if (trinoType instanceof RealType) {
            return Schema.create(Schema.Type.FLOAT);
        }
        else if (trinoType instanceof VarbinaryType) {
            return Schema.create(Schema.Type.BYTES);
        }
        else if (trinoType instanceof DateType) {
            return date().addToSchema(Schema.create(Schema.Type.INT));
        }
        else if (isTimestampMillisType(trinoType)) {
            return timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
        }
        else if (isTimestampMicrosType(trinoType)) {
            return timestampMicros().addToSchema(Schema.create(Schema.Type.LONG));
        }
        else if (isTimeMillisType(trinoType)) {
            return timeMillis().addToSchema(Schema.create(Schema.Type.INT));
        }
        else if (isTimeMicrosType(trinoType)) {
            return timeMicros().addToSchema(Schema.create(Schema.Type.LONG));
        }
        else {
            throw new UnsupportedOperationException(format("Unsupported type for single value key column: %s", trinoType));
        }
    }

    private static boolean isTimestampMillisType(Type type)
    {
        if (!(type instanceof TimestampType)) {
            return false;
        }
        TimestampType timestampType = (TimestampType) type;
        return timestampType.getPrecision() == TIMESTAMP_MILLIS.getPrecision();
    }

    private static boolean isTimestampMicrosType(Type type)
    {
        if (!(type instanceof TimestampType)) {
            return false;
        }
        TimestampType timestampType = (TimestampType) type;
        return timestampType.getPrecision() == TIMESTAMP_MICROS.getPrecision();
    }

    private static boolean isTimeMillisType(Type type)
    {
        if (!(type instanceof TimeType)) {
            return false;
        }
        TimeType timestampType = (TimeType) type;
        return timestampType.getPrecision() == TIME_MILLIS.getPrecision();
    }

    private static boolean isTimeMicrosType(Type type)
    {
        if (!(type instanceof TimeType)) {
            return false;
        }
        TimeType timestampType = (TimeType) type;
        return timestampType.getPrecision() == TIME_MICROS.getPrecision();
    }
}
