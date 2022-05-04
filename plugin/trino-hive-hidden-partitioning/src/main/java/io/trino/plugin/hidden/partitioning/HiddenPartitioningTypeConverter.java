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
package io.trino.plugin.hidden.partitioning;

import io.trino.spi.TrinoException;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.iceberg.types.Types;

import java.util.ArrayList;
import java.util.List;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.TimeType.TIME_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;

/**
 * Taken from Iceberg connector's {@code io.trino.plugin.iceberg.TypeConverter}
 */
public final class HiddenPartitioningTypeConverter
{
    private HiddenPartitioningTypeConverter() {}

    public static org.apache.iceberg.types.Type toIcebergType(Type type)
    {
        if (type instanceof BooleanType) {
            return Types.BooleanType.get();
        }
        if (type instanceof IntegerType) {
            return Types.IntegerType.get();
        }
        if (type instanceof BigintType) {
            return Types.LongType.get();
        }
        if (type instanceof RealType) {
            return Types.FloatType.get();
        }
        if (type instanceof DoubleType) {
            return Types.DoubleType.get();
        }
        if (type instanceof DecimalType) {
            return fromDecimal((DecimalType) type);
        }
        if (type instanceof VarcharType) {
            return Types.StringType.get();
        }
        if (type instanceof VarbinaryType) {
            return Types.BinaryType.get();
        }
        if (type instanceof DateType) {
            return Types.DateType.get();
        }
        if (type.equals(TIME_MICROS)) {
            return Types.TimeType.get();
        }
        if (type.equals(TIMESTAMP_MICROS)) {
            return Types.TimestampType.withoutZone();
        }
        if (type.equals(TIMESTAMP_TZ_MICROS)) {
            return Types.TimestampType.withZone();
        }
        if (type instanceof RowType) {
            return fromRow((RowType) type);
        }
        if (type instanceof ArrayType) {
            return fromArray((ArrayType) type);
        }
        if (type instanceof MapType) {
            return fromMap((MapType) type);
        }
        if (type instanceof TimeType) {
            return Types.TimeType.get();
        }
        if (type instanceof TimestampType) {
            return Types.TimestampType.withoutZone();
        }
        if (type instanceof TimestampWithTimeZoneType) {
            return Types.TimestampType.withZone();
        }
        throw new TrinoException(NOT_SUPPORTED, "Type not supported for Iceberg: " + type.getDisplayName());
    }

    private static org.apache.iceberg.types.Type fromDecimal(DecimalType type)
    {
        return Types.DecimalType.of(type.getPrecision(), type.getScale());
    }

    private static org.apache.iceberg.types.Type fromRow(RowType type)
    {
        List<Types.NestedField> fields = new ArrayList<>();
        for (RowType.Field field : type.getFields()) {
            String name = field.getName().orElseThrow(() ->
                    new TrinoException(NOT_SUPPORTED, "Row type field does not have a name: " + type.getDisplayName()));
            fields.add(Types.NestedField.optional(fields.size() + 1, name, toIcebergType(field.getType())));
        }
        return Types.StructType.of(fields);
    }

    private static org.apache.iceberg.types.Type fromArray(ArrayType type)
    {
        return Types.ListType.ofOptional(1, toIcebergType(type.getElementType()));
    }

    private static org.apache.iceberg.types.Type fromMap(MapType type)
    {
        return Types.MapType.ofOptional(1, 2, toIcebergType(type.getKeyType()), toIcebergType(type.getValueType()));
    }
}
