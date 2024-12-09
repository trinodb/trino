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
package io.trino.plugin.functions.python;

import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.SqlMap;
import io.trino.spi.block.SqlRow;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.LongTimeWithTimeZone;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.math.BigDecimal;
import java.util.List;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.base.util.JsonTypeUtil.jsonParse;
import static io.trino.plugin.functions.python.TimeZoneOffset.zoneOffsetMinutes;
import static io.trino.spi.StandardErrorCode.FUNCTION_IMPLEMENTATION_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.block.ArrayValueBuilder.buildArrayValue;
import static io.trino.spi.block.MapValueBuilder.buildMapValue;
import static io.trino.spi.block.RowValueBuilder.buildRowValue;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateTimeEncoding.packTimeWithTimeZone;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateTimeEncoding.unpackOffsetMinutes;
import static io.trino.spi.type.DateTimeEncoding.unpackTimeNanos;
import static io.trino.spi.type.DateTimeEncoding.unpackZoneKey;
import static io.trino.spi.type.Decimals.encodeScaledValue;
import static io.trino.spi.type.Decimals.encodeShortScaledValue;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKeyForOffset;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.round;
import static io.trino.spi.type.Timestamps.roundDiv;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static java.lang.Math.toIntExact;
import static java.math.RoundingMode.HALF_UP;

final class TrinoTypes
{
    private TrinoTypes() {}

    public static void validateReturnType(Type type)
    {
        switch (type) {
            case RowType rowType -> {
                for (RowType.Field field : rowType.getFields()) {
                    validateReturnType(field.getType());
                }
            }
            case ArrayType arrayType -> validateReturnType(arrayType.getElementType());
            case MapType mapType -> {
                validateReturnType(mapType.getKeyType());
                validateReturnType(mapType.getValueType());
            }
            case VarcharType varcharType -> {
                if (!varcharType.isUnbounded()) {
                    throw new TrinoException(NOT_SUPPORTED, "Type VARCHAR(x) not supported as return type. Use VARCHAR instead.");
                }
            }
            default -> {}
        }
    }

    public static Slice toRowTypeDescriptor(List<Type> types)
    {
        if (types.isEmpty()) {
            SliceOutput output = new DynamicSliceOutput(8);
            output.writeInt(TrinoType.ROW.id());
            output.writeInt(0);
            return output.slice();
        }

        return toTypeDescriptor(RowType.anonymous(types));
    }

    public static Slice toTypeDescriptor(Type type)
    {
        SliceOutput output = new DynamicSliceOutput(64);
        toTypeDescriptor(type, output);
        return output.slice();
    }

    private static void toTypeDescriptor(Type type, SliceOutput output)
    {
        switch (type) {
            case RowType rowType -> {
                output.writeInt(TrinoType.ROW.id());
                output.writeInt(rowType.getFields().size());
                for (RowType.Field field : rowType.getFields()) {
                    toTypeDescriptor(field.getType(), output);
                }
            }
            case ArrayType arrayType -> {
                output.writeInt(TrinoType.ARRAY.id());
                toTypeDescriptor(arrayType.getElementType(), output);
            }
            case MapType mapType -> {
                output.writeInt(TrinoType.MAP.id());
                toTypeDescriptor(mapType.getKeyType(), output);
                toTypeDescriptor(mapType.getValueType(), output);
            }
            default -> output.writeInt(singletonType(type).id());
        }
    }

    private static TrinoType singletonType(Type type)
    {
        return switch (type.getBaseName()) {
            case StandardTypes.BOOLEAN -> TrinoType.BOOLEAN;
            case StandardTypes.BIGINT -> TrinoType.BIGINT;
            case StandardTypes.INTEGER -> TrinoType.INTEGER;
            case StandardTypes.SMALLINT -> TrinoType.SMALLINT;
            case StandardTypes.TINYINT -> TrinoType.TINYINT;
            case StandardTypes.DOUBLE -> TrinoType.DOUBLE;
            case StandardTypes.REAL -> TrinoType.REAL;
            case StandardTypes.DECIMAL -> TrinoType.DECIMAL;
            case StandardTypes.VARCHAR -> TrinoType.VARCHAR;
            case StandardTypes.VARBINARY -> TrinoType.VARBINARY;
            case StandardTypes.DATE -> TrinoType.DATE;
            case StandardTypes.TIME -> TrinoType.TIME;
            case StandardTypes.TIME_WITH_TIME_ZONE -> TrinoType.TIME_WITH_TIME_ZONE;
            case StandardTypes.TIMESTAMP -> TrinoType.TIMESTAMP;
            case StandardTypes.TIMESTAMP_WITH_TIME_ZONE -> TrinoType.TIMESTAMP_WITH_TIME_ZONE;
            case StandardTypes.INTERVAL_YEAR_TO_MONTH -> TrinoType.INTERVAL_YEAR_TO_MONTH;
            case StandardTypes.INTERVAL_DAY_TO_SECOND -> TrinoType.INTERVAL_DAY_TO_SECOND;
            case StandardTypes.JSON -> TrinoType.JSON;
            case StandardTypes.UUID -> TrinoType.UUID;
            case StandardTypes.IPADDRESS -> TrinoType.IPADDRESS;
            default -> throw new TrinoException(NOT_SUPPORTED, "Unsupported type: " + type);
        };
    }

    public static Slice javaToBinary(List<Type> types, Object[] values)
    {
        SliceOutput output = new DynamicSliceOutput(64);
        output.writeByte(1); // row present
        for (int i = 0; i < types.size(); i++) {
            javaToBinary(types.get(i), values[i], output);
        }
        return output.slice();
    }

    private static void javaToBinary(Type type, Object value, SliceOutput output)
    {
        if (value == null) {
            output.writeByte(0);
            return;
        }
        output.writeByte(1);

        switch (type) {
            case RowType rowType -> rowBlockToBinary((SqlRow) value, output, rowType);
            case ArrayType arrayType -> arrayBlockToBinary((Block) value, output, arrayType);
            case MapType mapType -> mapBlockToBinary((SqlMap) value, output, mapType);
            case DecimalType decimalType -> {
                String decimalString = decimalType.isShort()
                        ? Decimals.toString((long) value, decimalType.getScale())
                        : Decimals.toString((Int128) value, decimalType.getScale());
                writeVariableSlice(utf8Slice(decimalString), output);
            }
            case TimeWithTimeZoneType timeType -> {
                if (timeType.isShort()) {
                    long time = (long) value;
                    output.writeLong(nanosToMicros(unpackTimeNanos(time)));
                    output.writeShort(unpackOffsetMinutes(time));
                }
                else {
                    LongTimeWithTimeZone time = (LongTimeWithTimeZone) value;
                    output.writeLong(picosToMicros(time.getPicoseconds()));
                    output.writeShort(time.getOffsetMinutes());
                }
            }
            case TimestampType timestampType -> output.writeLong(timestampType.isShort()
                    ? (long) value
                    : timestampToMicros((LongTimestamp) value));
            case TimestampWithTimeZoneType timestampType -> {
                if (timestampType.isShort()) {
                    long packed = (long) value;
                    long millis = unpackMillisUtc(packed);
                    output.writeLong(millis * MICROSECONDS_PER_MILLISECOND);
                    output.writeShort(zoneOffsetMinutes(millis, unpackZoneKey(packed).getKey()));
                }
                else {
                    LongTimestampWithTimeZone timestamp = (LongTimestampWithTimeZone) value;
                    long micros = timestamp.getEpochMillis() * MICROSECONDS_PER_MILLISECOND;
                    output.writeLong(micros + picosToMicros(timestamp.getPicosOfMilli()));
                    output.writeShort(zoneOffsetMinutes(timestamp.getEpochMillis(), timestamp.getTimeZoneKey()));
                }
            }
            default -> javaToBinarySimple(type, value, output);
        }
    }

    @SuppressWarnings("DuplicateBranchesInSwitch")
    private static void javaToBinarySimple(Type type, Object value, SliceOutput output)
    {
        switch (type.getBaseName()) {
            case StandardTypes.BOOLEAN -> output.writeByte((boolean) value ? 1 : 0);
            case StandardTypes.BIGINT -> output.writeLong((long) value);
            case StandardTypes.INTEGER -> output.writeInt(toIntExact((long) value));
            case StandardTypes.SMALLINT -> output.writeShort(toIntExact((long) value));
            case StandardTypes.TINYINT -> output.writeByte(toIntExact((long) value));
            case StandardTypes.DOUBLE -> output.writeDouble((double) value);
            case StandardTypes.REAL -> output.writeInt(toIntExact((long) value));
            case StandardTypes.DATE -> output.writeInt(toIntExact((long) value));
            case StandardTypes.TIME -> output.writeLong(picosToMicros((long) value));
            case StandardTypes.INTERVAL_YEAR_TO_MONTH -> output.writeInt(toIntExact((long) value));
            case StandardTypes.INTERVAL_DAY_TO_SECOND -> output.writeLong((long) value);
            case StandardTypes.UUID,
                 StandardTypes.IPADDRESS -> output.writeBytes((Slice) value);
            case StandardTypes.VARCHAR,
                 StandardTypes.VARBINARY,
                 StandardTypes.JSON -> writeVariableSlice((Slice) value, output);
            default -> throw new TrinoException(NOT_SUPPORTED, "Unsupported type: " + type);
        }
    }

    private static void blockToBinary(Type type, Block block, int position, SliceOutput output)
    {
        if (block.isNull(position)) {
            output.writeByte(0);
            return;
        }
        output.writeByte(1);

        switch (type) {
            case RowType rowType -> rowBlockToBinary(rowType.getObject(block, position), output, rowType);
            case ArrayType arrayType -> arrayBlockToBinary(arrayType.getObject(block, position), output, arrayType);
            case MapType mapType -> mapBlockToBinary(mapType.getObject(block, position), output, mapType);
            case BooleanType booleanType -> output.writeBoolean(booleanType.getBoolean(block, position));
            case BigintType bigintType -> output.writeLong(bigintType.getLong(block, position));
            case IntegerType integerType -> output.writeInt(integerType.getInt(block, position));
            case SmallintType smallintType -> output.writeShort(smallintType.getShort(block, position));
            case TinyintType tinyintType -> output.writeByte(tinyintType.getByte(block, position));
            case DoubleType doubleType -> output.writeDouble(doubleType.getDouble(block, position));
            case RealType realType -> output.writeFloat(realType.getFloat(block, position));
            case DecimalType decimalType -> {
                String decimalString = decimalType.isShort()
                        ? Decimals.toString(decimalType.getLong(block, position), decimalType.getScale())
                        : Decimals.toString((Int128) decimalType.getObject(block, position), decimalType.getScale());
                writeVariableSlice(utf8Slice(decimalString), output);
            }
            case DateType dateType -> output.writeInt(dateType.getInt(block, position));
            case TimeType timeType -> output.writeLong(picosToMicros(timeType.getLong(block, position)));
            case TimeWithTimeZoneType timeType -> {
                if (timeType.isShort()) {
                    long time = timeType.getLong(block, position);
                    output.writeLong(nanosToMicros(unpackTimeNanos(time)));
                    output.writeShort(unpackOffsetMinutes(time));
                }
                else {
                    LongTimeWithTimeZone time = (LongTimeWithTimeZone) timeType.getObject(block, position);
                    output.writeLong(picosToMicros(time.getPicoseconds()));
                    output.writeShort(time.getOffsetMinutes());
                }
            }
            case TimestampType timestampType -> output.writeLong(timestampType.isShort()
                    ? timestampType.getLong(block, position)
                    : timestampToMicros((LongTimestamp) timestampType.getObject(block, position)));
            case TimestampWithTimeZoneType timestampType -> {
                if (timestampType.isShort()) {
                    long packed = timestampType.getLong(block, position);
                    long millis = unpackMillisUtc(packed);
                    output.writeLong(millis * MICROSECONDS_PER_MILLISECOND);
                    output.writeShort(zoneOffsetMinutes(millis, unpackZoneKey(packed).getKey()));
                }
                else {
                    LongTimestampWithTimeZone timestamp = (LongTimestampWithTimeZone) timestampType.getObject(block, position);
                    long micros = timestamp.getEpochMillis() * MICROSECONDS_PER_MILLISECOND;
                    output.writeLong(micros + picosToMicros(timestamp.getPicosOfMilli()));
                    output.writeShort(zoneOffsetMinutes(timestamp.getEpochMillis(), timestamp.getTimeZoneKey()));
                }
            }
            default -> blockToBinarySimple(type, block, position, output);
        }
    }

    private static void blockToBinarySimple(Type type, Block block, int position, SliceOutput output)
    {
        switch (type.getBaseName()) {
            case StandardTypes.INTERVAL_YEAR_TO_MONTH -> output.writeInt(toIntExact(type.getLong(block, position)));
            case StandardTypes.INTERVAL_DAY_TO_SECOND -> output.writeLong(type.getLong(block, position));
            case StandardTypes.UUID,
                 StandardTypes.IPADDRESS -> output.writeBytes(type.getSlice(block, position));
            case StandardTypes.VARCHAR,
                 StandardTypes.VARBINARY,
                 StandardTypes.JSON -> writeVariableSlice(type.getSlice(block, position), output);
            default -> throw new TrinoException(NOT_SUPPORTED, "Unsupported type: " + type);
        }
    }

    private static void rowBlockToBinary(SqlRow row, SliceOutput output, RowType rowType)
    {
        for (int i = 0; i < rowType.getFields().size(); i++) {
            blockToBinary(
                    rowType.getFields().get(i).getType(),
                    row.getUnderlyingFieldBlock(i),
                    row.getUnderlyingFieldPosition(i),
                    output);
        }
    }

    private static void arrayBlockToBinary(Block value, SliceOutput output, ArrayType arrayType)
    {
        ValueBlock array = value.getUnderlyingValueBlock();
        output.writeInt(array.getPositionCount());
        for (int i = 0; i < array.getPositionCount(); i++) {
            blockToBinary(arrayType.getElementType(), array, i, output);
        }
    }

    private static void mapBlockToBinary(SqlMap map, SliceOutput output, MapType mapType)
    {
        output.writeInt(map.getSize());
        for (int i = 0; i < map.getSize(); i++) {
            blockToBinary(
                    mapType.getKeyType(),
                    map.getUnderlyingKeyBlock(),
                    map.getUnderlyingKeyPosition(i),
                    output);
            blockToBinary(
                    mapType.getValueType(),
                    map.getUnderlyingValueBlock(),
                    map.getUnderlyingValuePosition(i),
                    output);
        }
    }

    public static Object binaryToJava(Type type, SliceInput input)
    {
        if (!input.readBoolean()) {
            return null;
        }

        return switch (type) {
            case RowType rowType -> rowBinaryToJava(rowType, input);
            case ArrayType arrayType -> binaryArrayToJava(arrayType, input);
            case MapType mapType -> binaryMapToJava(mapType, input);
            case DecimalType decimalType -> {
                BigDecimal decimal = new BigDecimal(input.readSlice(input.readInt()).toStringUtf8());
                yield decimalType.isShort()
                        ? encodeShortScaledValue(decimal, decimalType.getScale(), HALF_UP)
                        : encodeScaledValue(decimal, decimalType.getScale(), HALF_UP);
            }
            case TimeType timeType -> {
                long micros = roundMicros(input.readLong(), timeType.getPrecision()) % MICROSECONDS_PER_DAY;
                yield micros * PICOSECONDS_PER_MICROSECOND;
            }
            case TimeWithTimeZoneType timeType -> {
                long micros = roundMicros(input.readLong(), timeType.getPrecision()) % MICROSECONDS_PER_DAY;
                short offset = input.readShort();
                yield timeType.isShort()
                        ? packTimeWithTimeZone(micros * NANOSECONDS_PER_MICROSECOND, offset)
                        : new LongTimeWithTimeZone(micros * PICOSECONDS_PER_MICROSECOND, offset);
            }
            case TimestampType timestampType -> {
                long micros = roundMicros(input.readLong(), timestampType.getPrecision());
                yield timestampType.isShort() ? micros : new LongTimestamp(micros, 0);
            }
            case TimestampWithTimeZoneType timestampType -> {
                long micros = roundMicros(input.readLong(), timestampType.getPrecision());
                TimeZoneKey zoneKey = getTimeZoneKeyForOffset(input.readShort());
                if (timestampType.isShort()) {
                    long millis = roundDiv(micros, MICROSECONDS_PER_MILLISECOND);
                    yield packDateTimeWithZone(millis, zoneKey);
                }
                long millis = micros / MICROSECONDS_PER_MILLISECOND;
                int picos = (int) (micros % MICROSECONDS_PER_MILLISECOND) * PICOSECONDS_PER_MICROSECOND;
                yield LongTimestampWithTimeZone.fromEpochMillisAndFraction(millis, picos, zoneKey);
            }
            default -> binaryToJavaSimple(type, input);
        };
    }

    private static Object rowBinaryToJava(RowType rowType, SliceInput input)
    {
        return buildRowValue(rowType, fieldBuilders -> {
            for (int i = 0; i < rowType.getFields().size(); i++) {
                Type fieldType = rowType.getFields().get(i).getType();
                Object value = binaryToJava(fieldType, input);
                writeNativeValue(fieldType, fieldBuilders.get(i), value);
            }
        });
    }

    private static Object binaryArrayToJava(ArrayType arrayType, SliceInput input)
    {
        int count = input.readInt();
        return buildArrayValue(arrayType, count, builder -> {
            for (int i = 0; i < count; i++) {
                Object element = binaryToJava(arrayType.getElementType(), input);
                writeNativeValue(arrayType.getElementType(), builder, element);
            }
        });
    }

    private static Object binaryMapToJava(MapType mapType, SliceInput input)
    {
        int count = input.readInt();
        return buildMapValue(mapType, count, (keyBuilder, valueBuilder) -> {
            for (int i = 0; i < count; i++) {
                Object key = binaryToJava(mapType.getKeyType(), input);
                Object value = binaryToJava(mapType.getValueType(), input);
                writeNativeValue(mapType.getKeyType(), keyBuilder, key);
                writeNativeValue(mapType.getValueType(), valueBuilder, value);
            }
        });
    }

    @SuppressWarnings("DuplicateBranchesInSwitch")
    private static Object binaryToJavaSimple(Type type, SliceInput input)
    {
        return switch (type.getBaseName()) {
            case StandardTypes.BOOLEAN -> input.readBoolean();
            case StandardTypes.BIGINT -> input.readLong();
            case StandardTypes.INTEGER -> (long) input.readInt();
            case StandardTypes.SMALLINT -> (long) input.readShort();
            case StandardTypes.TINYINT -> (long) input.readByte();
            case StandardTypes.DOUBLE -> input.readDouble();
            case StandardTypes.REAL -> (long) input.readInt();
            case StandardTypes.DATE -> (long) input.readInt();
            case StandardTypes.INTERVAL_YEAR_TO_MONTH -> (long) input.readInt();
            case StandardTypes.INTERVAL_DAY_TO_SECOND -> input.readLong();
            case StandardTypes.UUID,
                 StandardTypes.IPADDRESS -> input.readSlice(16);
            case StandardTypes.VARCHAR,
                 StandardTypes.VARBINARY -> input.readSlice(input.readInt());
            case StandardTypes.JSON -> toJson(input.readSlice(input.readInt()));
            default -> throw new TrinoException(NOT_SUPPORTED, "Unsupported type: " + type);
        };
    }

    private static void writeVariableSlice(Slice value, SliceOutput output)
    {
        output.writeInt(value.length());
        output.writeBytes(value);
    }

    private static long roundMicros(long micros, int precision)
    {
        return (precision < 6) ? round(micros, 6 - precision) : micros;
    }

    private static long nanosToMicros(long nanos)
    {
        return roundDiv(nanos, NANOSECONDS_PER_MICROSECOND);
    }

    private static long picosToMicros(long picos)
    {
        return roundDiv(picos, PICOSECONDS_PER_MICROSECOND);
    }

    private static long timestampToMicros(LongTimestamp timestamp)
    {
        long micros = timestamp.getEpochMicros();
        if (timestamp.getPicosOfMicro() >= PICOSECONDS_PER_MICROSECOND / 2) {
            micros++;
        }
        return micros;
    }

    private static Slice toJson(Slice value)
    {
        try {
            return jsonParse(value);
        }
        catch (TrinoException e) {
            throw new TrinoException(FUNCTION_IMPLEMENTATION_ERROR, "Python function returned invalid JSON value", e);
        }
    }
}
