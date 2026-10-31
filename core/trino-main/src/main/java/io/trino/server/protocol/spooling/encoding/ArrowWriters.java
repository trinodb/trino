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
package io.trino.server.protocol.spooling.encoding;

import io.airlift.slice.Slice;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.ColumnarArray;
import io.trino.spi.block.ColumnarMap;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.type.IntervalDayTimeType;
import io.trino.type.IntervalYearMonthType;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.DurationVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.IntervalMonthDayNanoVector;
import org.apache.arrow.vector.NullVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.IntervalUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.ExtensionType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.block.RowBlock.getRowFieldsFromBlock;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.Chars.padSpaces;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.UuidType.UUID;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.type.JsonType.JSON;
import static io.trino.type.UnknownType.UNKNOWN;
import static java.lang.Math.floorDiv;
import static java.lang.Math.multiplyExact;
import static java.lang.String.format;

/**
 * Maps Trino types to Arrow fields and writes Trino blocks into Arrow vectors.
 * Only types with a lossless Arrow equivalent are supported, everything else fails the query.
 */
public final class ArrowWriters
{
    private static final Map<String, String> UUID_EXTENSION = Map.of(
            ExtensionType.EXTENSION_METADATA_KEY_NAME, "arrow.uuid",
            ExtensionType.EXTENSION_METADATA_KEY_METADATA, "");

    private ArrowWriters() {}

    public interface ColumnWriter
    {
        void write(Block block);
    }

    private interface ValueWriter
    {
        void write(Block block, int position);
    }

    public static Field toField(String name, Type type)
    {
        return toField(name, type, true);
    }

    private static Field toField(String name, Type type, boolean nullable)
    {
        return switch (type) {
            case ArrayType arrayType -> new Field(name, new FieldType(nullable, ArrowType.List.INSTANCE, null), List.of(toField("element", arrayType.getElementType())));
            case MapType mapType -> new Field(name, new FieldType(nullable, new ArrowType.Map(false), null), List.of(
                    new Field(MapVector.DATA_VECTOR_NAME, FieldType.notNullable(ArrowType.Struct.INSTANCE), List.of(
                            toField(MapVector.KEY_NAME, mapType.getKeyType(), false),
                            toField(MapVector.VALUE_NAME, mapType.getValueType())))));
            case RowType rowType -> new Field(name, new FieldType(nullable, ArrowType.Struct.INSTANCE, null), IntStream.range(0, rowType.getFields().size())
                    .mapToObj(i -> toField(rowType.getFields().get(i).getName().orElse("field" + i), rowType.getFields().get(i).getType()))
                    .toList());
            // arrow.uuid canonical extension, stored as FixedSizeBinary(16)
            case Type _ when type.equals(UUID) -> new Field(name, new FieldType(nullable, toArrowType(type), null, UUID_EXTENSION), null);
            default -> new Field(name, new FieldType(nullable, toArrowType(type), null), null);
        };
    }

    private static ArrowType toArrowType(Type type)
    {
        if (type.equals(BOOLEAN)) {
            return ArrowType.Bool.INSTANCE;
        }
        if (type.equals(TINYINT)) {
            return new ArrowType.Int(8, true);
        }
        if (type.equals(SMALLINT)) {
            return new ArrowType.Int(16, true);
        }
        if (type.equals(INTEGER)) {
            return new ArrowType.Int(32, true);
        }
        if (type.equals(BIGINT)) {
            return new ArrowType.Int(64, true);
        }
        if (type.equals(REAL)) {
            return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
        }
        if (type.equals(DOUBLE)) {
            return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
        }
        if (type instanceof VarcharType || type instanceof CharType || type.equals(JSON)) {
            return ArrowType.Utf8.INSTANCE;
        }
        if (type.equals(VARBINARY)) {
            return ArrowType.Binary.INSTANCE;
        }
        if (type.equals(UUID)) {
            return new ArrowType.FixedSizeBinary(16);
        }
        if (type.equals(DATE)) {
            return new ArrowType.Date(DateUnit.DAY);
        }
        if (type.equals(UNKNOWN)) {
            return ArrowType.Null.INSTANCE;
        }
        if (type instanceof DecimalType decimalType) {
            return new ArrowType.Decimal(decimalType.getPrecision(), decimalType.getScale(), 128);
        }
        if (type instanceof TimeType timeType && timeType.getPrecision() <= 9) {
            TimeUnit unit = timeUnit(timeType.getPrecision());
            return new ArrowType.Time(unit, unit == TimeUnit.SECOND || unit == TimeUnit.MILLISECOND ? 32 : 64);
        }
        if (type instanceof TimestampType timestampType && timestampType.getPrecision() <= 9) {
            return new ArrowType.Timestamp(timeUnit(timestampType.getPrecision()), null);
        }
        if (type instanceof TimestampWithTimeZoneType timestampType && timestampType.getPrecision() <= 9) {
            return new ArrowType.Timestamp(timeUnit(timestampType.getPrecision()), "UTC");
        }
        if (type instanceof IntervalYearMonthType) {
            // MONTH_DAY_NANO is the most widely supported Arrow interval, for example pyarrow cannot read YEAR_MONTH values
            return new ArrowType.Interval(IntervalUnit.MONTH_DAY_NANO);
        }
        if (type instanceof IntervalDayTimeType intervalType && intervalType.getFractionalPrecision() <= IntervalDayTimeType.MAX_SHORT_FRACTIONAL_PRECISION) {
            return new ArrowType.Duration(TimeUnit.MICROSECOND);
        }
        throw new TrinoException(NOT_SUPPORTED, format("Type %s is not supported by the arrow spooling encoding, use json encoding instead", type));
    }

    public static ColumnWriter createWriter(FieldVector vector, Type type)
    {
        return switch (type) {
            case ArrayType arrayType -> arrayWriter((ListVector) vector, arrayType);
            case MapType mapType -> mapWriter((MapVector) vector, mapType);
            case RowType rowType -> rowWriter((StructVector) vector, rowType);
            default -> valueWriter(vector, primitiveWriter(vector, type));
        };
    }

    private static ValueWriter primitiveWriter(FieldVector vector, Type type)
    {
        return switch (vector) {
            case BitVector v -> (block, position) -> v.setSafe(position, BOOLEAN.getBoolean(block, position) ? 1 : 0);
            case TinyIntVector v -> (block, position) -> v.setSafe(position, TINYINT.getByte(block, position));
            case SmallIntVector v -> (block, position) -> v.setSafe(position, SMALLINT.getShort(block, position));
            case IntVector v -> (block, position) -> v.setSafe(position, INTEGER.getInt(block, position));
            case BigIntVector v -> (block, position) -> v.setSafe(position, BIGINT.getLong(block, position));
            case Float4Vector v -> (block, position) -> v.setSafe(position, REAL.getFloat(block, position));
            case Float8Vector v -> (block, position) -> v.setSafe(position, DOUBLE.getDouble(block, position));
            case DateDayVector v -> (block, position) -> v.setSafe(position, DATE.getInt(block, position));
            case IntervalMonthDayNanoVector v -> (block, position) -> v.setSafe(position, ((IntervalYearMonthType) type).getInt(block, position), 0, 0);
            case DurationVector v -> (block, position) -> v.setSafe(position, type.getLong(block, position));
            // Trino stores UUIDs in RFC 4122 byte order
            case FixedSizeBinaryVector v -> (block, position) -> v.setSafe(position, UUID.getSlice(block, position).getBytes());
            case DecimalVector v -> decimalWriter(v, (DecimalType) type);
            case BaseVariableWidthVector v -> type instanceof CharType charType
                    ? (block, position) -> setSlice(v, position, padSpaces(type.getSlice(block, position), charType))
                    : (block, position) -> setSlice(v, position, type.getSlice(block, position));
            case TimeSecVector v -> (block, position) -> v.setSafe(position, (int) (type.getLong(block, position) / 1_000_000_000_000L));
            case TimeMilliVector v -> (block, position) -> v.setSafe(position, (int) (type.getLong(block, position) / 1_000_000_000L));
            case TimeMicroVector v -> (block, position) -> v.setSafe(position, type.getLong(block, position) / 1_000_000L);
            case TimeNanoVector v -> (block, position) -> v.setSafe(position, type.getLong(block, position) / 1_000L);
            case TimeStampVector v when type instanceof TimestampType timestampType -> timestampWriter(v, timestampType);
            case TimeStampVector v when type instanceof TimestampWithTimeZoneType timestampType -> timestampWithTimeZoneWriter(v, timestampType);
            case NullVector _ -> (_, _) -> {};
            default -> throw new IllegalArgumentException("Unexpected vector: " + vector.getClass().getName());
        };
    }

    private static ValueWriter decimalWriter(DecimalVector vector, DecimalType type)
    {
        if (type.isShort()) {
            return (block, position) -> vector.setSafe(position, type.getLong(block, position));
        }
        return (block, position) -> vector.setBigEndianSafe(position, ((Int128) type.getObject(block, position)).toBigEndianBytes());
    }

    // Trino values are rounded to the type precision, so converting to the smallest Arrow unit that holds it is exact
    private static ValueWriter timestampWriter(TimeStampVector vector, TimestampType type)
    {
        int precision = type.getPrecision();
        if (type.isShort()) {
            return (block, position) -> {
                long epochMicros = type.getLong(block, position);
                vector.setSafe(position, switch (precision) {
                    case 0 -> floorDiv(epochMicros, 1_000_000L);
                    case 1, 2, 3 -> floorDiv(epochMicros, 1_000L);
                    default -> epochMicros;
                });
            };
        }
        return (block, position) -> {
            LongTimestamp timestamp = (LongTimestamp) type.getObject(block, position);
            vector.setSafe(position, multiplyExact(timestamp.getEpochMicros(), 1_000L) + timestamp.getPicosOfMicro() / 1_000L);
        };
    }

    private static ValueWriter timestampWithTimeZoneWriter(TimeStampVector vector, TimestampWithTimeZoneType type)
    {
        if (type.isShort()) {
            boolean seconds = type.getPrecision() == 0;
            return (block, position) -> {
                long epochMillis = unpackMillisUtc(type.getLong(block, position));
                vector.setSafe(position, seconds ? floorDiv(epochMillis, 1_000L) : epochMillis);
            };
        }
        boolean micros = type.getPrecision() <= 6;
        return (block, position) -> {
            LongTimestampWithTimeZone timestamp = (LongTimestampWithTimeZone) type.getObject(block, position);
            vector.setSafe(position, micros
                    ? multiplyExact(timestamp.getEpochMillis(), 1_000L) + timestamp.getPicosOfMilli() / 1_000_000L
                    : multiplyExact(timestamp.getEpochMillis(), 1_000_000L) + timestamp.getPicosOfMilli() / 1_000L);
        };
    }

    private static TimeUnit timeUnit(int precision)
    {
        if (precision == 0) {
            return TimeUnit.SECOND;
        }
        if (precision <= 3) {
            return TimeUnit.MILLISECOND;
        }
        return precision <= 6 ? TimeUnit.MICROSECOND : TimeUnit.NANOSECOND;
    }

    private static void setSlice(BaseVariableWidthVector vector, int position, Slice slice)
    {
        vector.setSafe(position, slice.byteArray(), slice.byteArrayOffset(), slice.length());
    }

    private static ColumnWriter valueWriter(FieldVector vector, ValueWriter writer)
    {
        return block -> {
            for (int position = 0; position < block.getPositionCount(); position++) {
                if (block.isNull(position)) {
                    vector.setNull(position);
                }
                else {
                    writer.write(block, position);
                }
            }
            vector.setValueCount(block.getPositionCount());
        };
    }

    private static ColumnWriter arrayWriter(ListVector vector, ArrayType type)
    {
        ColumnWriter elementWriter = createWriter(vector.getDataVector(), type.getElementType());
        return block -> {
            ColumnarArray array = ColumnarArray.toColumnarArray(block);
            for (int position = 0; position < array.getPositionCount(); position++) {
                if (array.isNull(position)) {
                    vector.setNull(position);
                }
                else {
                    vector.startNewValue(position);
                    vector.endValue(position, array.getLength(position));
                }
            }
            elementWriter.write(array.getElementsBlock());
            vector.setValueCount(array.getPositionCount());
        };
    }

    private static ColumnWriter mapWriter(MapVector vector, MapType type)
    {
        StructVector entries = (StructVector) vector.getDataVector();
        ColumnWriter keyWriter = createWriter(entries.getChild(MapVector.KEY_NAME), type.getKeyType());
        ColumnWriter valueWriter = createWriter(entries.getChild(MapVector.VALUE_NAME), type.getValueType());
        return block -> {
            ColumnarMap map = ColumnarMap.toColumnarMap(block);
            for (int position = 0; position < map.getPositionCount(); position++) {
                if (map.isNull(position)) {
                    vector.setNull(position);
                }
                else {
                    vector.startNewValue(position);
                    vector.endValue(position, map.getEntryCount(position));
                }
            }
            int entryCount = map.getKeysBlock().getPositionCount();
            for (int entry = 0; entry < entryCount; entry++) {
                entries.setIndexDefined(entry);
            }
            keyWriter.write(map.getKeysBlock());
            valueWriter.write(map.getValuesBlock());
            entries.setValueCount(entryCount);
            vector.setValueCount(map.getPositionCount());
        };
    }

    private static ColumnWriter rowWriter(StructVector vector, RowType type)
    {
        List<ColumnWriter> fieldWriters = IntStream.range(0, type.getFields().size())
                .mapToObj(i -> createWriter((FieldVector) vector.getChildByOrdinal(i), type.getFields().get(i).getType()))
                .toList();
        return block -> {
            for (int position = 0; position < block.getPositionCount(); position++) {
                if (block.isNull(position)) {
                    vector.setNull(position);
                }
                else {
                    vector.setIndexDefined(position);
                }
            }
            List<Block> fields = getRowFieldsFromBlock(block);
            for (int i = 0; i < fieldWriters.size(); i++) {
                fieldWriters.get(i).write(fields.get(i));
            }
            vector.setValueCount(block.getPositionCount());
        };
    }
}
