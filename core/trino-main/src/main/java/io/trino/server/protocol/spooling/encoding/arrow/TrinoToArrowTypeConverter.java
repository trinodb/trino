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
package io.trino.server.protocol.spooling.encoding.arrow;

import com.google.common.collect.ImmutableList;
import io.trino.server.protocol.OutputColumn;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.HyperLogLogType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.UuidType;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import io.trino.type.IntervalDayTimeType;
import io.trino.type.IntervalYearMonthType;
import io.trino.type.JsonType;
import io.trino.type.UnknownType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static org.apache.arrow.vector.types.DateUnit.DAY;
import static org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE;
import static org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE;
import static org.apache.arrow.vector.types.IntervalUnit.DAY_TIME;
import static org.apache.arrow.vector.types.IntervalUnit.YEAR_MONTH;
import static org.apache.arrow.vector.types.TimeUnit.MICROSECOND;
import static org.apache.arrow.vector.types.TimeUnit.MILLISECOND;
import static org.apache.arrow.vector.types.TimeUnit.NANOSECOND;
import static org.apache.arrow.vector.types.TimeUnit.SECOND;
import static org.apache.arrow.vector.types.pojo.FieldType.notNullable;
import static org.apache.arrow.vector.types.pojo.FieldType.nullable;

public final class TrinoToArrowTypeConverter
{
    private TrinoToArrowTypeConverter() {}

    public static Field toArrowField(String name, Type type, boolean nullable)
    {
        return switch (type) {
            case ArrayType t -> new Field(name, new FieldType(true, new ArrowType.List(), null), List.of(
                    toArrowField("element", t.getElementType(), true)));
            case MapType mapType -> {
                Field entries = new Field("entries",
                        notNullable(ArrowType.Struct.INSTANCE),
                        List.of(toArrowField("key", mapType.getKeyType(), false), toArrowField("value", mapType.getValueType(), nullable)));
                yield new Field(name, nullable(new ArrowType.Map(false)), List.of(entries));
            }
            case RowType rowType -> {
                List<Field> children = IntStream.range(0, rowType.getFields().size())
                        .mapToObj(i -> {
                            RowType.Field field = rowType.getFields().get(i);
                            String fieldName = field.getName().orElse("field" + i);
                            return toArrowField(fieldName, field.getType(), nullable);
                        })
                        .collect(toImmutableList());
                yield new Field(name, nullable(ArrowType.Struct.INSTANCE), children);
            }
            case UuidType _ -> {
                Map<String, String> metadata = Map.of(
                        "ARROW:extension:name", "arrow.uuid",
                        "ARROW:extension:metadata", "");
                FieldType fieldType = new FieldType(nullable, new ArrowType.FixedSizeBinary(16), null, metadata);
                yield new Field(name, fieldType, null);
            }
            default -> {
                ArrowType arrowType = toArrowField(type);
                FieldType fieldType;

                fieldType = nullableField(arrowType, nullable);

                yield new Field(name, fieldType, null);
            }
        };
    }

    private static ArrowType toArrowField(Type type)
    {
        return switch (type) {
            case BooleanType _ -> new ArrowType.Bool();
            case TinyintType _ -> new ArrowType.Int(8, true);
            case SmallintType _ -> new ArrowType.Int(16, true);
            case IntegerType _ -> new ArrowType.Int(32, true);
            case BigintType _ -> new ArrowType.Int(64, true);
            case RealType _ -> new ArrowType.FloatingPoint(SINGLE);
            case DoubleType _ -> new ArrowType.FloatingPoint(DOUBLE);
            case VarcharType _, CharType _ -> new ArrowType.Utf8();
            case VarbinaryType _ -> new ArrowType.Binary();
            case DateType _ -> new ArrowType.Date(DAY);
            case TimeType time -> switch (time.getPrecision()) {
                case 0 -> new ArrowType.Time(SECOND, 32);
                case 3 -> new ArrowType.Time(MILLISECOND, 32);
                case 6 -> new ArrowType.Time(MICROSECOND, 64);
                case 9 -> new ArrowType.Time(NANOSECOND, 64);
                case 12 -> new ArrowType.Time(NANOSECOND, 64); // Cast picoseconds to nanoseconds
                default -> throw new UnsupportedOperationException("Unsupported time precision: " + time.getPrecision());
            };
            case TimeWithTimeZoneType timeWithTimeZone -> switch (timeWithTimeZone.getPrecision()) {
                case 0 -> new ArrowType.Time(SECOND, 32);
                case 3 -> new ArrowType.Time(MILLISECOND, 32);
                case 6 -> new ArrowType.Time(MICROSECOND, 64);
                case 9 -> new ArrowType.Time(NANOSECOND, 64);
                case 12 -> new ArrowType.Time(NANOSECOND, 64); // Cast picoseconds to nanoseconds
                default -> throw new UnsupportedOperationException("Unsupported time with time zone precision: " + timeWithTimeZone.getPrecision());
            };
            case TimestampType timestamp -> switch (timestamp.getPrecision()) {
                case 0 -> new ArrowType.Timestamp(SECOND, null);
                case 3 -> new ArrowType.Timestamp(MILLISECOND, null);
                case 6 -> new ArrowType.Timestamp(MICROSECOND, null);
                case 9 -> new ArrowType.Timestamp(NANOSECOND, null);
                case 12 -> new ArrowType.Timestamp(NANOSECOND, null); // Cast picoseconds to nanoseconds
                default -> throw new UnsupportedOperationException("Unsupported timestamp precision: " + timestamp.getPrecision());
            };
            case TimestampWithTimeZoneType timestampWithTimeZone -> switch (timestampWithTimeZone.getPrecision()) {
                case 0 -> new ArrowType.Timestamp(SECOND, "UTC");
                case 3 -> new ArrowType.Timestamp(MILLISECOND, "UTC");
                case 6 -> new ArrowType.Timestamp(MICROSECOND, "UTC");
                case 9 -> new ArrowType.Timestamp(NANOSECOND, "UTC");
                case 12 -> new ArrowType.Timestamp(NANOSECOND, "UTC"); // Cast picoseconds to nanoseconds
                default -> throw new UnsupportedOperationException("Unsupported timestamp with time zone precision: " + timestampWithTimeZone.getPrecision());
            };
            case DecimalType decimal -> new ArrowType.Decimal(decimal.getPrecision(), decimal.getScale(), 128);
            case UuidType _ -> new ArrowType.FixedSizeBinary(16);
            case HyperLogLogType _ -> new ArrowType.Binary();
            case ArrayType _ -> new ArrowType.List();
            case MapType _ -> new ArrowType.Map(false);
            case RowType _ -> new ArrowType.Struct();
            case IntervalDayTimeType _ -> new ArrowType.Interval(DAY_TIME);
            case IntervalYearMonthType _ -> new ArrowType.Interval(YEAR_MONTH);
            case JsonType _ -> new ArrowType.Utf8();
            case UnknownType _ -> new ArrowType.Null();
            default -> throw new UnsupportedOperationException("Unsupported type: " + type);
        };
    }

    private static FieldType nullableField(ArrowType type, boolean nullable)
    {
        if (nullable) {
            return nullable(type);
        }
        return notNullable(type);
    }

    public static List<OutputColumn> unsupported(List<OutputColumn> columns)
    {
        ImmutableList.Builder<OutputColumn> builder = ImmutableList.builder();
        for (OutputColumn column : columns) {
            try {
                toArrowField(column.columnName(), column.type(), false);
            }
            catch (UnsupportedOperationException e) {
                builder.add(column);
            }
        }
        return builder.build();
    }
}
