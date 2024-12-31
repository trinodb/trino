package io.trino.arrow;

import io.trino.arrow.type.PicosecondTimeType;
import io.trino.arrow.type.PicosecondTimestampType;
import io.trino.arrow.type.TimeWithValueTimezoneType;
import io.trino.arrow.type.TimestampWithValueTimezoneType;
import io.trino.spi.type.*;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.util.List;
import java.util.stream.Collectors;

public final class ArrowTypeConverter
{
    private ArrowTypeConverter() {}

    public static Field toArrowField(String name, Type type)
    {
        return switch (type) {
            case ArrayType t -> {
                // The list field itself has a List type
                // Its child field represents the list's data type
                Field elementField = new Field("element", new FieldType(true, toArrowType(t.getElementType()), null), null);
                yield new Field(name, new FieldType(true, new ArrowType.List(), null), List.of(elementField));
            }
            case MapType t -> {
                Field keyField = toArrowField("key", t.getKeyType());
                Field valueField = toArrowField("value", t.getValueType());
                Field structField = new Field("entries",
                        FieldType.notNullable(new ArrowType.Struct()),
                        List.of(keyField, valueField));
                yield new Field(name, FieldType.nullable(new ArrowType.Map(false)), List.of(structField));
            }
            case RowType t -> {
                List<Field> children = t.getFields().stream()
                        .map(field -> toArrowField(field.getName().get(), field.getType()))
                        .collect(Collectors.toList());
                yield new Field(name, FieldType.nullable(new ArrowType.Struct()), children);
            }
            default -> new Field(name, FieldType.nullable(toArrowType(type)), null);
        };
    }
    private static ArrowType toArrowType(Type type)
    {
        return switch (type) {
            case BooleanType t -> new ArrowType.Bool();
            case TinyintType t -> new ArrowType.Int(8, true);
            case SmallintType t -> new ArrowType.Int(16, true);
            case IntegerType t -> new ArrowType.Int(32, true);
            case BigintType t -> new ArrowType.Int(64, true);
            case RealType t -> new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
            case DoubleType t -> new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
            case VarcharType t -> new ArrowType.Utf8();
            case CharType t -> new ArrowType.Utf8();
            case VarbinaryType t -> new ArrowType.Binary();
            case DateType t -> new ArrowType.Date(DateUnit.DAY);
            case TimeType t -> switch (t.getPrecision()) {
                case 0 -> new ArrowType.Time(TimeUnit.SECOND, 32);
                case 3 -> new ArrowType.Time(TimeUnit.MILLISECOND, 32);
                case 6 -> new ArrowType.Time(TimeUnit.MICROSECOND, 64);
                case 9 -> new ArrowType.Time(TimeUnit.NANOSECOND, 64);
                case 12 -> new PicosecondTimeType();
                default -> throw new UnsupportedOperationException("Unsupported timestamp precision: " + t.getPrecision());
            };
            case TimeWithTimeZoneType t -> new TimeWithValueTimezoneType(t.getPrecision());
            case TimestampType t -> switch (t.getPrecision()) {
                case 0 -> new ArrowType.Timestamp(TimeUnit.SECOND, null);
                case 3 -> new ArrowType.Timestamp(TimeUnit.MILLISECOND, null);
                case 6 -> new ArrowType.Timestamp(TimeUnit.MICROSECOND, null);
                case 9 -> new ArrowType.Timestamp(TimeUnit.NANOSECOND, null);
                case 12 -> new PicosecondTimestampType(null);
                default -> throw new UnsupportedOperationException("Unsupported timestamp precision: " + t.getPrecision());
            };
            case TimestampWithTimeZoneType t -> new TimestampWithValueTimezoneType(t.getPrecision());
            case DecimalType t -> new ArrowType.Decimal(t.getPrecision(), t.getScale());
            case UuidType t -> new ArrowType.FixedSizeBinary(16);
            case HyperLogLogType t -> new ArrowType.Binary(); //TODO: support HyperLogLogType
            case ArrayType t -> new ArrowType.List();
            case MapType t -> new ArrowType.Map(false);
            case RowType t -> new ArrowType.Struct();
            default -> throw new UnsupportedOperationException("Unsupported type: " + type);
        };
    }
}
