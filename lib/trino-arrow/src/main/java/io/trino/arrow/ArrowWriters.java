package io.trino.arrow;

import io.trino.arrow.writer.*;
import io.trino.spi.type.*;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.*;

public final class ArrowWriters
{
    private ArrowWriters() {}

    public static ArrowColumnWriter createWriter(ValueVector vector, Type type)
    {
        return switch (vector) {
            case BitVector v -> new BooleanColumnWriter(v);
            case TinyIntVector v -> new TinyIntColumnWriter(v);
            case SmallIntVector v -> new SmallIntColumnWriter(v);
            case IntVector v -> new IntegerColumnWriter(v);
            case BigIntVector v -> new BigIntColumnWriter(v);
            case Float4Vector v -> new RealColumnWriter(v);
            case Float8Vector v -> new DoubleColumnWriter(v);
            case VarCharVector v -> type instanceof CharType ? new CharColumnWriter(v, (CharType) type) : new VarcharColumnWriter(v);
            case VarBinaryVector v -> new VarbinaryColumnWriter(v);
            case DateDayVector v -> new DateColumnWriter(v);
            case DecimalVector v -> new DecimalColumnWriter(v, (DecimalType) type);
            case FixedSizeBinaryVector v -> new UuidColumnWriter(v);
            case TimeSecVector v -> new TimeSecColumnWriter(v, (TimeType) type);
            case TimeMilliVector v -> new TimeMilliColumnWriter(v, (TimeType) type);
            case TimeMicroVector v -> new TimeMicroColumnWriter(v, (TimeType) type);
            case TimeNanoVector v -> new TimeNanoColumnWriter(v, (TimeType) type);
            case TimeStampSecVector v -> new TimeStampSecColumnWriter(v, (TimestampType) type);
            case TimeStampMilliVector v -> new TimeStampMilliColumnWriter(v, (TimestampType) type);
            case TimeStampMicroVector v -> new TimeStampMicroColumnWriter(v, (TimestampType) type);
            case TimeStampNanoVector v -> new TimeStampNanoColumnWriter(v, (TimestampType) type);
            case MapVector v -> buildMapColumnWriter(v, type);
            case ListVector v -> new ArrayColumnWriter(v, createWriter(v.getDataVector(), ((ArrayType) type).getElementType()));
            case StructVector v -> new StructColumnWriter(v, type);
            default -> throw new UnsupportedOperationException("Unsupported vector type: " + vector.getClass().getName());
        };
    }

    private static MapColumnWriter buildMapColumnWriter(MapVector mapVector, Type trinoType){
        if(mapVector.getDataVector() instanceof StructVector structVector && trinoType instanceof MapType mapType){
            return new MapColumnWriter(mapVector,
                    createWriter(structVector.getChildByOrdinal(0), mapType.getKeyType()),
                    createWriter(structVector.getChildByOrdinal(1), mapType.getValueType()));
        }else{
            throw new UnsupportedOperationException("Unsupported data vector : " + mapVector.getDataVector().getClass());
        }
    }
}
