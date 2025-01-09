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
package io.trino.plugin.paimon;

import io.trino.spi.type.BigintType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BinaryType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeDefaultVisitor;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.DateType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.SmallIntType;
import org.apache.paimon.types.TimeType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.TinyIntType;
import org.apache.paimon.types.VarBinaryType;
import org.apache.paimon.types.VarCharType;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Trino type from Paimon Type.
 */
public final class PaimonTypeUtils
{
    private PaimonTypeUtils() {}

    public static Type fromPaimonType(DataType type)
    {
        return type.accept(PaimonToTrinoTypeVistor.INSTANCE);
    }

    public static DataType toPaimonType(Type trinoType)
    {
        return TrinoToPaimonTypeVistor.INSTANCE.visit(trinoType);
    }

    private static class PaimonToTrinoTypeVistor
            extends DataTypeDefaultVisitor<Type>
    {
        private static final PaimonToTrinoTypeVistor INSTANCE = new PaimonToTrinoTypeVistor();

        @Override
        public Type visit(CharType charType)
        {
            return io.trino.spi.type.CharType.createCharType(
                    Math.min(io.trino.spi.type.CharType.MAX_LENGTH, charType.getLength()));
        }

        @Override
        public Type visit(VarCharType varCharType)
        {
            if (varCharType.getLength() == VarCharType.MAX_LENGTH) {
                return VarcharType.createUnboundedVarcharType();
            }
            return VarcharType.createVarcharType(
                    varCharType.getLength());
        }

        @Override
        public Type visit(BooleanType booleanType)
        {
            return io.trino.spi.type.BooleanType.BOOLEAN;
        }

        @Override
        public Type visit(BinaryType binaryType)
        {
            return VarbinaryType.VARBINARY;
        }

        @Override
        public Type visit(VarBinaryType varBinaryType)
        {
            return VarbinaryType.VARBINARY;
        }

        @Override
        public Type visit(DecimalType decimalType)
        {
            return io.trino.spi.type.DecimalType.createDecimalType(
                    decimalType.getPrecision(), decimalType.getScale());
        }

        @Override
        public Type visit(TinyIntType tinyIntType)
        {
            return TinyintType.TINYINT;
        }

        @Override
        public Type visit(SmallIntType smallIntType)
        {
            return SmallintType.SMALLINT;
        }

        @Override
        public Type visit(IntType intType)
        {
            return IntegerType.INTEGER;
        }

        @Override
        public Type visit(BigIntType bigIntType)
        {
            return BigintType.BIGINT;
        }

        @Override
        public Type visit(FloatType floatType)
        {
            return RealType.REAL;
        }

        @Override
        public Type visit(DoubleType doubleType)
        {
            return io.trino.spi.type.DoubleType.DOUBLE;
        }

        @Override
        public Type visit(DateType dateType)
        {
            return io.trino.spi.type.DateType.DATE;
        }

        @Override
        public Type visit(TimeType timeType)
        {
            return io.trino.spi.type.TimeType.TIME_MILLIS;
        }

        @Override
        public Type visit(TimestampType timestampType)
        {
            int precision = timestampType.getPrecision();
            if (precision <= 3) {
                return io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
            }
            else if (precision <= 6) {
                return io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
            }
            else if (precision <= 9) {
                return io.trino.spi.type.TimestampType.TIMESTAMP_NANOS;
            }
            else {
                return io.trino.spi.type.TimestampType.TIMESTAMP_PICOS;
            }
        }

        @Override
        public Type visit(LocalZonedTimestampType localZonedTimestampType)
        {
            int precision = localZonedTimestampType.getPrecision();
            if (precision <= 3) {
                return TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
            }
            else if (precision <= 6) {
                return TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
            }
            else if (precision <= 9) {
                return TimestampWithTimeZoneType.TIMESTAMP_TZ_NANOS;
            }
            else {
                return TimestampWithTimeZoneType.TIMESTAMP_TZ_PICOS;
            }
        }

        @Override
        public Type visit(ArrayType arrayType)
        {
            DataType elementType = arrayType.getElementType();
            return new io.trino.spi.type.ArrayType(elementType.accept(this));
        }

        @Override
        public Type visit(MultisetType multisetType)
        {
            return new MapType(multisetType.getElementType(), new IntType()).accept(this);
        }

        @Override
        public Type visit(MapType mapType)
        {
            return new io.trino.spi.type.MapType(
                    mapType.getKeyType().accept(this),
                    mapType.getValueType().accept(this),
                    new TypeOperators());
        }

        @Override
        public Type visit(RowType rowType)
        {
            List<io.trino.spi.type.RowType.Field> fields =
                    rowType.getFields().stream()
                            .map(
                                    field ->
                                            io.trino.spi.type.RowType.field(
                                                    field.name(), field.type().accept(this)))
                            .collect(Collectors.toList());
            return io.trino.spi.type.RowType.from(fields);
        }

        @Override
        protected Type defaultMethod(DataType logicalType)
        {
            throw new UnsupportedOperationException("Unsupported type: " + logicalType);
        }
    }

    private static class TrinoToPaimonTypeVistor
    {
        private static final TrinoToPaimonTypeVistor INSTANCE = new TrinoToPaimonTypeVistor();

        private final AtomicInteger currentIndex = new AtomicInteger(0);

        public DataType visit(Type trinoType)
        {
            switch (trinoType) {
                case io.trino.spi.type.CharType charType -> {
                    return DataTypes.CHAR(
                            Math.min(
                                    io.trino.spi.type.CharType.MAX_LENGTH,
                                    charType.getLength()));
                }
                case VarcharType varcharType -> {
                    Optional<Integer> length = varcharType.getLength();
                    if (length.isPresent()) {
                        return DataTypes.VARCHAR(
                                Math.min(
                                        VarcharType.MAX_LENGTH,
                                        ((VarcharType) trinoType).getBoundedLength()));
                    }
                    return DataTypes.VARCHAR(VarcharType.MAX_LENGTH);
                }
                case io.trino.spi.type.BooleanType _ -> {
                    return DataTypes.BOOLEAN();
                }
                case VarbinaryType _ -> {
                    return DataTypes.VARBINARY(Integer.MAX_VALUE);
                }
                case io.trino.spi.type.DecimalType decimalType -> {
                    return DataTypes.DECIMAL(
                            decimalType.getPrecision(),
                            decimalType.getScale());
                }
                case TinyintType _ -> {
                    return DataTypes.TINYINT();
                }
                case SmallintType _ -> {
                    return DataTypes.SMALLINT();
                }
                case IntegerType _ -> {
                    return DataTypes.INT();
                }
                case BigintType _ -> {
                    return DataTypes.BIGINT();
                }
                case RealType _ -> {
                    return DataTypes.FLOAT();
                }
                case io.trino.spi.type.DoubleType _ -> {
                    return DataTypes.DOUBLE();
                }
                case io.trino.spi.type.DateType _ -> {
                    return DataTypes.DATE();
                }
                case io.trino.spi.type.TimeType _ -> {
                    return new TimeType();
                }
                case io.trino.spi.type.TimestampType timestampType -> {
                    int precision = timestampType.getPrecision();
                    return new TimestampType(precision);
                }
                case TimestampWithTimeZoneType _ -> {
                    return DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE();
                }
                case io.trino.spi.type.ArrayType arrayType -> {
                    return DataTypes.ARRAY(
                            visit(arrayType.getElementType()));
                }
                case io.trino.spi.type.MapType mapType -> {
                    return DataTypes.MAP(
                            visit(mapType.getKeyType()),
                            visit(mapType.getValueType()));
                }
                case io.trino.spi.type.RowType rowType -> {
                    List<DataField> dataFields =
                            rowType.getFields().stream()
                                    .map(
                                            field ->
                                                    new DataField(
                                                            currentIndex.getAndIncrement(),
                                                            field.getName().get(),
                                                            visit(field.getType())))
                                    .collect(Collectors.toList());
                    return new RowType(true, dataFields);
                }
                case null, default -> throw new UnsupportedOperationException("Unsupported type: " + trinoType);
            }
        }
    }
}
