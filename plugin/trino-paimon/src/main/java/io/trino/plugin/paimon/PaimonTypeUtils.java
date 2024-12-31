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
public class PaimonTypeUtils
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
                    Math.min(VarcharType.MAX_LENGTH, varCharType.getLength()));
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
            if (trinoType instanceof io.trino.spi.type.CharType) {
                return DataTypes.CHAR(
                        Math.min(
                                io.trino.spi.type.CharType.MAX_LENGTH,
                                ((io.trino.spi.type.CharType) trinoType).getLength()));
            }
            else if (trinoType instanceof VarcharType) {
                Optional<Integer> length = ((VarcharType) trinoType).getLength();
                if (length.isPresent()) {
                    return DataTypes.VARCHAR(
                            Math.min(
                                    VarcharType.MAX_LENGTH,
                                    ((VarcharType) trinoType).getBoundedLength()));
                }
                return DataTypes.VARCHAR(VarcharType.MAX_LENGTH);
            }
            else if (trinoType instanceof io.trino.spi.type.BooleanType) {
                return DataTypes.BOOLEAN();
            }
            else if (trinoType instanceof VarbinaryType) {
                return DataTypes.VARBINARY(Integer.MAX_VALUE);
            }
            else if (trinoType instanceof io.trino.spi.type.DecimalType) {
                return DataTypes.DECIMAL(
                        ((io.trino.spi.type.DecimalType) trinoType).getPrecision(),
                        ((io.trino.spi.type.DecimalType) trinoType).getScale());
            }
            else if (trinoType instanceof TinyintType) {
                return DataTypes.TINYINT();
            }
            else if (trinoType instanceof SmallintType) {
                return DataTypes.SMALLINT();
            }
            else if (trinoType instanceof IntegerType) {
                return DataTypes.INT();
            }
            else if (trinoType instanceof BigintType) {
                return DataTypes.BIGINT();
            }
            else if (trinoType instanceof RealType) {
                return DataTypes.FLOAT();
            }
            else if (trinoType instanceof io.trino.spi.type.DoubleType) {
                return DataTypes.DOUBLE();
            }
            else if (trinoType instanceof io.trino.spi.type.DateType) {
                return DataTypes.DATE();
            }
            else if (trinoType instanceof io.trino.spi.type.TimeType) {
                return new TimeType();
            }
            else if (trinoType instanceof io.trino.spi.type.TimestampType) {
                int precision = ((io.trino.spi.type.TimestampType) trinoType).getPrecision();
                return new TimestampType(precision);
            }
            else if (trinoType instanceof TimestampWithTimeZoneType) {
                return DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE();
            }
            else if (trinoType instanceof io.trino.spi.type.ArrayType) {
                return DataTypes.ARRAY(
                        visit(((io.trino.spi.type.ArrayType) trinoType).getElementType()));
            }
            else if (trinoType instanceof io.trino.spi.type.MapType) {
                return DataTypes.MAP(
                        visit(((io.trino.spi.type.MapType) trinoType).getKeyType()),
                        visit(((io.trino.spi.type.MapType) trinoType).getValueType()));
            }
            else if (trinoType instanceof io.trino.spi.type.RowType rowType) {
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
            else {
                throw new UnsupportedOperationException("Unsupported type: " + trinoType);
            }
        }
    }
}
