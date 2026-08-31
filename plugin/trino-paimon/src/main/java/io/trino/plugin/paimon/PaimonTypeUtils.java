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
import io.trino.spi.type.RowType.Field;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeDescriptor;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BinaryType;
import org.apache.paimon.types.BlobType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeChecks;
import org.apache.paimon.types.DataTypeDefaultVisitor;
import org.apache.paimon.types.DataTypeRoot;
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
import org.apache.paimon.types.VariantType;
import org.apache.paimon.types.VectorType;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static io.trino.spi.type.RowType.field;
import static java.util.Objects.requireNonNull;

public class PaimonTypeUtils
{
    // Paimon TimeType metadata allows 0..9, but internal rows and file formats store int milliseconds.
    private static final int PAIMON_TIME_MAX_STORED_PRECISION = 3;

    private PaimonTypeUtils() {}

    public static Type fromPaimonType(DataType type)
    {
        return requireNonNull(type, "type is null").accept(new PaimonToTrinoTypeVistor(Optional.empty()));
    }

    public static Type fromPaimonType(DataType type, TypeManager typeManager)
    {
        return requireNonNull(type, "type is null")
                .accept(new PaimonToTrinoTypeVistor(Optional.of(requireNonNull(typeManager, "typeManager is null"))));
    }

    public static DataType toPaimonType(Type trinoType)
    {
        return new TrinoToPaimonTypeVistor().visit(requireNonNull(trinoType, "trinoType is null"));
    }

    public static boolean containsVariant(DataType type)
    {
        return contains(type, dataType -> dataType.getTypeRoot() == DataTypeRoot.VARIANT);
    }

    public static boolean containsUnsupportedTrinoFormatProviderReadType(DataType type)
    {
        return containsUnsupportedTrinoFormatProviderType(type);
    }

    public static boolean containsUnsupportedTrinoFormatProviderWriteType(DataType type)
    {
        return containsUnsupportedTrinoFormatProviderType(type);
    }

    private static boolean containsUnsupportedTrinoFormatProviderType(DataType type)
    {
        return contains(type, dataType -> dataType.getTypeRoot() == DataTypeRoot.VARIANT
                || dataType instanceof BlobType
                || dataType instanceof MultisetType
                || dataType instanceof VectorType);
    }

    private static boolean contains(DataType type, Predicate<DataType> predicate)
    {
        requireNonNull(type, "type is null");
        requireNonNull(predicate, "predicate is null");
        if (predicate.test(type)) {
            return true;
        }
        return switch (type.getTypeRoot()) {
            case ARRAY, MAP, MULTISET, ROW, VECTOR -> DataTypeChecks.getNestedTypes(type).stream()
                    .anyMatch(nestedType -> contains(nestedType, predicate));
            default -> false;
        };
    }

    private static class PaimonToTrinoTypeVistor
            extends DataTypeDefaultVisitor<Type>
    {
        private final Optional<TypeManager> typeManager;

        private PaimonToTrinoTypeVistor(Optional<TypeManager> typeManager)
        {
            this.typeManager = requireNonNull(typeManager, "typeManager is null");
        }

        @Override
        public Type visit(CharType charType)
        {
            int length = charType.getLength();
            if (length > io.trino.spi.type.CharType.MAX_LENGTH) {
                throw new UnsupportedOperationException(
                        "Trino supports char length up to %s, got Paimon char(%s)"
                                .formatted(io.trino.spi.type.CharType.MAX_LENGTH, length));
            }
            return io.trino.spi.type.CharType.createCharType(length);
        }

        @Override
        public Type visit(VarCharType varCharType)
        {
            if (varCharType.getLength() == VarCharType.MAX_LENGTH) {
                return VarcharType.createUnboundedVarcharType();
            }
            return VarcharType.createVarcharType(Math.min(VarcharType.MAX_LENGTH, varCharType.getLength()));
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
        public Type visit(BlobType blobType)
        {
            return VarbinaryType.VARBINARY;
        }

        @Override
        public Type visit(VariantType variantType)
        {
            return typeManager
                    .map(manager -> manager.getType(new TypeDescriptor(StandardTypes.JSON)))
                    .orElseThrow(() -> new UnsupportedOperationException("Paimon VARIANT requires TypeManager for Trino JSON type"));
        }

        @Override
        public Type visit(DecimalType decimalType)
        {
            return io.trino.spi.type.DecimalType.createDecimalType(decimalType.getPrecision(), decimalType.getScale());
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
            return io.trino.spi.type.TimeType.createTimeType(
                    Math.min(timeType.getPrecision(), PAIMON_TIME_MAX_STORED_PRECISION));
        }

        @Override
        public Type visit(TimestampType timestampType)
        {
            return io.trino.spi.type.TimestampType.createTimestampType(timestampType.getPrecision());
        }

        @Override
        public Type visit(LocalZonedTimestampType localZonedTimestampType)
        {
            return TimestampWithTimeZoneType.createTimestampWithTimeZoneType(localZonedTimestampType.getPrecision());
        }

        @Override
        public Type visit(ArrayType arrayType)
        {
            DataType elementType = arrayType.getElementType();
            return new io.trino.spi.type.ArrayType(elementType.accept(this));
        }

        @Override
        public Type visit(VectorType vectorType)
        {
            return new io.trino.spi.type.ArrayType(vectorType.getElementType().accept(this));
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
            List<Field> fields = rowType.getFields().stream()
                    .map(dataField -> field(dataField.name(), dataField.type().accept(this)))
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
        private final AtomicInteger currentIndex = new AtomicInteger(0);

        public DataType visit(Type trinoType)
        {
            if (trinoType.getBaseName().equals(StandardTypes.JSON)) {
                return DataTypes.VARIANT();
            }
            if (trinoType instanceof io.trino.spi.type.CharType charType) {
                int length = charType.getLength();
                checkPaimonStringLength("char", trinoType, length, CharType.MIN_LENGTH, CharType.MAX_LENGTH);
                return DataTypes.CHAR(length);
            }
            else if (trinoType instanceof VarcharType varcharType) {
                Optional<Integer> length = varcharType.getLength();
                if (length.isPresent()) {
                    int boundedLength = varcharType.getBoundedLength();
                    checkPaimonStringLength("varchar", trinoType, boundedLength, VarCharType.MIN_LENGTH, VarCharType.MAX_LENGTH);
                    return DataTypes.VARCHAR(boundedLength);
                }
                return DataTypes.STRING();
            }
            else if (trinoType instanceof io.trino.spi.type.BooleanType) {
                return DataTypes.BOOLEAN();
            }
            else if (trinoType instanceof VarbinaryType) {
                return DataTypes.VARBINARY(Integer.MAX_VALUE);
            }
            else if (trinoType instanceof io.trino.spi.type.DecimalType decimalType) {
                return DataTypes.DECIMAL(decimalType.getPrecision(), decimalType.getScale());
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
            else if (trinoType instanceof io.trino.spi.type.TimeType timeType) {
                int precision = timeType.getPrecision();
                checkPaimonStoredTimePrecision(trinoType, precision);
                return new TimeType(precision);
            }
            else if (trinoType instanceof io.trino.spi.type.TimestampType timestampType) {
                int precision = timestampType.getPrecision();
                checkPaimonTemporalPrecision("timestamp", trinoType, precision, TimestampType.MAX_PRECISION);
                return new TimestampType(precision);
            }
            else if (trinoType instanceof TimestampWithTimeZoneType timestampWithTimeZoneType) {
                int precision = timestampWithTimeZoneType.getPrecision();
                checkPaimonTemporalPrecision("timestamp with time zone", trinoType, precision, LocalZonedTimestampType.MAX_PRECISION);
                return DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(precision);
            }
            else if (trinoType instanceof io.trino.spi.type.ArrayType arrayType) {
                return DataTypes.ARRAY(visit(arrayType.getElementType()));
            }
            else if (trinoType instanceof io.trino.spi.type.MapType mapType) {
                return DataTypes.MAP(visit(mapType.getKeyType()), visit(mapType.getValueType()));
            }
            else if (trinoType instanceof io.trino.spi.type.RowType rowType) {
                List<DataField> dataFields = new ArrayList<>();
                for (int fieldIndex = 0; fieldIndex < rowType.getFields().size(); fieldIndex++) {
                    Field field = rowType.getFields().get(fieldIndex);
                    dataFields.add(new DataField(
                            currentIndex.getAndIncrement(),
                            field.getName().orElse("f" + fieldIndex),
                            visit(field.getType())));
                }
                return new RowType(true, dataFields);
            }
            else {
                throw new UnsupportedOperationException("Unsupported type: " + trinoType);
            }
        }

        private static void checkPaimonTemporalPrecision(String typeName, Type trinoType, int precision, int maxPrecision)
        {
            if (precision > maxPrecision) {
                throw new UnsupportedOperationException(
                        "Paimon supports %s precision up to %s, got %s"
                                .formatted(typeName, maxPrecision, trinoType.getDisplayName()));
            }
        }

        private static void checkPaimonStoredTimePrecision(Type trinoType, int precision)
        {
            if (precision > PAIMON_TIME_MAX_STORED_PRECISION) {
                throw new UnsupportedOperationException(
                        "Paimon stores time values with millisecond precision, got %s"
                                .formatted(trinoType.getDisplayName()));
            }
        }

        private static void checkPaimonStringLength(String typeName, Type trinoType, int length, int minLength, int maxLength)
        {
            if (length < minLength || length > maxLength) {
                throw new UnsupportedOperationException(
                        "Paimon supports %s length between %s and %s, got %s"
                                .formatted(typeName, minLength, maxLength, trinoType.getDisplayName()));
            }
        }
    }
}
