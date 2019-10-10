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
package io.prestosql.type;

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.CharType;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.MapType;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.spi.type.TypeSignatureParameter;
import io.prestosql.spi.type.VarcharType;
import io.prestosql.type.setdigest.SetDigestType;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.CharType.createCharType;
import static io.prestosql.spi.type.DecimalType.createDecimalType;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.HyperLogLogType.HYPER_LOG_LOG;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.RowType.Field;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static io.prestosql.spi.type.VarcharType.createVarcharType;
import static io.prestosql.type.CodePointsType.CODE_POINTS;
import static io.prestosql.type.JoniRegexpType.JONI_REGEXP;
import static io.prestosql.type.JsonPathType.JSON_PATH;
import static io.prestosql.type.LikePatternType.LIKE_PATTERN;
import static io.prestosql.type.Re2JRegexpType.RE2J_REGEXP;
import static java.util.Objects.requireNonNull;

public final class TypeCoercion
{
    private final Function<TypeSignature, Type> lookupType;

    public TypeCoercion(Function<TypeSignature, Type> lookupType)
    {
        this.lookupType = requireNonNull(lookupType, "lookupType is null");
    }

    public boolean isTypeOnlyCoercion(Type source, Type result)
    {
        if (source.equals(result)) {
            return true;
        }

        if (!canCoerce(source, result)) {
            return false;
        }

        if (source instanceof VarcharType && result instanceof VarcharType) {
            return true;
        }

        if (source instanceof DecimalType && result instanceof DecimalType) {
            DecimalType sourceDecimal = (DecimalType) source;
            DecimalType resultDecimal = (DecimalType) result;
            boolean sameDecimalSubtype = (sourceDecimal.isShort() && resultDecimal.isShort())
                    || (!sourceDecimal.isShort() && !resultDecimal.isShort());
            boolean sameScale = sourceDecimal.getScale() == resultDecimal.getScale();
            boolean sourcePrecisionIsLessOrEqualToResultPrecision = sourceDecimal.getPrecision() <= resultDecimal.getPrecision();
            return sameDecimalSubtype && sameScale && sourcePrecisionIsLessOrEqualToResultPrecision;
        }

        String sourceTypeBase = source.getBaseName();
        String resultTypeBase = result.getBaseName();

        if (sourceTypeBase.equals(resultTypeBase) && isCovariantParametrizedType(source)) {
            List<Type> sourceTypeParameters = source.getTypeParameters();
            List<Type> resultTypeParameters = result.getTypeParameters();
            checkState(sourceTypeParameters.size() == resultTypeParameters.size());
            for (int i = 0; i < sourceTypeParameters.size(); i++) {
                if (!isTypeOnlyCoercion(sourceTypeParameters.get(i), resultTypeParameters.get(i))) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    public Optional<Type> getCommonSuperType(Type firstType, Type secondType)
    {
        TypeCompatibility compatibility = compatibility(firstType, secondType);
        if (!compatibility.isCompatible()) {
            return Optional.empty();
        }
        return Optional.of(compatibility.getCommonSuperType());
    }

    public boolean canCoerce(Type fromType, Type toType)
    {
        TypeCompatibility typeCompatibility = compatibility(fromType, toType);
        return typeCompatibility.isCoercible();
    }

    private TypeCompatibility compatibility(Type fromType, Type toType)
    {
        if (fromType.equals(toType)) {
            return TypeCompatibility.compatible(toType, true);
        }

        if (fromType.equals(UnknownType.UNKNOWN)) {
            return TypeCompatibility.compatible(toType, true);
        }

        if (toType.equals(UnknownType.UNKNOWN)) {
            return TypeCompatibility.compatible(fromType, false);
        }

        String fromTypeBaseName = fromType.getBaseName();
        String toTypeBaseName = toType.getBaseName();
        if (fromTypeBaseName.equals(toTypeBaseName)) {
            if (fromTypeBaseName.equals(StandardTypes.DECIMAL)) {
                Type commonSuperType = getCommonSuperTypeForDecimal((DecimalType) fromType, (DecimalType) toType);
                return TypeCompatibility.compatible(commonSuperType, commonSuperType.equals(toType));
            }
            if (fromTypeBaseName.equals(StandardTypes.VARCHAR)) {
                Type commonSuperType = getCommonSuperTypeForVarchar((VarcharType) fromType, (VarcharType) toType);
                return TypeCompatibility.compatible(commonSuperType, commonSuperType.equals(toType));
            }
            if (fromTypeBaseName.equals(StandardTypes.CHAR)) {
                Type commonSuperType = getCommonSuperTypeForChar((CharType) fromType, (CharType) toType);
                return TypeCompatibility.compatible(commonSuperType, commonSuperType.equals(toType));
            }
            if (fromTypeBaseName.equals(StandardTypes.ROW)) {
                return typeCompatibilityForRow((RowType) fromType, (RowType) toType);
            }

            if (isCovariantParametrizedType(fromType)) {
                return typeCompatibilityForCovariantParametrizedType(fromType, toType);
            }
            return TypeCompatibility.incompatible();
        }

        Optional<Type> coercedType = coerceTypeBase(fromType, toType.getBaseName());
        if (coercedType.isPresent()) {
            return compatibility(coercedType.get(), toType);
        }

        coercedType = coerceTypeBase(toType, fromType.getBaseName());
        if (coercedType.isPresent()) {
            TypeCompatibility typeCompatibility = compatibility(fromType, coercedType.get());
            if (!typeCompatibility.isCompatible()) {
                return TypeCompatibility.incompatible();
            }
            return TypeCompatibility.compatible(typeCompatibility.getCommonSuperType(), false);
        }

        return TypeCompatibility.incompatible();
    }

    private static Type getCommonSuperTypeForDecimal(DecimalType firstType, DecimalType secondType)
    {
        int targetScale = Math.max(firstType.getScale(), secondType.getScale());
        int targetPrecision = Math.max(firstType.getPrecision() - firstType.getScale(), secondType.getPrecision() - secondType.getScale()) + targetScale;
        //we allow potential loss of precision here. Overflow checking is done in operators.
        targetPrecision = Math.min(38, targetPrecision);
        return createDecimalType(targetPrecision, targetScale);
    }

    private static Type getCommonSuperTypeForVarchar(VarcharType firstType, VarcharType secondType)
    {
        if (firstType.isUnbounded() || secondType.isUnbounded()) {
            return createUnboundedVarcharType();
        }

        return createVarcharType(Math.max(firstType.getBoundedLength(), secondType.getBoundedLength()));
    }

    private static Type getCommonSuperTypeForChar(CharType firstType, CharType secondType)
    {
        return createCharType(Math.max(firstType.getLength(), secondType.getLength()));
    }

    private TypeCompatibility typeCompatibilityForRow(RowType firstType, RowType secondType)
    {
        List<Field> firstFields = firstType.getFields();
        List<Field> secondFields = secondType.getFields();
        if (firstFields.size() != secondFields.size()) {
            return TypeCompatibility.incompatible();
        }

        ImmutableList.Builder<Field> fields = ImmutableList.builder();
        boolean coercible = true;
        for (int i = 0; i < firstFields.size(); i++) {
            Type firstFieldType = firstFields.get(i).getType();
            Type secondFieldType = secondFields.get(i).getType();
            TypeCompatibility typeCompatibility = compatibility(firstFieldType, secondFieldType);
            if (!typeCompatibility.isCompatible()) {
                return TypeCompatibility.incompatible();
            }
            Type commonParameterType = typeCompatibility.getCommonSuperType();

            Optional<String> firstParameterName = firstFields.get(i).getName();
            Optional<String> secondParameterName = secondFields.get(i).getName();
            Optional<String> commonName = firstParameterName.equals(secondParameterName) ? firstParameterName : Optional.empty();

            // ignore parameter name for coercible
            coercible &= typeCompatibility.isCoercible();
            fields.add(new Field(commonName, commonParameterType));
        }

        return TypeCompatibility.compatible(RowType.from(fields.build()), coercible);
    }

    private TypeCompatibility typeCompatibilityForCovariantParametrizedType(Type fromType, Type toType)
    {
        checkState(fromType.getClass().equals(toType.getClass()));
        ImmutableList.Builder<TypeSignatureParameter> commonParameterTypes = ImmutableList.builder();
        List<Type> fromTypeParameters = fromType.getTypeParameters();
        List<Type> toTypeParameters = toType.getTypeParameters();

        if (fromTypeParameters.size() != toTypeParameters.size()) {
            return TypeCompatibility.incompatible();
        }

        boolean coercible = true;
        for (int i = 0; i < fromTypeParameters.size(); i++) {
            TypeCompatibility compatibility = compatibility(fromTypeParameters.get(i), toTypeParameters.get(i));
            if (!compatibility.isCompatible()) {
                return TypeCompatibility.incompatible();
            }
            coercible &= compatibility.isCoercible();
            commonParameterTypes.add(TypeSignatureParameter.of(compatibility.getCommonSuperType().getTypeSignature()));
        }
        String typeBase = fromType.getBaseName();
        return TypeCompatibility.compatible(lookupType.apply(new TypeSignature(typeBase, commonParameterTypes.build())), coercible);
    }

    /**
     * coerceTypeBase and isCovariantParametrizedType defines all hand-coded rules for type coercion.
     * Other methods should reference these two functions instead of hand-code new rules.
     */
    @SuppressWarnings("SwitchStatementWithTooFewBranches")
    public Optional<Type> coerceTypeBase(Type sourceType, String resultTypeBase)
    {
        String sourceTypeName = sourceType.getBaseName();
        if (sourceTypeName.equals(resultTypeBase)) {
            return Optional.of(sourceType);
        }

        switch (sourceTypeName) {
            case UnknownType.NAME: {
                switch (resultTypeBase) {
                    case StandardTypes.BOOLEAN:
                    case StandardTypes.BIGINT:
                    case StandardTypes.INTEGER:
                    case StandardTypes.DOUBLE:
                    case StandardTypes.REAL:
                    case StandardTypes.VARBINARY:
                    case StandardTypes.DATE:
                    case StandardTypes.TIME:
                    case StandardTypes.TIME_WITH_TIME_ZONE:
                    case StandardTypes.TIMESTAMP:
                    case StandardTypes.TIMESTAMP_WITH_TIME_ZONE:
                    case StandardTypes.HYPER_LOG_LOG:
                    case SetDigestType.NAME:
                    case StandardTypes.P4_HYPER_LOG_LOG:
                    case StandardTypes.JSON:
                    case StandardTypes.INTERVAL_YEAR_TO_MONTH:
                    case StandardTypes.INTERVAL_DAY_TO_SECOND:
                    case JoniRegexpType.NAME:
                    case LikePatternType.NAME:
                    case JsonPathType.NAME:
                    case ColorType.NAME:
                    case CodePointsType.NAME:
                        return Optional.of(lookupType.apply(new TypeSignature(resultTypeBase)));
                    case StandardTypes.VARCHAR:
                        return Optional.of(createVarcharType(0));
                    case StandardTypes.CHAR:
                        return Optional.of(createCharType(0));
                    case StandardTypes.DECIMAL:
                        return Optional.of(createDecimalType(1, 0));
                    default:
                        return Optional.empty();
                }
            }
            case StandardTypes.TINYINT: {
                switch (resultTypeBase) {
                    case StandardTypes.SMALLINT:
                        return Optional.of(SMALLINT);
                    case StandardTypes.INTEGER:
                        return Optional.of(INTEGER);
                    case StandardTypes.BIGINT:
                        return Optional.of(BIGINT);
                    case StandardTypes.REAL:
                        return Optional.of(REAL);
                    case StandardTypes.DOUBLE:
                        return Optional.of(DOUBLE);
                    case StandardTypes.DECIMAL:
                        return Optional.of(createDecimalType(3, 0));
                    default:
                        return Optional.empty();
                }
            }
            case StandardTypes.SMALLINT: {
                switch (resultTypeBase) {
                    case StandardTypes.INTEGER:
                        return Optional.of(INTEGER);
                    case StandardTypes.BIGINT:
                        return Optional.of(BIGINT);
                    case StandardTypes.REAL:
                        return Optional.of(REAL);
                    case StandardTypes.DOUBLE:
                        return Optional.of(DOUBLE);
                    case StandardTypes.DECIMAL:
                        return Optional.of(createDecimalType(5, 0));
                    default:
                        return Optional.empty();
                }
            }
            case StandardTypes.INTEGER: {
                switch (resultTypeBase) {
                    case StandardTypes.BIGINT:
                        return Optional.of(BIGINT);
                    case StandardTypes.REAL:
                        return Optional.of(REAL);
                    case StandardTypes.DOUBLE:
                        return Optional.of(DOUBLE);
                    case StandardTypes.DECIMAL:
                        return Optional.of(createDecimalType(10, 0));
                    default:
                        return Optional.empty();
                }
            }
            case StandardTypes.BIGINT: {
                switch (resultTypeBase) {
                    case StandardTypes.REAL:
                        return Optional.of(REAL);
                    case StandardTypes.DOUBLE:
                        return Optional.of(DOUBLE);
                    case StandardTypes.DECIMAL:
                        return Optional.of(createDecimalType(19, 0));
                    default:
                        return Optional.empty();
                }
            }
            case StandardTypes.DECIMAL: {
                switch (resultTypeBase) {
                    case StandardTypes.REAL:
                        return Optional.of(REAL);
                    case StandardTypes.DOUBLE:
                        return Optional.of(DOUBLE);
                    default:
                        return Optional.empty();
                }
            }
            case StandardTypes.REAL: {
                switch (resultTypeBase) {
                    case StandardTypes.DOUBLE:
                        return Optional.of(DOUBLE);
                    default:
                        return Optional.empty();
                }
            }
            case StandardTypes.DATE: {
                switch (resultTypeBase) {
                    case StandardTypes.TIMESTAMP:
                        return Optional.of(TIMESTAMP);
                    case StandardTypes.TIMESTAMP_WITH_TIME_ZONE:
                        return Optional.of(TIMESTAMP_WITH_TIME_ZONE);
                    default:
                        return Optional.empty();
                }
            }
            case StandardTypes.TIME: {
                switch (resultTypeBase) {
                    case StandardTypes.TIME_WITH_TIME_ZONE:
                        return Optional.of(TIME_WITH_TIME_ZONE);
                    default:
                        return Optional.empty();
                }
            }
            case StandardTypes.TIMESTAMP: {
                switch (resultTypeBase) {
                    case StandardTypes.TIMESTAMP_WITH_TIME_ZONE:
                        return Optional.of(TIMESTAMP_WITH_TIME_ZONE);
                    default:
                        return Optional.empty();
                }
            }
            case StandardTypes.VARCHAR: {
                switch (resultTypeBase) {
                    case StandardTypes.CHAR:
                        VarcharType varcharType = (VarcharType) sourceType;
                        if (varcharType.isUnbounded()) {
                            return Optional.of(createCharType(CharType.MAX_LENGTH));
                        }

                        return Optional.of(createCharType(Math.min(CharType.MAX_LENGTH, varcharType.getBoundedLength())));
                    case JoniRegexpType.NAME:
                        return Optional.of(JONI_REGEXP);
                    case Re2JRegexpType.NAME:
                        return Optional.of(RE2J_REGEXP);
                    case LikePatternType.NAME:
                        return Optional.of(LIKE_PATTERN);
                    case JsonPathType.NAME:
                        return Optional.of(JSON_PATH);
                    case CodePointsType.NAME:
                        return Optional.of(CODE_POINTS);
                    default:
                        return Optional.empty();
                }
            }
            case StandardTypes.CHAR: {
                switch (resultTypeBase) {
                    case StandardTypes.VARCHAR:
                        return Optional.empty();
                    case JoniRegexpType.NAME:
                        return Optional.of(JONI_REGEXP);
                    case Re2JRegexpType.NAME:
                        return Optional.of(RE2J_REGEXP);
                    case LikePatternType.NAME:
                        return Optional.of(LIKE_PATTERN);
                    case JsonPathType.NAME:
                        return Optional.of(JSON_PATH);
                    case CodePointsType.NAME:
                        return Optional.of(CODE_POINTS);
                    default:
                        return Optional.empty();
                }
            }
            case StandardTypes.P4_HYPER_LOG_LOG: {
                switch (resultTypeBase) {
                    case StandardTypes.HYPER_LOG_LOG:
                        return Optional.of(HYPER_LOG_LOG);
                    default:
                        return Optional.empty();
                }
            }
            default:
                return Optional.empty();
        }
    }

    public static boolean isCovariantTypeBase(String typeBase)
    {
        return typeBase.equals(StandardTypes.ARRAY) || typeBase.equals(StandardTypes.MAP);
    }

    private static boolean isCovariantParametrizedType(Type type)
    {
        // if we ever introduce contravariant, this function should be changed to return an enumeration: INVARIANT, COVARIANT, CONTRAVARIANT
        return type instanceof MapType || type instanceof ArrayType;
    }

    public static class TypeCompatibility
    {
        private final Optional<Type> commonSuperType;
        private final boolean coercible;

        // Do not call constructor directly. Use factory methods.
        private TypeCompatibility(Optional<Type> commonSuperType, boolean coercible)
        {
            // Assert that: coercible => commonSuperType.isPresent
            // The factory API is designed such that this is guaranteed.
            checkArgument(!coercible || commonSuperType.isPresent());

            this.commonSuperType = commonSuperType;
            this.coercible = coercible;
        }

        private static TypeCompatibility compatible(Type commonSuperType, boolean coercible)
        {
            return new TypeCompatibility(Optional.of(commonSuperType), coercible);
        }

        private static TypeCompatibility incompatible()
        {
            return new TypeCompatibility(Optional.empty(), false);
        }

        public boolean isCompatible()
        {
            return commonSuperType.isPresent();
        }

        public Type getCommonSuperType()
        {
            checkState(commonSuperType.isPresent(), "Types are not compatible");
            return commonSuperType.get();
        }

        public boolean isCoercible()
        {
            return coercible;
        }
    }
}
