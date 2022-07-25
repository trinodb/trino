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
package io.trino.sql.analyzer;

import com.google.common.collect.ImmutableList;
import io.trino.spi.TrinoException;
import io.trino.spi.type.NamedTypeSignature;
import io.trino.spi.type.RowFieldName;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;
import io.trino.spi.type.TypeSignatureParameter;
import io.trino.spi.type.VarcharType;
import io.trino.sql.ReservedIdentifiers;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.DataType;
import io.trino.sql.tree.DataTypeParameter;
import io.trino.sql.tree.DateTimeDataType;
import io.trino.sql.tree.GenericDataType;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.IntervalDayTimeDataType;
import io.trino.sql.tree.NumericParameter;
import io.trino.sql.tree.RowDataType;
import io.trino.sql.tree.TypeParameter;
import org.assertj.core.util.VisibleForTesting;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.TYPE_MISMATCH;
import static io.trino.spi.type.StandardTypes.INTERVAL_DAY_TO_SECOND;
import static io.trino.spi.type.StandardTypes.INTERVAL_YEAR_TO_MONTH;
import static io.trino.spi.type.TypeSignatureParameter.namedTypeParameter;
import static io.trino.spi.type.TypeSignatureParameter.numericParameter;
import static io.trino.spi.type.TypeSignatureParameter.typeParameter;
import static io.trino.spi.type.TypeSignatureParameter.typeVariable;
import static io.trino.spi.type.VarcharType.UNBOUNDED_LENGTH;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;
import static io.trino.type.IntervalDayTimeType.INTERVAL_DAY_TIME;
import static io.trino.type.IntervalYearMonthType.INTERVAL_YEAR_MONTH;
import static java.lang.String.format;

public class TypeSignatureTranslator
{
    private static final SqlParser SQL_PARSER = new SqlParser();

    private TypeSignatureTranslator() {}

    public static DataType toSqlType(Type type)
    {
        return toDataType(type.getTypeSignature());
    }

    public static TypeSignature toTypeSignature(DataType type)
    {
        return toTypeSignature(type, Set.of());
    }

    private static TypeSignature toTypeSignature(DataType type, Set<String> typeVariables)
    {
        if (type instanceof DateTimeDataType) {
            return toTypeSignature((DateTimeDataType) type, typeVariables);
        }
        if (type instanceof IntervalDayTimeDataType) {
            return toTypeSignature((IntervalDayTimeDataType) type);
        }
        if (type instanceof RowDataType) {
            return toTypeSignature((RowDataType) type, typeVariables);
        }
        if (type instanceof GenericDataType) {
            return toTypeSignature((GenericDataType) type, typeVariables);
        }

        throw new UnsupportedOperationException("Unsupported DataType: " + type.getClass().getName());
    }

    public static TypeSignature parseTypeSignature(String signature, Set<String> typeVariables)
    {
        Set<String> variables = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        variables.addAll(typeVariables);
        return toTypeSignature(SQL_PARSER.createType(signature), variables);
    }

    private static TypeSignature toTypeSignature(GenericDataType type, Set<String> typeVariables)
    {
        ImmutableList.Builder<TypeSignatureParameter> parameters = ImmutableList.builder();

        if (type.getName().getValue().equalsIgnoreCase(StandardTypes.VARCHAR) && type.getArguments().isEmpty()) {
            // We treat VARCHAR specially because currently, the unbounded VARCHAR type is modeled in the system as a VARCHAR(n) with a "magic" length
            // TODO: Eventually, we should split the types into VARCHAR and VARCHAR(n)
            return VarcharType.VARCHAR.getTypeSignature();
        }

        checkArgument(!typeVariables.contains(type.getName().getValue()), "Base type name cannot be a type variable");

        for (DataTypeParameter parameter : type.getArguments()) {
            if (parameter instanceof NumericParameter) {
                String value = ((NumericParameter) parameter).getValue();
                try {
                    parameters.add(numericParameter(Long.parseLong(value)));
                }
                catch (NumberFormatException e) {
                    throw semanticException(TYPE_MISMATCH, parameter, "Invalid type parameter: %s", value);
                }
            }
            else if (parameter instanceof TypeParameter) {
                DataType value = ((TypeParameter) parameter).getValue();
                if (value instanceof GenericDataType && ((GenericDataType) value).getArguments().isEmpty() && typeVariables.contains(((GenericDataType) value).getName().getValue())) {
                    parameters.add(typeVariable(((GenericDataType) value).getName().getValue()));
                }
                else {
                    parameters.add(typeParameter(toTypeSignature(value, typeVariables)));
                }
            }
            else {
                throw new UnsupportedOperationException("Unsupported type parameter kind: " + parameter.getClass().getName());
            }
        }

        return new TypeSignature(canonicalize(type.getName()), parameters.build());
    }

    private static TypeSignature toTypeSignature(RowDataType type, Set<String> typeVariables)
    {
        List<TypeSignatureParameter> parameters = type.getFields().stream()
                .map(field -> namedTypeParameter(new NamedTypeSignature(
                        field.getName()
                                .map(TypeSignatureTranslator::canonicalize)
                                .map(RowFieldName::new),
                        toTypeSignature(field.getType(), typeVariables))))
                .collect(toImmutableList());

        return new TypeSignature(StandardTypes.ROW, parameters);
    }

    private static TypeSignature toTypeSignature(IntervalDayTimeDataType type)
    {
        if (type.getFrom() == IntervalDayTimeDataType.Field.YEAR && type.getTo() == IntervalDayTimeDataType.Field.MONTH) {
            return INTERVAL_YEAR_MONTH.getTypeSignature();
        }

        if (type.getFrom() == IntervalDayTimeDataType.Field.DAY && type.getTo() == IntervalDayTimeDataType.Field.SECOND) {
            return INTERVAL_DAY_TIME.getTypeSignature();
        }

        throw new TrinoException(NOT_SUPPORTED, format("INTERVAL %s TO %s type not supported", type.getFrom(), type.getTo()));
    }

    private static TypeSignature toTypeSignature(DateTimeDataType type, Set<String> typeVariables)
    {
        boolean withTimeZone = type.isWithTimeZone();

        String base;
        switch (type.getType()) {
            case TIMESTAMP:
                if (withTimeZone) {
                    base = StandardTypes.TIMESTAMP_WITH_TIME_ZONE;
                }
                else {
                    base = StandardTypes.TIMESTAMP;
                }
                break;
            case TIME:
                if (withTimeZone) {
                    base = StandardTypes.TIME_WITH_TIME_ZONE;
                }
                else {
                    base = StandardTypes.TIME;
                }
                break;
            default:
                throw new UnsupportedOperationException("Unknown dateTime type: " + type.getType());
        }

        return new TypeSignature(base, translateParameters(type, typeVariables));
    }

    private static List<TypeSignatureParameter> translateParameters(DateTimeDataType type, Set<String> typeVariables)
    {
        List<TypeSignatureParameter> parameters = new ArrayList<>();

        if (type.getPrecision().isPresent()) {
            DataTypeParameter precision = type.getPrecision().get();
            if (precision instanceof NumericParameter) {
                parameters.add(TypeSignatureParameter.numericParameter(Long.parseLong(((NumericParameter) precision).getValue())));
            }
            else if (precision instanceof TypeParameter) {
                DataType typeVariable = ((TypeParameter) precision).getValue();
                checkArgument(typeVariable instanceof GenericDataType && ((GenericDataType) typeVariable).getArguments().isEmpty());
                String variable = ((GenericDataType) typeVariable).getName().getValue();
                checkArgument(typeVariables.contains(variable), "Parameter to datetime type must be either a number or a type variable: %s", variable);
                parameters.add(TypeSignatureParameter.typeVariable(variable));
            }
        }
        return parameters;
    }

    private static String canonicalize(Identifier identifier)
    {
        if (identifier.isDelimited()) {
            return identifier.getValue();
        }

        return identifier.getValue().toLowerCase(Locale.ENGLISH); // TODO: make this toUpperCase to match standard SQL semantics
    }

    @VisibleForTesting
    static DataType toDataType(TypeSignature typeSignature)
    {
        switch (typeSignature.getBase()) {
            case INTERVAL_YEAR_TO_MONTH:
                return new IntervalDayTimeDataType(Optional.empty(), IntervalDayTimeDataType.Field.YEAR, IntervalDayTimeDataType.Field.MONTH);
            case INTERVAL_DAY_TO_SECOND:
                return new IntervalDayTimeDataType(Optional.empty(), IntervalDayTimeDataType.Field.DAY, IntervalDayTimeDataType.Field.SECOND);
            case StandardTypes.TIMESTAMP_WITH_TIME_ZONE: {
                if (typeSignature.getParameters().isEmpty()) {
                    return new DateTimeDataType(Optional.empty(), DateTimeDataType.Type.TIMESTAMP, true, Optional.empty());
                }

                Optional<DataTypeParameter> argument = typeSignature.getParameters().stream()
                        .map(TypeSignatureTranslator::toTypeParameter)
                        .findAny();

                return new DateTimeDataType(Optional.empty(), DateTimeDataType.Type.TIMESTAMP, true, argument);
            }
            case StandardTypes.TIMESTAMP: {
                if (typeSignature.getParameters().isEmpty()) {
                    return new DateTimeDataType(Optional.empty(), DateTimeDataType.Type.TIMESTAMP, false, Optional.empty());
                }

                Optional<DataTypeParameter> argument = typeSignature.getParameters().stream()
                        .map(TypeSignatureTranslator::toTypeParameter)
                        .findAny();

                return new DateTimeDataType(Optional.empty(), DateTimeDataType.Type.TIMESTAMP, false, argument);
            }
            case StandardTypes.TIME_WITH_TIME_ZONE: {
                if (typeSignature.getParameters().isEmpty()) {
                    return new DateTimeDataType(Optional.empty(), DateTimeDataType.Type.TIME, true, Optional.empty());
                }

                Optional<DataTypeParameter> argument = typeSignature.getParameters().stream()
                        .map(TypeSignatureTranslator::toTypeParameter)
                        .findAny();

                return new DateTimeDataType(Optional.empty(), DateTimeDataType.Type.TIME, true, argument);
            }
            case StandardTypes.TIME: {
                if (typeSignature.getParameters().isEmpty()) {
                    return new DateTimeDataType(Optional.empty(), DateTimeDataType.Type.TIME, false, Optional.empty());
                }

                Optional<DataTypeParameter> argument = typeSignature.getParameters().stream()
                        .map(TypeSignatureTranslator::toTypeParameter)
                        .findAny();

                return new DateTimeDataType(Optional.empty(), DateTimeDataType.Type.TIME, false, argument);
            }
            case StandardTypes.ROW:
                return new RowDataType(
                        Optional.empty(),
                        typeSignature.getParameters().stream()
                                .map(parameter -> new RowDataType.Field(
                                        Optional.empty(),
                                        parameter.getNamedTypeSignature().getFieldName().map(fieldName -> new Identifier(fieldName.getName(), requiresDelimiting(fieldName.getName()))),
                                        toDataType(parameter.getNamedTypeSignature().getTypeSignature())))
                                .collect(toImmutableList()));
            case StandardTypes.VARCHAR:
                return new GenericDataType(
                        Optional.empty(),
                        new Identifier(typeSignature.getBase(), false),
                        typeSignature.getParameters().stream()
                                .filter(parameter -> parameter.getLongLiteral() != UNBOUNDED_LENGTH)
                                .map(parameter -> new NumericParameter(Optional.empty(), String.valueOf(parameter)))
                                .collect(toImmutableList()));
            default:
                return new GenericDataType(
                        Optional.empty(),
                        new Identifier(typeSignature.getBase(), false),
                        typeSignature.getParameters().stream()
                                .map(TypeSignatureTranslator::toTypeParameter)
                                .collect(toImmutableList()));
        }
    }

    private static boolean requiresDelimiting(String identifier)
    {
        if (!identifier.matches("[a-zA-Z][a-zA-Z0-9_]*")) {
            return true;
        }

        if (ReservedIdentifiers.reserved(identifier)) {
            return true;
        }

        return false;
    }

    private static DataTypeParameter toTypeParameter(TypeSignatureParameter parameter)
    {
        switch (parameter.getKind()) {
            case LONG:
                return new NumericParameter(Optional.empty(), String.valueOf(parameter.getLongLiteral()));
            case TYPE:
                return new TypeParameter(toDataType(parameter.getTypeSignature()));
            default:
                throw new UnsupportedOperationException("Unsupported parameter kind");
        }
    }
}
