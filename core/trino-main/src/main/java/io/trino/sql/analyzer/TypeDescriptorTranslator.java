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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.CharMatcher;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheLoader;
import io.trino.cache.EvictableCacheBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.type.NumericExpression;
import io.trino.spi.type.TemplateParameter;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeDescriptor;
import io.trino.spi.type.TypeParameter;
import io.trino.spi.type.TypeTemplate;
import io.trino.spi.type.TypeTemplates;
import io.trino.spi.type.VarcharType;
import io.trino.sql.ReservedIdentifiers;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.CompositeIntervalQualifier;
import io.trino.sql.tree.DataType;
import io.trino.sql.tree.DataTypeParameter;
import io.trino.sql.tree.DateTimeDataType;
import io.trino.sql.tree.GenericDataType;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.IntervalDataType;
import io.trino.sql.tree.IntervalField;
import io.trino.sql.tree.NodeLocation;
import io.trino.sql.tree.NumericParameter;
import io.trino.sql.tree.RowDataType;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.TreeSet;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.StandardTypes.INTERVAL_DAY_TO_SECOND;
import static io.trino.spi.type.StandardTypes.INTERVAL_YEAR_TO_MONTH;
import static io.trino.spi.type.StandardTypes.ROW;
import static io.trino.spi.type.StandardTypes.TIME;
import static io.trino.spi.type.StandardTypes.TIMESTAMP;
import static io.trino.spi.type.StandardTypes.TIMESTAMP_WITH_TIME_ZONE;
import static io.trino.spi.type.StandardTypes.TIME_WITH_TIME_ZONE;
import static io.trino.spi.type.StandardTypes.VARCHAR;
import static io.trino.spi.type.VarcharType.UNBOUNDED_LENGTH;
import static io.trino.type.IntervalDayTimeType.INTERVAL_DAY_TIME;
import static io.trino.type.IntervalYearMonthType.INTERVAL_YEAR_MONTH;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;

public final class TypeDescriptorTranslator
{
    private static final SqlParser SQL_PARSER = new SqlParser();

    private static final Cache<String, DataType> DATA_TYPE_CACHE = EvictableCacheBuilder.newBuilder()
            .maximumSize(4096)
            .build(new CacheLoader<>()
            {
                @Override
                public DataType load(String signature)
                {
                    return parseDataType(signature);
                }
            });

    private static final CharMatcher IS_DIGIT = CharMatcher.inRange('0', '9')
            .precomputed();

    private static final CharMatcher IS_VALID_IDENTIFIER_CHAR = CharMatcher.inRange('a', 'z')
            .or(CharMatcher.inRange('A', 'Z'))
            .or(CharMatcher.is('_'))
            .or(CharMatcher.inRange('0', '9'))
            .precomputed();

    private TypeDescriptorTranslator() {}

    public static DataType toSqlType(Type type)
    {
        return toDataType(type.getTypeDescriptor());
    }

    public static TypeDescriptor toTypeDescriptor(DataType type)
    {
        // A ground type is the degenerate case of a template: parse it with no variables in scope
        // and lower the (variable-free) result, so both forms share one structural translation.
        return TypeTemplates.toTypeDescriptor(toTypeTemplate(type, Set.of(), Set.of()));
    }

    /// Parses a ground type descriptor — one that refers to no signature variables. Function signature
    /// argument and return types, which may reference a function's type or numeric variables, are parsed
    /// with [#parseTypeTemplate] instead.
    public static TypeDescriptor parseTypeDescriptor(String signature)
    {
        try {
            return toTypeDescriptor(DATA_TYPE_CACHE.get(signature.toLowerCase(ENGLISH), () -> parseDataType(signature)));
        }
        catch (Exception e) {
            if (e.getCause() != null) {
                throwIfUnchecked(e.getCause());
            }
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }

    /// Parses a type template — the open, variable-bearing form used in function signature argument and
    /// return positions. Unlike [#parseTypeDescriptor], a variable's kind is structural in the result,
    /// so the declared type-variable and numeric-variable names are passed separately (the syntax alone
    /// cannot tell `E` in `array(E)` from a numeric `p` in `decimal(p, s)`).
    public static TypeTemplate parseTypeTemplate(String signature, Set<String> typeVariables, Set<String> numericVariables)
    {
        Set<String> types = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        types.addAll(typeVariables);
        Set<String> numerics = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        numerics.addAll(numericVariables);
        Set<String> overlap = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        overlap.addAll(types);
        overlap.retainAll(numerics);
        checkArgument(overlap.isEmpty(), "A name cannot be both a type variable and a numeric variable: %s", overlap);
        try {
            return toTypeTemplate(DATA_TYPE_CACHE.get(signature.toLowerCase(ENGLISH), () -> parseDataType(signature)), types, numerics);
        }
        catch (Exception e) {
            if (e.getCause() != null) {
                throwIfUnchecked(e.getCause());
            }
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }

    private static TypeTemplate toTypeTemplate(DataType type, Set<String> typeVariables, Set<String> numericVariables)
    {
        return switch (type) {
            case GenericDataType genericDataType -> toTypeTemplate(genericDataType, typeVariables, numericVariables);
            case RowDataType rowDataType -> toTypeTemplate(rowDataType, typeVariables, numericVariables);
            case DateTimeDataType dateTimeDataType -> toTypeTemplate(dateTimeDataType, numericVariables);
            // Interval types are always ground; lift the ground signature into a (variable-free) template.
            case IntervalDataType intervalDataType -> TypeTemplates.fromTypeDescriptor(toTypeDescriptor(intervalDataType));
        };
    }

    private static TypeTemplate toTypeTemplate(GenericDataType type, Set<String> typeVariables, Set<String> numericVariables)
    {
        String name = type.getName().getValue();

        // A bare reference to a declared type variable, e.g. a top-level E or the E in array(E).
        if (type.getArguments().isEmpty() && typeVariables.contains(name)) {
            return new TypeTemplate.TypeVariable(name);
        }

        // A numeric variable belongs in a parameter position (the n in varchar(n)), never as a type itself.
        checkArgument(!(type.getArguments().isEmpty() && numericVariables.contains(name)),
                "Numeric variable cannot appear in a type position: %s",
                name);

        if (name.equalsIgnoreCase(VARCHAR) && type.getArguments().isEmpty()) {
            // Unbounded VARCHAR is modeled as VARCHAR(n) with a magic length, matching toTypeDescriptor.
            return TypeTemplates.fromTypeDescriptor(VarcharType.VARCHAR.getTypeDescriptor());
        }

        List<TemplateParameter> parameters = new ArrayList<>();
        for (DataTypeParameter parameter : type.getArguments()) {
            parameters.add(toTemplateParameter(parameter, typeVariables, numericVariables));
        }
        return new TypeTemplate.TypeApplication(canonicalize(type.getName()), parameters);
    }

    private static TemplateParameter toTemplateParameter(DataTypeParameter parameter, Set<String> typeVariables, Set<String> numericVariables)
    {
        return switch (parameter) {
            case NumericParameter numericParameter -> new TemplateParameter.NumericArgument(new NumericExpression.Literal(numericParameter.getParsedValue()));
            case io.trino.sql.tree.TypeParameter typeParameter -> {
                DataType value = typeParameter.getValue();
                if (value instanceof GenericDataType genericDataType && genericDataType.getArguments().isEmpty()) {
                    String variable = genericDataType.getName().getValue();
                    if (numericVariables.contains(variable)) {
                        yield new TemplateParameter.NumericArgument(new NumericExpression.Variable(variable));
                    }
                    if (typeVariables.contains(variable)) {
                        yield new TemplateParameter.TypeArgument(Optional.empty(), new TypeTemplate.TypeVariable(variable));
                    }
                }
                yield new TemplateParameter.TypeArgument(Optional.empty(), toTypeTemplate(value, typeVariables, numericVariables));
            }
            default -> throw new UnsupportedOperationException("Unsupported type parameter kind: " + parameter.getClass().getName());
        };
    }

    private static TypeTemplate toTypeTemplate(RowDataType type, Set<String> typeVariables, Set<String> numericVariables)
    {
        List<TemplateParameter> parameters = type.getFields().stream()
                .map(field -> (TemplateParameter) new TemplateParameter.TypeArgument(
                        field.getName().map(TypeDescriptorTranslator::canonicalize),
                        toTypeTemplate(field.getType(), typeVariables, numericVariables)))
                .collect(toImmutableList());
        return new TypeTemplate.TypeApplication(ROW, parameters);
    }

    private static TypeTemplate toTypeTemplate(DateTimeDataType type, Set<String> numericVariables)
    {
        boolean withTimeZone = type.isWithTimeZone();
        String base = switch (type.getType()) {
            case TIMESTAMP -> withTimeZone ? TIMESTAMP_WITH_TIME_ZONE : TIMESTAMP;
            case TIME -> withTimeZone ? TIME_WITH_TIME_ZONE : TIME;
        };

        List<TemplateParameter> parameters = new ArrayList<>();
        if (type.getPrecision().isPresent()) {
            DataTypeParameter precision = type.getPrecision().get();
            if (precision instanceof NumericParameter numericParameter) {
                parameters.add(new TemplateParameter.NumericArgument(new NumericExpression.Literal(numericParameter.getParsedValue())));
            }
            else if (precision instanceof io.trino.sql.tree.TypeParameter typeParameter) {
                DataType value = typeParameter.getValue();
                if (!(value instanceof GenericDataType genericDataType) || !genericDataType.getArguments().isEmpty()) {
                    throw new IllegalArgumentException("Parameter to datetime type must be either a number or a variable");
                }
                String variable = genericDataType.getName().getValue();
                checkArgument(numericVariables.contains(variable), "Parameter to datetime type must be a number or a numeric variable: %s", variable);
                parameters.add(new TemplateParameter.NumericArgument(new NumericExpression.Variable(variable)));
            }
        }
        return new TypeTemplate.TypeApplication(base, parameters);
    }

    private static TypeDescriptor toTypeDescriptor(IntervalDataType type)
    {
        if (type.qualifier() instanceof CompositeIntervalQualifier qualifier &&
                qualifier.getFrom() instanceof IntervalField.Year() &&
                qualifier.getTo() instanceof IntervalField.Month &&
                qualifier.getPrecision().isEmpty()) {
            return INTERVAL_YEAR_MONTH.getTypeDescriptor();
        }

        if (type.qualifier() instanceof CompositeIntervalQualifier qualifier &&
                qualifier.getFrom() instanceof IntervalField.Day() &&
                qualifier.getTo() instanceof IntervalField.Second(OptionalInt fractionalPrecision) &&
                qualifier.getPrecision().isEmpty() &&
                fractionalPrecision.isEmpty()) {
            return INTERVAL_DAY_TIME.getTypeDescriptor();
        }

        throw new TrinoException(NOT_SUPPORTED, format("INTERVAL %s type not supported", type.qualifier()));
    }

    private static String canonicalize(Identifier identifier)
    {
        if (identifier.isDelimited()) {
            return identifier.getValue();
        }

        return identifier.getValue().toLowerCase(ENGLISH); // TODO: make this toUpperCase to match standard SQL semantics
    }

    @VisibleForTesting
    static DataType toDataType(TypeDescriptor typeDescriptor)
    {
        return switch (typeDescriptor.getBase()) {
            case INTERVAL_YEAR_TO_MONTH -> new IntervalDataType(
                    Optional.empty(),
                    new CompositeIntervalQualifier(
                            new NodeLocation(1, 1),
                            OptionalInt.empty(),
                            new IntervalField.Year(),
                            new IntervalField.Month()));
            case INTERVAL_DAY_TO_SECOND -> new IntervalDataType(
                    Optional.empty(),
                    new CompositeIntervalQualifier(
                            new NodeLocation(1, 1),
                            OptionalInt.empty(),
                            new IntervalField.Day(),
                            new IntervalField.Second(OptionalInt.empty())));
            case TIMESTAMP_WITH_TIME_ZONE -> new DateTimeDataType(
                    Optional.empty(),
                    DateTimeDataType.Type.TIMESTAMP,
                    true,
                    typeDescriptor.getParameters().stream()
                            .findAny()
                            .map(TypeDescriptorTranslator::toTypeParameter));
            case TIMESTAMP -> new DateTimeDataType(
                    Optional.empty(),
                    DateTimeDataType.Type.TIMESTAMP,
                    false,
                    typeDescriptor.getParameters().stream()
                            .findAny()
                            .map(TypeDescriptorTranslator::toTypeParameter));
            case TIME_WITH_TIME_ZONE -> new DateTimeDataType(
                    Optional.empty(),
                    DateTimeDataType.Type.TIME,
                    true,
                    typeDescriptor.getParameters().stream()
                            .findAny()
                            .map(TypeDescriptorTranslator::toTypeParameter));
            case TIME -> new DateTimeDataType(
                    Optional.empty(),
                    DateTimeDataType.Type.TIME,
                    false,
                    typeDescriptor.getParameters().stream()
                            .findAny()
                            .map(TypeDescriptorTranslator::toTypeParameter));
            case ROW -> new RowDataType(
                    Optional.empty(),
                    typeDescriptor.getParameters().stream()
                            .map(parameter -> {
                                TypeParameter.Type typeParameter = (TypeParameter.Type) parameter;
                                return new RowDataType.Field(
                                        Optional.empty(),
                                        typeParameter.name().map(fieldName -> new Identifier(fieldName, requiresDelimiting(fieldName))),
                                        toDataType(typeParameter.type()));
                            })
                            .collect(toImmutableList()));
            case VARCHAR -> new GenericDataType(
                    Optional.empty(),
                    new Identifier(typeDescriptor.getBase(), false),
                    typeDescriptor.getParameters().stream()
                            .filter(parameter -> ((TypeParameter.Numeric) parameter).value() != UNBOUNDED_LENGTH)
                            .map(parameter -> new NumericParameter(Optional.empty(), parameter.toString()))
                            .collect(toImmutableList()));
            default -> new GenericDataType(
                    Optional.empty(),
                    new Identifier(typeDescriptor.getBase(), false),
                    typeDescriptor.getParameters().stream()
                            .map(TypeDescriptorTranslator::toTypeParameter)
                            .collect(toImmutableList()));
        };
    }

    private static boolean requiresDelimiting(String identifier)
    {
        if (!isValidIdentifier(identifier)) {
            return true;
        }

        return ReservedIdentifiers.reserved(identifier);
    }

    private static boolean isValidIdentifier(String identifier)
    {
        if (IS_DIGIT.matches(identifier.charAt(0))) {
            return false;
        }

        // We've already checked that first char does not contain digits,
        // so to avoid copying we are checking whole string.
        return IS_VALID_IDENTIFIER_CHAR.matchesAllOf(identifier);
    }

    private static DataTypeParameter toTypeParameter(TypeParameter parameter)
    {
        return switch (parameter) {
            case TypeParameter.Numeric numeric -> new NumericParameter(Optional.empty(), Long.toString(numeric.value()));
            case TypeParameter.Type type -> new io.trino.sql.tree.TypeParameter(toDataType(type.type()));
            default -> throw new UnsupportedOperationException("Unsupported parameter kind");
        };
    }

    private static DataType parseDataType(String signature)
    {
        return SQL_PARSER.createType(signature);
    }

    /// Whether a type signature is written with explicit parameters, distinguishing `varchar(10)` from a
    /// bare `varchar`. The written form is inspected directly, so an unbounded `varchar` -- which lowers
    /// to a magic length parameter -- is reported as having none.
    public static boolean hasTypeParameters(String signature)
    {
        return !(parseDataType(signature) instanceof GenericDataType genericDataType) || !genericDataType.getArguments().isEmpty();
    }
}
