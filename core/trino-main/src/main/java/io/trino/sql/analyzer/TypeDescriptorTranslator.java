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
import io.trino.sql.tree.IntervalQualifier;
import io.trino.sql.tree.NodeLocation;
import io.trino.sql.tree.NumericParameter;
import io.trino.sql.tree.RowDataType;
import io.trino.sql.tree.SimpleIntervalQualifier;
import io.trino.type.IntervalDayTimeType;
import io.trino.type.IntervalYearMonthType;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.type.StandardTypes.INTERVAL_DAY_TO_SECOND;
import static io.trino.spi.type.StandardTypes.INTERVAL_YEAR_TO_MONTH;
import static io.trino.spi.type.StandardTypes.ROW;
import static io.trino.spi.type.StandardTypes.TIME;
import static io.trino.spi.type.StandardTypes.TIMESTAMP;
import static io.trino.spi.type.StandardTypes.TIMESTAMP_WITH_TIME_ZONE;
import static io.trino.spi.type.StandardTypes.TIME_WITH_TIME_ZONE;
import static io.trino.spi.type.StandardTypes.VARCHAR;
import static io.trino.spi.type.TypeParameter.numericParameter;
import static io.trino.spi.type.VarcharType.UNBOUNDED_LENGTH;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;

public final class TypeDescriptorTranslator
{
    /// The SQL standard's implicit leading-field precision for an interval type written without one
    /// (ISO/IEC 9075-2 §6.1). Interval literals override this by inferring from the value.
    private static final int IMPLICIT_LEADING_PRECISION = 2;

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
            return toTypeDescriptor(DATA_TYPE_CACHE.get(signature, () -> parseDataType(signature)));
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
            case IntervalDataType intervalDataType -> toTypeTemplate(intervalDataType, numericVariables);
        };
    }

    private static TypeTemplate toTypeTemplate(IntervalDataType type, Set<String> numericVariables)
    {
        IntervalParts parts = intervalParts(type);
        NumericExpression precision;
        if (parts.precision().isEmpty()) {
            // A written interval type with no precision takes the SQL standard's implicit leading precision.
            precision = new NumericExpression.Literal(IMPLICIT_LEADING_PRECISION);
        }
        else if (parts.precision().get() instanceof NumericParameter numeric) {
            precision = new NumericExpression.Literal(validateLeadingPrecision(parts, numeric.getParsedValue()));
        }
        else if (parts.precision().get() instanceof io.trino.sql.tree.TypeParameter typeParameter) {
            DataType value = typeParameter.getValue();
            if (!(value instanceof GenericDataType genericDataType) || !genericDataType.getArguments().isEmpty()) {
                throw new IllegalArgumentException("Parameter to interval type must be either a number or a variable");
            }
            String variable = genericDataType.getName().getValue();
            checkArgument(numericVariables.contains(variable), "Parameter to interval type must be a number or a numeric variable: %s", variable);
            precision = new NumericExpression.Variable(variable);
        }
        else {
            throw new IllegalArgumentException("Unexpected interval precision parameter: " + parts.precision().get());
        }

        if (parts.yearMonth()) {
            return new TypeTemplate.TypeApplication(parts.base(), List.of(
                    new TemplateParameter.NumericArgument(new NumericExpression.Literal(parts.startField().code())),
                    new TemplateParameter.NumericArgument(new NumericExpression.Literal(parts.endField().code())),
                    new TemplateParameter.NumericArgument(precision)));
        }
        // A day-time interval carries a fourth parameter, the fractional-seconds precision.
        return new TypeTemplate.TypeApplication(parts.base(), List.of(
                new TemplateParameter.NumericArgument(new NumericExpression.Literal(parts.startField().code())),
                new TemplateParameter.NumericArgument(new NumericExpression.Literal(parts.endField().code())),
                new TemplateParameter.NumericArgument(precision),
                new TemplateParameter.NumericArgument(fractionalPrecision(parts, numericVariables))));
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

    /// The start field, end field, base name, (possibly variable) leading precision, and explicit
    /// fractional-seconds precision of an interval qualifier. The fractional precision is present only
    /// when the trailing field is `SECOND` and was written with a precision.
    private record IntervalParts(io.trino.spi.type.IntervalField startField, io.trino.spi.type.IntervalField endField, boolean yearMonth, String base, Optional<DataTypeParameter> precision, Optional<DataTypeParameter> fractionalPrecision) {}

    private static IntervalParts intervalParts(IntervalDataType type)
    {
        IntervalField from;
        IntervalField to;
        Optional<DataTypeParameter> precision;
        switch (type.qualifier()) {
            case SimpleIntervalQualifier qualifier -> {
                from = qualifier.getField();
                to = qualifier.getField();
                precision = qualifier.getPrecision();
            }
            case CompositeIntervalQualifier qualifier -> {
                from = qualifier.getFrom();
                to = qualifier.getTo();
                precision = qualifier.getPrecision();
            }
        }

        Optional<DataTypeParameter> fractionalPrecision = to instanceof IntervalField.Second(Optional<DataTypeParameter> fractional) ? fractional : Optional.empty();

        io.trino.spi.type.IntervalField startField = toIntervalField(from);
        io.trino.spi.type.IntervalField endField = toIntervalField(to);
        boolean yearMonth = startField == io.trino.spi.type.IntervalField.YEAR || startField == io.trino.spi.type.IntervalField.MONTH;
        String base = yearMonth ? INTERVAL_YEAR_TO_MONTH : INTERVAL_DAY_TO_SECOND;
        return new IntervalParts(startField, endField, yearMonth, base, precision, fractionalPrecision);
    }

    /// The fractional-seconds precision an interval template carries: the written value (validated), a
    /// numeric variable, or the trailing field's default when none was written. Mirrors the leading
    /// precision, which is also a [DataTypeParameter] that may be a literal or a variable.
    private static NumericExpression fractionalPrecision(IntervalParts parts, Set<String> numericVariables)
    {
        if (parts.fractionalPrecision().isEmpty()) {
            return new NumericExpression.Literal(IntervalDayTimeType.defaultFractionalPrecision(parts.endField()));
        }
        DataTypeParameter parameter = parts.fractionalPrecision().get();
        if (parameter instanceof NumericParameter numeric) {
            long value = numeric.getParsedValue();
            if (value < 0 || value > IntervalDayTimeType.MAX_FRACTIONAL_PRECISION) {
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, format("INTERVAL fractional seconds precision must be in range [0, %s]: %s", IntervalDayTimeType.MAX_FRACTIONAL_PRECISION, value));
            }
            return new NumericExpression.Literal(value);
        }
        if (parameter instanceof io.trino.sql.tree.TypeParameter typeParameter) {
            DataType value = typeParameter.getValue();
            if (!(value instanceof GenericDataType genericDataType) || !genericDataType.getArguments().isEmpty()) {
                throw new IllegalArgumentException("Parameter to interval type must be either a number or a variable");
            }
            String variable = genericDataType.getName().getValue();
            checkArgument(numericVariables.contains(variable), "Parameter to interval type must be a number or a numeric variable: %s", variable);
            return new NumericExpression.Variable(variable);
        }
        throw new IllegalArgumentException("Unexpected interval fractional precision parameter: " + parameter);
    }

    private static int validateLeadingPrecision(IntervalParts parts, long value)
    {
        int maxPrecision = parts.yearMonth() ? IntervalYearMonthType.maxLeadingPrecision(parts.startField()) : IntervalDayTimeType.maxLeadingPrecision(parts.startField());
        if (value < 1 || value > maxPrecision) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, format("INTERVAL %s leading precision must be in range [1, %s]: %s", parts.startField().keyword(), maxPrecision, value));
        }
        return (int) value;
    }

    /// Whether the interval type's qualifier was written with an explicit leading-field precision.
    /// A literal without one infers its precision from the value rather than taking the implicit default.
    public static boolean intervalQualifierHasPrecision(IntervalDataType type)
    {
        return intervalParts(type).precision().isPresent();
    }

    /// Whether the interval qualifier is the bare single field `SECOND` written with no explicit
    /// fractional-seconds precision. A literal in that case infers its fractional precision from the
    /// value; a multi-field qualifier such as `DAY TO SECOND` keeps the SQL default instead.
    public static boolean intervalQualifierHasInferableFractionalPrecision(IntervalDataType type)
    {
        IntervalParts parts = intervalParts(type);
        return parts.startField() == io.trino.spi.type.IntervalField.SECOND && parts.fractionalPrecision().isEmpty();
    }

    /// Replaces a day-time interval descriptor's fractional-seconds precision with the number of
    /// fractional digits in the literal value — e.g. `INTERVAL '1.123' SECOND` is `interval second(.., 3)`
    /// and `INTERVAL '5' SECOND` is `interval second(.., 0)` — capped at the maximum. Called for a
    /// `SECOND`-ending literal whose qualifier carried no explicit fractional precision.
    public static TypeDescriptor inferIntervalFractionalPrecision(TypeDescriptor descriptor, String value)
    {
        List<TypeParameter> parameters = descriptor.getParameters();
        int fractionalPrecision = Math.min(fractionalDigitCount(value), IntervalDayTimeType.MAX_FRACTIONAL_PRECISION);
        return new TypeDescriptor(descriptor.getBase(), parameters.get(0), parameters.get(1), parameters.get(2), numericParameter(fractionalPrecision));
    }

    private static int fractionalDigitCount(String value)
    {
        int dot = value.indexOf('.');
        if (dot < 0) {
            return 0;
        }
        int digits = 0;
        for (int index = dot + 1; index < value.length() && Character.isDigit(value.charAt(index)); index++) {
            digits++;
        }
        return digits;
    }

    /// Replaces an interval descriptor's leading-field precision with the width of the literal value's
    /// leading component — e.g. `INTERVAL '340' DAY` is `interval day(3)` — capped at the field's
    /// maximum. Called for a literal whose qualifier carried no explicit precision.
    public static TypeDescriptor inferIntervalLeadingPrecision(TypeDescriptor descriptor, String value)
    {
        List<TypeParameter> parameters = descriptor.getParameters();
        io.trino.spi.type.IntervalField startField = io.trino.spi.type.IntervalField.fromCode((int) ((TypeParameter.Numeric) parameters.get(0)).value());
        boolean yearMonth = startField == io.trino.spi.type.IntervalField.YEAR || startField == io.trino.spi.type.IntervalField.MONTH;
        int maxPrecision = yearMonth ? IntervalYearMonthType.maxLeadingPrecision(startField) : IntervalDayTimeType.maxLeadingPrecision(startField);
        int precision = Math.min(leadingDigitCount(value), maxPrecision);
        // A day-time interval keeps its fourth parameter, the fractional-seconds precision.
        if (parameters.size() > 3) {
            return new TypeDescriptor(descriptor.getBase(), parameters.get(0), parameters.get(1), numericParameter(precision), parameters.get(3));
        }
        return new TypeDescriptor(descriptor.getBase(), parameters.get(0), parameters.get(1), numericParameter(precision));
    }

    private static int leadingDigitCount(String value)
    {
        int index = 0;
        while (index < value.length() && !Character.isDigit(value.charAt(index))) {
            index++;
        }
        int digits = 0;
        while (index < value.length() && Character.isDigit(value.charAt(index))) {
            digits++;
            index++;
        }
        return Math.max(1, digits);
    }

    private static io.trino.spi.type.IntervalField toIntervalField(IntervalField field)
    {
        return switch (field) {
            case IntervalField.Year _ -> io.trino.spi.type.IntervalField.YEAR;
            case IntervalField.Month _ -> io.trino.spi.type.IntervalField.MONTH;
            case IntervalField.Day _ -> io.trino.spi.type.IntervalField.DAY;
            case IntervalField.Hour _ -> io.trino.spi.type.IntervalField.HOUR;
            case IntervalField.Minute _ -> io.trino.spi.type.IntervalField.MINUTE;
            case IntervalField.Second _ -> io.trino.spi.type.IntervalField.SECOND;
        };
    }

    private static IntervalField toIntervalFieldNode(io.trino.spi.type.IntervalField field, Optional<DataTypeParameter> fractionalPrecision)
    {
        return switch (field) {
            case YEAR -> new IntervalField.Year();
            case MONTH -> new IntervalField.Month();
            case DAY -> new IntervalField.Day();
            case HOUR -> new IntervalField.Hour();
            case MINUTE -> new IntervalField.Minute();
            case SECOND -> new IntervalField.Second(fractionalPrecision);
        };
    }

    private static IntervalDataType toIntervalDataType(TypeDescriptor typeDescriptor)
    {
        List<TypeParameter> parameters = typeDescriptor.getParameters();
        io.trino.spi.type.IntervalField startField = io.trino.spi.type.IntervalField.fromCode((int) ((TypeParameter.Numeric) parameters.get(0)).value());
        io.trino.spi.type.IntervalField endField = io.trino.spi.type.IntervalField.fromCode((int) ((TypeParameter.Numeric) parameters.get(1)).value());
        long precision = parameters.size() > 2 ? ((TypeParameter.Numeric) parameters.get(2)).value() : IMPLICIT_LEADING_PRECISION;
        Optional<DataTypeParameter> leadingPrecision = Optional.of(new NumericParameter(new NodeLocation(1, 1), Long.toString(precision)));
        // A day-time interval ending in SECOND carries the fractional-seconds precision in its fourth parameter.
        Optional<DataTypeParameter> fractionalPrecision = endField == io.trino.spi.type.IntervalField.SECOND && parameters.size() > 3
                ? Optional.of(new NumericParameter(new NodeLocation(1, 1), Long.toString(((TypeParameter.Numeric) parameters.get(3)).value())))
                : Optional.empty();

        IntervalQualifier qualifier;
        if (startField == endField) {
            qualifier = new SimpleIntervalQualifier(new NodeLocation(1, 1), leadingPrecision, toIntervalFieldNode(startField, fractionalPrecision));
        }
        else {
            qualifier = new CompositeIntervalQualifier(new NodeLocation(1, 1), leadingPrecision, toIntervalFieldNode(startField, Optional.empty()), toIntervalFieldNode(endField, fractionalPrecision));
        }
        return new IntervalDataType(Optional.empty(), qualifier);
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
            case INTERVAL_YEAR_TO_MONTH, INTERVAL_DAY_TO_SECOND -> toIntervalDataType(typeDescriptor);
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
