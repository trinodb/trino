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
package io.trino.sql;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.metadata.Metadata;
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.function.CatalogSchemaFunctionName;
import io.trino.spi.function.IsNull;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.ComparisonExpression;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.FunctionCall;
import io.trino.sql.ir.GenericLiteral;
import io.trino.sql.ir.NullLiteral;
import io.trino.sql.ir.SymbolReference;
import io.trino.sql.planner.BuiltinFunctionCallBuilder;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.DynamicFilterId;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableListMultimap.toImmutableListMultimap;
import static io.trino.metadata.GlobalFunctionCatalog.builtinFunctionName;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.StandardTypes.BOOLEAN;
import static io.trino.spi.type.StandardTypes.INTEGER;
import static io.trino.spi.type.StandardTypes.VARCHAR;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.sql.ir.BooleanLiteral.FALSE_LITERAL;
import static io.trino.sql.ir.BooleanLiteral.TRUE_LITERAL;
import static io.trino.sql.ir.ComparisonExpression.Operator.EQUAL;
import static io.trino.sql.ir.IrUtils.extractConjuncts;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class DynamicFilters
{
    private DynamicFilters() {}

    public static Expression createDynamicFilterExpression(
            Metadata metadata,
            DynamicFilterId id,
            Type inputType,
            Expression input,
            ComparisonExpression.Operator operator)
    {
        return createDynamicFilterExpression(metadata, id, inputType, input, operator, false);
    }

    public static Expression createDynamicFilterExpression(
            Metadata metadata,
            DynamicFilterId id,
            Type inputType,
            Expression input,
            ComparisonExpression.Operator operator,
            boolean nullAllowed)
    {
        return createDynamicFilterExpression(metadata, id, inputType, input, operator, nullAllowed, Optional.empty());
    }

    @VisibleForTesting
    public static Expression createDynamicFilterExpression(
            Metadata metadata,
            DynamicFilterId id,
            Type inputType,
            Expression input,
            ComparisonExpression.Operator operator,
            boolean nullAllowed,
            Optional<Long> minDynamicFilterTimeout)
    {
        Expression timeoutExpression;
        if (minDynamicFilterTimeout.isEmpty()) {
            timeoutExpression = new NullLiteral();
        }
        else {
            timeoutExpression = GenericLiteral.constant(IntegerType.INTEGER, minDynamicFilterTimeout.get());
        }
        return BuiltinFunctionCallBuilder.resolve(metadata)
                .setName(nullAllowed ? NullableFunction.NAME : Function.NAME)
                .addArgument(inputType, input)
                .addArgument(GenericLiteral.constant(VarcharType.VARCHAR, Slices.utf8Slice(operator.toString())))
                .addArgument(GenericLiteral.constant(VarcharType.VARCHAR, Slices.utf8Slice(id.toString())))
                .addArgument(BooleanType.BOOLEAN, nullAllowed ? TRUE_LITERAL : FALSE_LITERAL)
                .addArgument(IntegerType.INTEGER, timeoutExpression)
                .build();
    }

    @VisibleForTesting
    public static Expression createDynamicFilterExpression(Metadata metadata, DynamicFilterId id, Type inputType, Expression input)
    {
        return createDynamicFilterExpression(metadata, id, inputType, input, EQUAL);
    }

    public static ExtractResult extractDynamicFilters(Expression expression)
    {
        List<Expression> conjuncts = extractConjuncts(expression);

        ImmutableList.Builder<Expression> staticConjuncts = ImmutableList.builder();
        ImmutableList.Builder<Descriptor> dynamicConjuncts = ImmutableList.builder();

        for (Expression conjunct : conjuncts) {
            Optional<Descriptor> descriptor = getDescriptor(conjunct);
            if (descriptor.isPresent()) {
                dynamicConjuncts.add(descriptor.get());
            }
            else {
                staticConjuncts.add(conjunct);
            }
        }

        return new ExtractResult(staticConjuncts.build(), dynamicConjuncts.build());
    }

    public static Multimap<DynamicFilterId, Descriptor> extractSourceSymbols(List<DynamicFilters.Descriptor> dynamicFilters)
    {
        return dynamicFilters.stream()
                .collect(toImmutableListMultimap(
                        DynamicFilters.Descriptor::getId,
                        descriptor -> new DynamicFilters.Descriptor(
                                descriptor.getId(),
                                extractSourceSymbol(descriptor).toSymbolReference(),
                                descriptor.getOperator(),
                                descriptor.isNullAllowed(),
                                descriptor.getPreferredTimeout())));
    }

    public static Symbol extractSourceSymbol(DynamicFilters.Descriptor descriptor)
    {
        Expression dynamicFilterExpression = descriptor.getInput();
        if (dynamicFilterExpression instanceof SymbolReference) {
            return Symbol.from(dynamicFilterExpression);
        }
        checkState(dynamicFilterExpression instanceof Cast);
        checkState(((Cast) dynamicFilterExpression).getExpression() instanceof SymbolReference);
        return Symbol.from(((Cast) dynamicFilterExpression).getExpression());
    }

    public static Expression replaceDynamicFilterId(FunctionCall dynamicFilterFunctionCall, DynamicFilterId newId)
    {
        return new FunctionCall(
                dynamicFilterFunctionCall.getName(),
                ImmutableList.of(
                        dynamicFilterFunctionCall.getArguments().get(0),
                        dynamicFilterFunctionCall.getArguments().get(1),
                        GenericLiteral.constant(VarcharType.VARCHAR, Slices.utf8Slice(newId.toString())), // dynamic filter id is the 3rd argument
                        dynamicFilterFunctionCall.getArguments().get(3),
                        dynamicFilterFunctionCall.getArguments().get(4)));
    }

    public static Expression replaceDynamicFilterTimeout(FunctionCall dynamicFilterFunctionCall, long timeout)
    {
        Expression timeoutArgument = GenericLiteral.constant(IntegerType.INTEGER, timeout);

        return new FunctionCall(
                dynamicFilterFunctionCall.getName(),
                ImmutableList.of(
                        dynamicFilterFunctionCall.getArguments().get(0),
                        dynamicFilterFunctionCall.getArguments().get(1),
                        dynamicFilterFunctionCall.getArguments().get(2),
                        dynamicFilterFunctionCall.getArguments().get(3),
                        timeoutArgument));
    }

    public static boolean isDynamicFilter(Expression expression)
    {
        return getDescriptor(expression).isPresent();
    }

    public static Optional<Descriptor> getDescriptor(Expression expression)
    {
        if (!(expression instanceof FunctionCall functionCall)) {
            return Optional.empty();
        }

        if (!isDynamicFilterFunction(functionCall)) {
            return Optional.empty();
        }

        List<Expression> arguments = functionCall.getArguments();
        checkArgument(arguments.size() == 5, "invalid arguments count: %s", arguments.size());

        Expression probeSymbol = arguments.get(0);

        Expression operatorExpression = arguments.get(1);
        checkArgument(operatorExpression instanceof GenericLiteral literal && literal.getType().equals(VarcharType.VARCHAR), "operatorExpression is expected to be a varchar: %s", operatorExpression.getClass().getSimpleName());
        String operatorExpressionString = ((Slice) ((GenericLiteral) operatorExpression).getRawValue()).toStringUtf8();
        ComparisonExpression.Operator operator = ComparisonExpression.Operator.valueOf(operatorExpressionString);

        Expression idExpression = arguments.get(2);
        checkArgument(idExpression instanceof GenericLiteral literal && literal.getType().equals(VarcharType.VARCHAR), "id is expected to be a varchar: %s", idExpression.getClass().getSimpleName());
        String id = ((Slice) ((GenericLiteral) idExpression).getRawValue()).toStringUtf8();

        Expression nullAllowedExpression = arguments.get(3);
        checkArgument(nullAllowedExpression instanceof GenericLiteral literal && literal.getType().equals(BooleanType.BOOLEAN), "nullAllowedExpression is expected to be a boolean constant: %s", nullAllowedExpression.getClass().getSimpleName());
        boolean nullAllowed = Boolean.parseBoolean(((GenericLiteral) nullAllowedExpression).getValue());

        Expression timeoutExpression = arguments.get(4);
        OptionalLong timeout;
        if (timeoutExpression instanceof NullLiteral) {
            timeout = OptionalLong.empty();
        }
        else if (timeoutExpression instanceof GenericLiteral longTimeoutLiteral && isInteger(longTimeoutLiteral.getType())) {
            timeout = OptionalLong.of(Long.parseLong(longTimeoutLiteral.getValue()));
        }
        else {
            throw new IllegalArgumentException(format("timeout is expected to be an instance of LongLiteral or NullLiteral: %s", timeoutExpression.getClass().getSimpleName()));
        }

        return Optional.of(new Descriptor(new DynamicFilterId(id), probeSymbol, operator, nullAllowed, timeout));
    }

    private static boolean isInteger(Type type)
    {
        return type.equals(BIGINT)
                || type.equals(IntegerType.INTEGER)
                || type.equals(SMALLINT)
                || type.equals(TINYINT);
    }

    private static boolean isDynamicFilterFunction(FunctionCall functionCall)
    {
        return isDynamicFilterFunction(ResolvedFunction.extractFunctionName(functionCall.getName()));
    }

    public static boolean isDynamicFilterFunction(CatalogSchemaFunctionName functionName)
    {
        return functionName.equals(builtinFunctionName(Function.NAME)) || functionName.equals(builtinFunctionName(NullableFunction.NAME));
    }

    public static class ExtractResult
    {
        private final List<Expression> staticConjuncts;
        private final List<Descriptor> dynamicConjuncts;

        public ExtractResult(List<Expression> staticConjuncts, List<Descriptor> dynamicConjuncts)
        {
            this.staticConjuncts = ImmutableList.copyOf(requireNonNull(staticConjuncts, "staticConjuncts is null"));
            this.dynamicConjuncts = ImmutableList.copyOf(requireNonNull(dynamicConjuncts, "dynamicConjuncts is null"));
        }

        public List<Expression> getStaticConjuncts()
        {
            return staticConjuncts;
        }

        public List<Descriptor> getDynamicConjuncts()
        {
            return dynamicConjuncts;
        }
    }

    public static final class Descriptor
    {
        private final DynamicFilterId id;
        private final Expression input;
        private final ComparisonExpression.Operator operator;
        private final boolean nullAllowed;
        private final OptionalLong preferredTimeout;

        public Descriptor(DynamicFilterId id, Expression input, ComparisonExpression.Operator operator, boolean nullAllowed, OptionalLong preferredTimeout)
        {
            this.id = requireNonNull(id, "id is null");
            this.input = requireNonNull(input, "input is null");
            this.operator = requireNonNull(operator, "operator is null");
            checkArgument(!nullAllowed || operator == EQUAL, "nullAllowed should be true only with EQUAL operator");
            this.nullAllowed = nullAllowed;
            this.preferredTimeout = requireNonNull(preferredTimeout, "preferredTimeout is null");
        }

        public Descriptor(DynamicFilterId id, Expression input, ComparisonExpression.Operator operator)
        {
            this(id, input, operator, false, OptionalLong.empty());
        }

        public Descriptor(DynamicFilterId id, Expression input)
        {
            this(id, input, EQUAL);
        }

        public DynamicFilterId getId()
        {
            return id;
        }

        public Expression getInput()
        {
            return input;
        }

        public ComparisonExpression.Operator getOperator()
        {
            return operator;
        }

        public boolean isNullAllowed()
        {
            return nullAllowed;
        }

        public OptionalLong getPreferredTimeout()
        {
            return preferredTimeout;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Descriptor that = (Descriptor) o;
            return Objects.equals(id, that.id) &&
                    Objects.equals(input, that.input) &&
                    Objects.equals(operator, that.operator) &&
                    nullAllowed == that.nullAllowed &&
                    preferredTimeout.equals(that.preferredTimeout);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(id, input, operator, nullAllowed);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("id", id)
                    .add("input", input)
                    .add("operator", operator)
                    .add("nullAllowed", nullAllowed)
                    .add("timeout", preferredTimeout.isPresent() ? preferredTimeout.getAsLong() : null)
                    .toString();
        }

        public Domain applyComparison(Domain domain)
        {
            if (domain.isAll()) {
                return domain;
            }
            if (domain.isNone()) {
                // Dynamic filter collection skips nulls
                // In case of IS NOT DISTINCT FROM, an empty Domain should still allow null
                if (nullAllowed) {
                    return Domain.onlyNull(domain.getType());
                }
                return domain;
            }
            Range span = domain.getValues().getRanges().getSpan();
            switch (operator) {
                case EQUAL:
                    if (nullAllowed) {
                        return Domain.create(domain.getValues(), true);
                    }
                    return domain;
                case LESS_THAN: {
                    Range range = Range.lessThan(span.getType(), span.getHighBoundedValue());
                    return Domain.create(ValueSet.ofRanges(range), false);
                }
                case LESS_THAN_OR_EQUAL: {
                    Range range = Range.lessThanOrEqual(span.getType(), span.getHighBoundedValue());
                    return Domain.create(ValueSet.ofRanges(range), false);
                }
                case GREATER_THAN: {
                    Range range = Range.greaterThan(span.getType(), span.getLowBoundedValue());
                    return Domain.create(ValueSet.ofRanges(range), false);
                }
                case GREATER_THAN_OR_EQUAL: {
                    Range range = Range.greaterThanOrEqual(span.getType(), span.getLowBoundedValue());
                    return Domain.create(ValueSet.ofRanges(range), false);
                }
                default:
                    throw new IllegalArgumentException("Unsupported dynamic filtering comparison operator: " + operator);
            }
        }
    }

    @ScalarFunction(value = Function.NAME, hidden = true)
    public static final class Function
    {
        private Function() {}

        public static final String NAME = "$internal$dynamic_filter_function";

        @TypeParameter("T")
        @SqlType(BOOLEAN)
        public static boolean dynamicFilter(@SqlType("T") Object input, @SqlType(VARCHAR) Slice operator, @SqlType(VARCHAR) Slice id, @SqlType(BOOLEAN) boolean nullAllowed, @SqlNullable @SqlType(INTEGER) Long timeout)
        {
            throw new UnsupportedOperationException();
        }

        @TypeParameter("T")
        @SqlType(BOOLEAN)
        public static boolean dynamicFilter(@SqlType("T") long input, @SqlType(VARCHAR) Slice operator, @SqlType(VARCHAR) Slice id, @SqlType(BOOLEAN) boolean nullAllowed, @SqlNullable @SqlType(INTEGER) Long timeout)
        {
            throw new UnsupportedOperationException();
        }

        @TypeParameter("T")
        @SqlType(BOOLEAN)
        public static boolean dynamicFilter(@SqlType("T") boolean input, @SqlType(VARCHAR) Slice operator, @SqlType(VARCHAR) Slice id, @SqlType(BOOLEAN) boolean nullAllowed, @SqlNullable @SqlType(INTEGER) Long timeout)
        {
            throw new UnsupportedOperationException();
        }

        @TypeParameter("T")
        @SqlType(BOOLEAN)
        public static boolean dynamicFilter(@SqlType("T") double input, @SqlType(VARCHAR) Slice operator, @SqlType(VARCHAR) Slice id, @SqlType(BOOLEAN) boolean nullAllowed, @SqlNullable @SqlType(INTEGER) Long timeout)
        {
            throw new UnsupportedOperationException();
        }
    }

    // Used for "IS NOT DISTINCT FROM" to let the engine know that the
    // DF expression accepts null parameters.
    // This in turn may influence optimization decisions like whether
    // LEFT join can be converted to INNER.
    @ScalarFunction(value = NullableFunction.NAME, hidden = true)
    public static final class NullableFunction
    {
        private NullableFunction() {}

        private static final String NAME = "$internal$dynamic_filter_nullable_function";

        @TypeParameter("T")
        @SqlType(BOOLEAN)
        public static boolean dynamicFilter(@SqlType("T") Object input, @IsNull boolean inputNull, @SqlType(VARCHAR) Slice operator, @SqlType(VARCHAR) Slice id, @SqlType(BOOLEAN) boolean nullAllowed, @SqlNullable @SqlType(INTEGER) Long timeout)
        {
            throw new UnsupportedOperationException();
        }

        @TypeParameter("T")
        @SqlType(BOOLEAN)
        public static boolean dynamicFilter(@SqlType("T") long input, @IsNull boolean inputNull, @SqlType(VARCHAR) Slice operator, @SqlType(VARCHAR) Slice id, @SqlType(BOOLEAN) boolean nullAllowed, @SqlNullable @SqlType(INTEGER) Long timeout)
        {
            throw new UnsupportedOperationException();
        }

        @TypeParameter("T")
        @SqlType(BOOLEAN)
        public static boolean dynamicFilter(@SqlType("T") boolean input, @IsNull boolean inputNull, @SqlType(VARCHAR) Slice operator, @SqlType(VARCHAR) Slice id, @SqlType(BOOLEAN) boolean nullAllowed, @SqlNullable @SqlType(INTEGER) Long timeout)
        {
            throw new UnsupportedOperationException();
        }

        @TypeParameter("T")
        @SqlType(BOOLEAN)
        public static boolean dynamicFilter(@SqlType("T") double input, @IsNull boolean inputNull, @SqlType(VARCHAR) Slice operator, @SqlType(VARCHAR) Slice id, @SqlType(BOOLEAN) boolean nullAllowed, @SqlNullable @SqlType(INTEGER) Long timeout)
        {
            throw new UnsupportedOperationException();
        }
    }
}
