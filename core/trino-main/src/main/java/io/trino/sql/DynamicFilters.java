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
import io.trino.Session;
import io.trino.metadata.Metadata;
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.function.IsNull;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.sql.planner.FunctionCallBuilder;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.DynamicFilterId;
import io.trino.sql.tree.BooleanLiteral;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.StringLiteral;
import io.trino.sql.tree.SymbolReference;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableListMultimap.toImmutableListMultimap;
import static io.trino.spi.type.StandardTypes.BOOLEAN;
import static io.trino.spi.type.StandardTypes.VARCHAR;
import static io.trino.sql.ExpressionUtils.extractConjuncts;
import static io.trino.sql.tree.BooleanLiteral.FALSE_LITERAL;
import static io.trino.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static io.trino.sql.tree.ComparisonExpression.Operator.EQUAL;
import static java.util.Objects.requireNonNull;

public final class DynamicFilters
{
    private DynamicFilters() {}

    public static Expression createDynamicFilterExpression(
            Session session,
            Metadata metadata,
            DynamicFilterId id,
            Type inputType,
            SymbolReference input,
            ComparisonExpression.Operator operator,
            boolean nullAllowed)
    {
        return createDynamicFilterExpression(session, metadata, id, inputType, (Expression) input, operator, nullAllowed);
    }

    @VisibleForTesting
    public static Expression createDynamicFilterExpression(
            Session session,
            Metadata metadata,
            DynamicFilterId id,
            Type inputType,
            Expression input,
            ComparisonExpression.Operator operator)
    {
        return createDynamicFilterExpression(session, metadata, id, inputType, input, operator, false);
    }

    @VisibleForTesting
    public static Expression createDynamicFilterExpression(
            Session session,
            Metadata metadata,
            DynamicFilterId id,
            Type inputType,
            Expression input,
            ComparisonExpression.Operator operator,
            boolean nullAllowed)
    {
        return FunctionCallBuilder.resolve(session, metadata)
                .setName(QualifiedName.of(nullAllowed ? NullableFunction.NAME : Function.NAME))
                .addArgument(inputType, input)
                .addArgument(VarcharType.VARCHAR, new StringLiteral(operator.toString()))
                .addArgument(VarcharType.VARCHAR, new StringLiteral(id.toString()))
                .addArgument(BooleanType.BOOLEAN, nullAllowed ? TRUE_LITERAL : FALSE_LITERAL)
                .build();
    }

    @VisibleForTesting
    public static Expression createDynamicFilterExpression(Session session, Metadata metadata, DynamicFilterId id, Type inputType, Expression input)
    {
        return createDynamicFilterExpression(session, metadata, id, inputType, input, EQUAL);
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
                                descriptor.isNullAllowed())));
    }

    private static Symbol extractSourceSymbol(DynamicFilters.Descriptor descriptor)
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
                dynamicFilterFunctionCall.getLocation(),
                dynamicFilterFunctionCall.getName(),
                dynamicFilterFunctionCall.getWindow(),
                dynamicFilterFunctionCall.getFilter(),
                dynamicFilterFunctionCall.getOrderBy(),
                dynamicFilterFunctionCall.isDistinct(),
                dynamicFilterFunctionCall.getNullTreatment(),
                dynamicFilterFunctionCall.getProcessingMode(),
                ImmutableList.of(
                        dynamicFilterFunctionCall.getArguments().get(0),
                        dynamicFilterFunctionCall.getArguments().get(1),
                        new StringLiteral(newId.toString()), // dynamic filter id is the 3rd argument
                        dynamicFilterFunctionCall.getArguments().get(3)));
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
        checkArgument(arguments.size() == 4, "invalid arguments count: %s", arguments.size());

        Expression probeSymbol = arguments.get(0);

        Expression operatorExpression = arguments.get(1);
        checkArgument(operatorExpression instanceof StringLiteral, "operatorExpression is expected to be an instance of StringLiteral: %s", operatorExpression.getClass().getSimpleName());
        String operatorExpressionString = ((StringLiteral) operatorExpression).getValue();
        ComparisonExpression.Operator operator = ComparisonExpression.Operator.valueOf(operatorExpressionString);

        Expression idExpression = arguments.get(2);
        checkArgument(idExpression instanceof StringLiteral, "id is expected to be an instance of StringLiteral: %s", idExpression.getClass().getSimpleName());
        String id = ((StringLiteral) idExpression).getValue();

        Expression nullAllowedExpression = arguments.get(3);
        checkArgument(nullAllowedExpression instanceof BooleanLiteral, "nullAllowedExpression is expected to be an instance of BooleanLiteral: %s", nullAllowedExpression.getClass().getSimpleName());
        boolean nullAllowed = ((BooleanLiteral) nullAllowedExpression).getValue();
        return Optional.of(new Descriptor(new DynamicFilterId(id), probeSymbol, operator, nullAllowed));
    }

    private static boolean isDynamicFilterFunction(FunctionCall functionCall)
    {
        String functionName = ResolvedFunction.extractFunctionName(functionCall.getName());
        return functionName.equals(Function.NAME) || functionName.equals(NullableFunction.NAME);
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

        public Descriptor(DynamicFilterId id, Expression input, ComparisonExpression.Operator operator, boolean nullAllowed)
        {
            this.id = requireNonNull(id, "id is null");
            this.input = requireNonNull(input, "input is null");
            this.operator = requireNonNull(operator, "operator is null");
            checkArgument(!nullAllowed || operator == EQUAL, "nullAllowed should be true only with EQUAL operator");
            this.nullAllowed = nullAllowed;
        }

        public Descriptor(DynamicFilterId id, Expression input, ComparisonExpression.Operator operator)
        {
            this(id, input, operator, false);
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
                    nullAllowed == that.nullAllowed;
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
        public static boolean dynamicFilter(@SqlType("T") Object input, @SqlType(VARCHAR) Slice operator, @SqlType(VARCHAR) Slice id, @SqlType(BOOLEAN) boolean nullAllowed)
        {
            throw new UnsupportedOperationException();
        }

        @TypeParameter("T")
        @SqlType(BOOLEAN)
        public static boolean dynamicFilter(@SqlType("T") long input, @SqlType(VARCHAR) Slice operator, @SqlType(VARCHAR) Slice id, @SqlType(BOOLEAN) boolean nullAllowed)
        {
            throw new UnsupportedOperationException();
        }

        @TypeParameter("T")
        @SqlType(BOOLEAN)
        public static boolean dynamicFilter(@SqlType("T") boolean input, @SqlType(VARCHAR) Slice operator, @SqlType(VARCHAR) Slice id, @SqlType(BOOLEAN) boolean nullAllowed)
        {
            throw new UnsupportedOperationException();
        }

        @TypeParameter("T")
        @SqlType(BOOLEAN)
        public static boolean dynamicFilter(@SqlType("T") double input, @SqlType(VARCHAR) Slice operator, @SqlType(VARCHAR) Slice id, @SqlType(BOOLEAN) boolean nullAllowed)
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
        public static boolean dynamicFilter(@SqlType("T") Object input, @IsNull boolean inputNull, @SqlType(VARCHAR) Slice operator, @SqlType(VARCHAR) Slice id, @SqlType(BOOLEAN) boolean nullAllowed)
        {
            throw new UnsupportedOperationException();
        }

        @TypeParameter("T")
        @SqlType(BOOLEAN)
        public static boolean dynamicFilter(@SqlType("T") long input, @IsNull boolean inputNull, @SqlType(VARCHAR) Slice operator, @SqlType(VARCHAR) Slice id, @SqlType(BOOLEAN) boolean nullAllowed)
        {
            throw new UnsupportedOperationException();
        }

        @TypeParameter("T")
        @SqlType(BOOLEAN)
        public static boolean dynamicFilter(@SqlType("T") boolean input, @IsNull boolean inputNull, @SqlType(VARCHAR) Slice operator, @SqlType(VARCHAR) Slice id, @SqlType(BOOLEAN) boolean nullAllowed)
        {
            throw new UnsupportedOperationException();
        }

        @TypeParameter("T")
        @SqlType(BOOLEAN)
        public static boolean dynamicFilter(@SqlType("T") double input, @IsNull boolean inputNull, @SqlType(VARCHAR) Slice operator, @SqlType(VARCHAR) Slice id, @SqlType(BOOLEAN) boolean nullAllowed)
        {
            throw new UnsupportedOperationException();
        }
    }
}
