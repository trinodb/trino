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
package io.prestosql.sql;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import io.airlift.slice.Slice;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.ResolvedFunction;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.function.TypeParameter;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.Range;
import io.prestosql.spi.predicate.ValueSet;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarcharType;
import io.prestosql.sql.planner.FunctionCallBuilder;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.plan.DynamicFilterId;
import io.prestosql.sql.tree.Cast;
import io.prestosql.sql.tree.ComparisonExpression;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.FunctionCall;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.sql.tree.StringLiteral;
import io.prestosql.sql.tree.SymbolReference;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableListMultimap.toImmutableListMultimap;
import static io.prestosql.spi.type.StandardTypes.BOOLEAN;
import static io.prestosql.spi.type.StandardTypes.VARCHAR;
import static io.prestosql.sql.ExpressionUtils.extractConjuncts;
import static io.prestosql.sql.tree.ComparisonExpression.Operator.EQUAL;
import static java.util.Objects.requireNonNull;

public final class DynamicFilters
{
    private DynamicFilters() {}

    public static Expression createDynamicFilterExpression(Metadata metadata, DynamicFilterId id, Type inputType, SymbolReference input, ComparisonExpression.Operator operator)
    {
        return createDynamicFilterExpression(metadata, id, inputType, (Expression) input, operator);
    }

    @VisibleForTesting
    public static Expression createDynamicFilterExpression(Metadata metadata, DynamicFilterId id, Type inputType, Expression input, ComparisonExpression.Operator operator)
    {
        return new FunctionCallBuilder(metadata)
                .setName(QualifiedName.of(Function.NAME))
                .addArgument(inputType, input)
                .addArgument(VarcharType.VARCHAR, new StringLiteral(operator.toString()))
                .addArgument(VarcharType.VARCHAR, new StringLiteral(id.toString()))
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
                        descriptor -> new DynamicFilters.Descriptor(descriptor.getId(), extractSourceSymbol(descriptor).toSymbolReference(), descriptor.getOperator())));
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

    public static boolean isDynamicFilter(Expression expression)
    {
        return getDescriptor(expression).isPresent();
    }

    public static Optional<Descriptor> getDescriptor(Expression expression)
    {
        if (!(expression instanceof FunctionCall)) {
            return Optional.empty();
        }

        FunctionCall functionCall = (FunctionCall) expression;
        boolean isDynamicFilterFunction = ResolvedFunction.extractFunctionName(functionCall.getName()).equals(Function.NAME);
        if (!isDynamicFilterFunction) {
            return Optional.empty();
        }

        List<Expression> arguments = functionCall.getArguments();
        checkArgument(arguments.size() == 3, "invalid arguments count: %s", arguments.size());

        Expression probeSymbol = arguments.get(0);

        Expression operatorExpression = arguments.get(1);
        checkArgument(operatorExpression instanceof StringLiteral, "operatorExpression is expected to be an instance of StringLiteral: %s", operatorExpression.getClass().getSimpleName());
        String operatorExpressionString = ((StringLiteral) operatorExpression).getValue();
        ComparisonExpression.Operator operator = ComparisonExpression.Operator.valueOf(operatorExpressionString);

        Expression idExpression = arguments.get(2);
        checkArgument(idExpression instanceof StringLiteral, "id is expected to be an instance of StringLiteral: %s", idExpression.getClass().getSimpleName());
        String id = ((StringLiteral) idExpression).getValue();
        return Optional.of(new Descriptor(new DynamicFilterId(id), probeSymbol, operator));
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

        public Descriptor(DynamicFilterId id, Expression input, ComparisonExpression.Operator operator)
        {
            this.id = requireNonNull(id, "id is null");
            this.input = requireNonNull(input, "input is null");
            this.operator = requireNonNull(operator, "operator is null");
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
                    Objects.equals(operator, that.operator);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(id, input, operator);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("id", id)
                    .add("input", input)
                    .add("operator", operator)
                    .toString();
        }

        public Domain applyComparison(Domain domain)
        {
            if (domain.isNone() || domain.isAll()) {
                return domain;
            }
            Range span = domain.getValues().getRanges().getSpan();
            switch (operator) {
                case EQUAL:
                    return domain;
                case LESS_THAN: {
                    Range range = Range.lessThan(span.getType(), span.getHigh().getValue());
                    return Domain.create(ValueSet.ofRanges(range), false);
                }
                case LESS_THAN_OR_EQUAL: {
                    Range range = Range.lessThanOrEqual(span.getType(), span.getHigh().getValue());
                    return Domain.create(ValueSet.ofRanges(range), false);
                }
                case GREATER_THAN: {
                    Range range = Range.greaterThan(span.getType(), span.getLow().getValue());
                    return Domain.create(ValueSet.ofRanges(range), false);
                }
                case GREATER_THAN_OR_EQUAL: {
                    Range range = Range.greaterThanOrEqual(span.getType(), span.getLow().getValue());
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

        private static final String NAME = "$internal$dynamic_filter_function";

        @TypeParameter("T")
        @SqlType(BOOLEAN)
        public static boolean dynamicFilter(@SqlType("T") Object input, @SqlType(VARCHAR) Slice operator, @SqlType(VARCHAR) Slice id)
        {
            throw new UnsupportedOperationException();
        }

        @TypeParameter("T")
        @SqlType(BOOLEAN)
        public static boolean dynamicFilter(@SqlType("T") long input, @SqlType(VARCHAR) Slice operator, @SqlType(VARCHAR) Slice id)
        {
            throw new UnsupportedOperationException();
        }

        @TypeParameter("T")
        @SqlType(BOOLEAN)
        public static boolean dynamicFilter(@SqlType("T") boolean input, @SqlType(VARCHAR) Slice operator, @SqlType(VARCHAR) Slice id)
        {
            throw new UnsupportedOperationException();
        }

        @TypeParameter("T")
        @SqlType(BOOLEAN)
        public static boolean dynamicFilter(@SqlType("T") double input, @SqlType(VARCHAR) Slice operator, @SqlType(VARCHAR) Slice id)
        {
            throw new UnsupportedOperationException();
        }
    }
}
