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
package io.trino.plugin.base.aggregation;

import com.google.common.collect.ImmutableList;
import io.trino.matching.Captures;
import io.trino.matching.Match;
import io.trino.matching.Pattern;
import io.trino.matching.PatternVisitor;
import io.trino.matching.Property;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public final class AggregateFunctionPatterns
{
    private AggregateFunctionPatterns() {}

    public static Pattern<AggregateFunction> basicAggregation()
    {
        return Pattern.typeOf(AggregateFunction.class)
                .with(hasSortOrder().equalTo(false))
                .with(distinct().equalTo(false))
                .with(hasFilter().equalTo(false));
    }

    public static Property<AggregateFunction, ?, String> functionName()
    {
        return Property.property("functionName", AggregateFunction::getFunctionName);
    }

    public static Property<AggregateFunction, ?, Type> outputType()
    {
        return Property.property("outputType", AggregateFunction::getOutputType);
    }

    public static Property<AggregateFunction, ?, List<ConnectorExpression>> arguments()
    {
        return Property.property("arguments", AggregateFunction::getArguments);
    }

    public static Property<AggregateFunction, ?, ConnectorExpression> singleArgument()
    {
        return Property.optionalProperty("arguments", aggregateFunction -> {
            List<ConnectorExpression> arguments = aggregateFunction.getArguments();
            if (arguments.size() != 1) {
                return Optional.empty();
            }
            return Optional.of(arguments.get(0));
        });
    }

    public static Property<AggregateFunction, ?, Boolean> hasSortOrder()
    {
        return Property.property("hasSortOrder", aggregateFunction -> !aggregateFunction.getSortItems().isEmpty());
    }

    public static Property<AggregateFunction, ?, Boolean> distinct()
    {
        return Property.property("distinct", AggregateFunction::isDistinct);
    }

    public static Property<AggregateFunction, ?, Boolean> hasFilter()
    {
        return Property.property("hasFilter", aggregateFunction -> aggregateFunction.getFilter().isPresent());
    }

    public static Pattern<Variable> variable()
    {
        return Pattern.typeOf(Variable.class);
    }

    public static Pattern<List<Variable>> variables()
    {
        return new Pattern<>(Optional.empty())
        {
            @Override
            public <C> Stream<Match> accept(Object object, Captures captures, C context)
            {
                if (!(object instanceof List)) {
                    return Stream.of();
                }
                @SuppressWarnings("unchecked")
                List<ConnectorExpression> arguments = (List<ConnectorExpression>) object;
                if (!arguments.stream().allMatch(Variable.class::isInstance)) {
                    return Stream.of();
                }
                return Stream.of(Match.of(captures));
            }

            @Override
            public void accept(PatternVisitor patternVisitor)
            {
            }
        };
    }

    public static Property<ConnectorExpression, ?, Type> expressionType()
    {
        return Property.property("type", ConnectorExpression::getType);
    }

    public static Predicate<List<? extends ConnectorExpression>> expressionTypes(Type... types)
    {
        List<Type> expectedTypes = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        return expressions -> expectedTypes.equals(expressions.stream()
                .map(ConnectorExpression::getType)
                .collect(toImmutableList()));
    }
}
