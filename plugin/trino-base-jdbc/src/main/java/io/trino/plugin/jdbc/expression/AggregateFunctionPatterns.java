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
package io.prestosql.plugin.jdbc.expression;

import io.prestosql.matching.Pattern;
import io.prestosql.matching.Property;
import io.prestosql.spi.connector.AggregateFunction;
import io.prestosql.spi.expression.ConnectorExpression;
import io.prestosql.spi.expression.Variable;
import io.prestosql.spi.type.Type;

import java.util.List;
import java.util.Optional;

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

    public static Property<AggregateFunction, ?, List<ConnectorExpression>> inputs()
    {
        return Property.property("inputs", AggregateFunction::getInputs);
    }

    public static Property<AggregateFunction, ?, ConnectorExpression> singleInput()
    {
        return Property.optionalProperty("inputs", aggregateFunction -> {
            List<ConnectorExpression> inputs = aggregateFunction.getInputs();
            if (inputs.size() != 1) {
                return Optional.empty();
            }
            return Optional.of(inputs.get(0));
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

    public static Property<ConnectorExpression, ?, Type> expressionType()
    {
        return Property.property("type", ConnectorExpression::getType);
    }
}
