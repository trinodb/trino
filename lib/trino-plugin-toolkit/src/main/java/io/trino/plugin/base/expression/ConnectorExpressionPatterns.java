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
package io.trino.plugin.base.expression;

import com.google.common.collect.ImmutableList;
import io.trino.matching.Pattern;
import io.trino.matching.Property;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorCast;
import io.trino.spi.expression.ConnectorComparison;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.ConnectorLogicalExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class ConnectorExpressionPatterns
{
    private ConnectorExpressionPatterns() {}

    public static Pattern<ConnectorExpression> expression()
    {
        return Pattern.typeOf(ConnectorExpression.class);
    }

    public static Property<ConnectorExpression, ?, Type> type()
    {
        return Property.property("type", ConnectorExpression::getType);
    }

    public static Pattern<Call> call()
    {
        return Pattern.typeOf(Call.class);
    }

    public static Pattern<ConnectorCast> connectorCast()
    {
        return Pattern.typeOf(ConnectorCast.class);
    }

    public static Pattern<ConnectorLogicalExpression> connectorLogicalExpression()
    {
        return Pattern.typeOf(ConnectorLogicalExpression.class);
    }

    public static Property<ConnectorLogicalExpression, ?, List<? extends ConnectorExpression>> terms()
    {
        return Property.property("terms", ConnectorLogicalExpression::getTerms);
    }

    public static Pattern<ConnectorComparison> connectorComparison()
    {
        return Pattern.typeOf(ConnectorComparison.class);
    }

    // TODO rename to `name()` if https://github.com/trinodb/trino/pull/7994/files#r806611558 is dismissed
    public static Property<Call, ?, String> functionName()
    {
        return Property.optionalProperty("functionName", call -> {
            FunctionName qualifiedFunctionName = call.getFunctionName();
            if (qualifiedFunctionName.getCatalogSchemaName().isPresent()) {
                return Optional.empty();
            }
            return Optional.of(qualifiedFunctionName.getFunctionName());
        });
    }

    public static Property<Call, ?, Integer> argumentCount()
    {
        return Property.property("argumentCount", call -> call.getArguments().size());
    }

    public static Property<Call, ?, ConnectorExpression> argument(int argument)
    {
        checkArgument(0 <= argument, "Invalid argument index: %s", argument);
        return Property.optionalProperty(format("argument(%s)", argument), call -> {
            if (argument < call.getArguments().size()) {
                return Optional.of(call.getArguments().get(argument));
            }
            return Optional.empty();
        });
    }

    public static Property<ConnectorCast, ?, ConnectorExpression> castExpression()
    {
        return Property.optionalProperty("expression", cast -> Optional.of(cast.getExpression()));
    }

    public static Property<ConnectorComparison, ?, ConnectorExpression> comparisonLeft()
    {
        return Property.optionalProperty("comparisonLeft", comparison -> Optional.of(comparison.getLeft()));
    }

    public static Property<ConnectorComparison, ?, ConnectorExpression> comparisonRight()
    {
        return Property.optionalProperty("comparisonRight", ConnectorComparison::getRight);
    }

    public static Property<Call, ?, Type> argumentType(int argument)
    {
        checkArgument(0 <= argument, "Invalid argument index: %s", argument);
        return Property.optionalProperty(format("argumentType(%s)", argument), call -> {
            if (argument < call.getArguments().size()) {
                return Optional.of(call.getArguments().get(argument).getType());
            }
            return Optional.empty();
        });
    }

    public static Property<Call, ?, List<Type>> argumentTypes()
    {
        return Property.property("argumentTypes", call -> call.getArguments().stream()
                .map(ConnectorExpression::getType)
                .collect(toImmutableList()));
    }

    public static Pattern<Constant> constant()
    {
        return Pattern.typeOf(Constant.class);
    }

    public static Pattern<Variable> variable()
    {
        return Pattern.typeOf(Variable.class);
    }

    public static Predicate<List<? extends ConnectorExpression>> expressionTypes(Type... types)
    {
        List<Type> expectedTypes = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        return expressions -> expectedTypes.equals(expressions.stream()
                .map(ConnectorExpression::getType)
                .collect(toImmutableList()));
    }
}
