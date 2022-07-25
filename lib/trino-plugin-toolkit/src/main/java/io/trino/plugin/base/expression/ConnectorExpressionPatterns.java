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
import io.trino.spi.expression.ConnectorExpression;
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

    /**
     * @see #functionUnqualifiedName()
     */
    public static Property<Call, ?, FunctionName> functionName()
    {
        return Property.property("functionName", Call::getFunctionName);
    }

    /**
     * @see #functionName()
     */
    public static Property<Call, ?, String> functionUnqualifiedName()
    {
        return Property.optionalProperty("functionUnqualifiedName", call -> {
            FunctionName functionName = call.getFunctionName();
            if (functionName.getCatalogSchema().isPresent()) {
                // The name is qualified.
                return Optional.empty();
            }
            return Optional.of(functionName.getName());
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
