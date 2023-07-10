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
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;

import java.util.Arrays;
import java.util.List;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.spi.expression.Constant.TRUE;
import static io.trino.spi.expression.StandardFunctions.AND_FUNCTION_NAME;
import static io.trino.spi.type.BooleanType.BOOLEAN;

public final class ConnectorExpressions
{
    private ConnectorExpressions() {}

    public static List<ConnectorExpression> extractConjuncts(ConnectorExpression expression)
    {
        ImmutableList.Builder<ConnectorExpression> resultBuilder = ImmutableList.builder();
        extractConjuncts(expression, resultBuilder);
        return resultBuilder.build();
    }

    private static void extractConjuncts(ConnectorExpression expression, ImmutableList.Builder<ConnectorExpression> resultBuilder)
    {
        if (expression instanceof Call call) {
            if (AND_FUNCTION_NAME.equals(call.getFunctionName())) {
                for (ConnectorExpression argument : call.getArguments()) {
                    extractConjuncts(argument, resultBuilder);
                }
                return;
            }
        }
        resultBuilder.add(expression);
    }

    public static ConnectorExpression and(ConnectorExpression... expressions)
    {
        return and(Arrays.asList(expressions));
    }

    public static ConnectorExpression and(List<ConnectorExpression> expressions)
    {
        if (expressions.size() > 1) {
            return new Call(BOOLEAN, AND_FUNCTION_NAME, expressions);
        }
        if (expressions.isEmpty()) {
            return TRUE;
        }
        return getOnlyElement(expressions);
    }
}
