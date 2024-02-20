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
package io.trino.plugin.jdbc.expression;

import io.trino.spi.expression.FunctionName;

import java.util.Map;
import java.util.stream.Stream;

import static com.google.common.base.Verify.verifyNotNull;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.spi.expression.StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.GREATER_THAN_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.IS_DISTINCT_FROM_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.LESS_THAN_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.NOT_EQUAL_OPERATOR_FUNCTION_NAME;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

public enum ComparisonOperator
{
    EQUAL(EQUAL_OPERATOR_FUNCTION_NAME, "="),
    NOT_EQUAL(NOT_EQUAL_OPERATOR_FUNCTION_NAME, "<>"),
    LESS_THAN(LESS_THAN_OPERATOR_FUNCTION_NAME, "<"),
    LESS_THAN_OR_EQUAL(LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME, "<="),
    GREATER_THAN(GREATER_THAN_OPERATOR_FUNCTION_NAME, ">"),
    GREATER_THAN_OR_EQUAL(GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME, ">="),
    IS_DISTINCT_FROM(IS_DISTINCT_FROM_OPERATOR_FUNCTION_NAME, "IS DISTINCT FROM"),
    /**/;

    private final FunctionName functionName;
    private final String operator;

    private static final Map<FunctionName, ComparisonOperator> OPERATOR_BY_FUNCTION_NAME = Stream.of(values())
            .collect(toImmutableMap(ComparisonOperator::getFunctionName, identity()));

    ComparisonOperator(FunctionName functionName, String operator)
    {
        this.functionName = requireNonNull(functionName, "functionName is null");
        this.operator = requireNonNull(operator, "operator is null");
    }

    public FunctionName getFunctionName()
    {
        return functionName;
    }

    public String getOperator()
    {
        return operator;
    }

    public static ComparisonOperator forFunctionName(FunctionName functionName)
    {
        return verifyNotNull(OPERATOR_BY_FUNCTION_NAME.get(functionName), "Function name not recognized: %s", functionName);
    }
}
