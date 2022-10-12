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
package io.trino.spi.function;

public enum OperatorType
{
    ADD("+", 2),
    SUBTRACT("-", 2),
    MULTIPLY("*", 2),
    DIVIDE("/", 2),
    MODULUS("%", 2),
    NEGATION("-", 1),
    EQUAL("=", 2),
    /**
     * Normal comparison operator, but unordered values such as NaN are placed after all normal values.
     */
    COMPARISON_UNORDERED_LAST("COMPARISON_UNORDERED_LAST", 2),
    /**
     * Normal comparison operator, but unordered values such as NaN are placed before all normal values.
     */
    COMPARISON_UNORDERED_FIRST("COMPARISON_UNORDERED_FIRST", 2),
    LESS_THAN("<", 2),
    LESS_THAN_OR_EQUAL("<=", 2),
    CAST("CAST", 1),
    SUBSCRIPT("[]", 2),
    HASH_CODE("HASH CODE", 1),
    SATURATED_FLOOR_CAST("SATURATED FLOOR CAST", 1),
    IS_DISTINCT_FROM("IS DISTINCT FROM", 2),
    XX_HASH_64("XX HASH 64", 1),
    INDETERMINATE("INDETERMINATE", 1),
    READ_VALUE("READ VALUE", 1),
    /**/;

    private final String operator;
    private final int argumentCount;

    OperatorType(String operator, int argumentCount)
    {
        this.operator = operator;
        this.argumentCount = argumentCount;
    }

    public String getOperator()
    {
        return operator;
    }

    public int getArgumentCount()
    {
        return argumentCount;
    }
}
