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

import io.trino.spi.Unstable;

public enum OperatorType
{
    ADD("+", 2, false),
    SUBTRACT("-", 2, false),
    MULTIPLY("*", 2, false),
    DIVIDE("/", 2, false),
    MODULO("%", 2, false),
    NEGATION("-", 1, false),
    EQUAL("=", 2, true),
    /**
     * Normal comparison operator, but unordered values such as NaN are placed after all normal values.
     */
    COMPARISON_UNORDERED_LAST("COMPARISON_UNORDERED_LAST", 2, true),
    /**
     * Normal comparison operator, but unordered values such as NaN are placed before all normal values.
     */
    COMPARISON_UNORDERED_FIRST("COMPARISON_UNORDERED_FIRST", 2, true),
    LESS_THAN("<", 2, true),
    LESS_THAN_OR_EQUAL("<=", 2, true),
    CAST("CAST", 1, false),
    SUBSCRIPT("[]", 2, false),
    HASH_CODE("HASH CODE", 1, true),
    SATURATED_FLOOR_CAST("SATURATED FLOOR CAST", 1, false),
    IDENTICAL("IDENTICAL", 2, true),
    XX_HASH_64("XX HASH 64", 1, true),
    INDETERMINATE("INDETERMINATE", 1, true),
    READ_VALUE("READ VALUE", 1, true),
    /**/;

    private final String operator;
    private final int argumentCount;
    private final boolean neverFails;

    OperatorType(String operator, int argumentCount, boolean neverFails)
    {
        this.operator = operator;
        this.argumentCount = argumentCount;
        this.neverFails = neverFails;
    }

    public String getOperator()
    {
        return operator;
    }

    public int getArgumentCount()
    {
        return argumentCount;
    }

    @Unstable
    public boolean neverFails()
    {
        return neverFails;
    }
}
