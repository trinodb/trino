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
package io.trino.spi.expression;

public final class StandardFunctions
{
    private StandardFunctions() {}

    /**
     * $and is a vararg function accepting boolean arguments
     */
    public static final FunctionName AND_FUNCTION_NAME = new FunctionName("$and");

    /**
     * $or is a vararg function accepting boolean arguments
     */
    public static final FunctionName OR_FUNCTION_NAME = new FunctionName("$or");

    /**
     * $not is a function accepting boolean argument
     */
    public static final FunctionName NOT_FUNCTION_NAME = new FunctionName("$not");

    public static final FunctionName IS_NULL_FUNCTION_NAME = new FunctionName("$is_null");
    /**
     * $nullif is a function accepting two arguments. Returns null if both values are the same, otherwise returns the first value.
     */
    public static final FunctionName NULLIF_FUNCTION_NAME = new FunctionName("$nullif");

    public static final FunctionName EQUAL_OPERATOR_FUNCTION_NAME = new FunctionName("$equal");
    public static final FunctionName NOT_EQUAL_OPERATOR_FUNCTION_NAME = new FunctionName("$not_equal");
    public static final FunctionName LESS_THAN_OPERATOR_FUNCTION_NAME = new FunctionName("$less_than");
    public static final FunctionName LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME = new FunctionName("$less_than_or_equal");
    public static final FunctionName GREATER_THAN_OPERATOR_FUNCTION_NAME = new FunctionName("$greater_than");
    public static final FunctionName GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME = new FunctionName("$greater_than_or_equal");
    public static final FunctionName IS_DISTINCT_FROM_OPERATOR_FUNCTION_NAME = new FunctionName("$is_distinct_from");

    /**
     * Arithmetic addition.
     */
    public static final FunctionName ADD_FUNCTION_NAME = new FunctionName("$add");

    /**
     * Arithmetic subtraction.
     */
    public static final FunctionName SUBTRACT_FUNCTION_NAME = new FunctionName("$subtract");

    /**
     * Arithmetic multiplication.
     */
    public static final FunctionName MULTIPLY_FUNCTION_NAME = new FunctionName("$multiply");

    /**
     * Arithmetic division.
     */
    public static final FunctionName DIVIDE_FUNCTION_NAME = new FunctionName("$divide");

    /**
     * Arithmetic modulus.
     */
    public static final FunctionName MODULUS_FUNCTION_NAME = new FunctionName("$modulus");

    /**
     * Arithmetic unary minus.
     */
    public static final FunctionName NEGATE_FUNCTION_NAME = new FunctionName("$negate");

    /**
     * $if is a function accepting 2 arguments - condition and trueValue, or 3 arguments - condition, trueValue and falseValue.
     */
    public static final FunctionName IF_FUNCTION_NAME = new FunctionName("$if");

    public static final FunctionName LIKE_PATTERN_FUNCTION_NAME = new FunctionName("$like_pattern");
}
