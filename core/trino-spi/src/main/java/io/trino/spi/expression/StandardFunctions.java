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

import io.trino.spi.type.ArrayType;

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

    /**
     * $cast function result type is determined by the {@link Call#getType()}
     */
    public static final FunctionName CAST_FUNCTION_NAME = new FunctionName("$cast");
    public static final FunctionName TRY_CAST_FUNCTION_NAME = new FunctionName("$try_cast");

    public static final FunctionName EQUAL_OPERATOR_FUNCTION_NAME = new FunctionName("$equal");
    public static final FunctionName NOT_EQUAL_OPERATOR_FUNCTION_NAME = new FunctionName("$not_equal");
    public static final FunctionName LESS_THAN_OPERATOR_FUNCTION_NAME = new FunctionName("$less_than");
    public static final FunctionName LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME = new FunctionName("$less_than_or_equal");
    public static final FunctionName GREATER_THAN_OPERATOR_FUNCTION_NAME = new FunctionName("$greater_than");
    public static final FunctionName GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME = new FunctionName("$greater_than_or_equal");
    public static final FunctionName IDENTICAL_OPERATOR_FUNCTION_NAME = new FunctionName("$identical");

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

    public static final FunctionName LIKE_FUNCTION_NAME = new FunctionName("$like");

    /**
     * {@code $in(value, array)} returns {@code true} when value is equal to an element of the array,
     * otherwise returns {@code NULL} when comparing value to an element of the array returns an
     * indeterminate result, otherwise returns {@code false}
     */
    public static final FunctionName IN_PREDICATE_FUNCTION_NAME = new FunctionName("$in");

    /**
     * $array creates instance of {@link ArrayType}
     */
    public static final FunctionName ARRAY_CONSTRUCTOR_FUNCTION_NAME = new FunctionName("$array");
}
