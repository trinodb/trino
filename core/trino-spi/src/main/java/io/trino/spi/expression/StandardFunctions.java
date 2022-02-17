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

    public static final FunctionName EQUAL_OPERATOR_FUNCTION_NAME = new FunctionName("$equal");
    public static final FunctionName NOT_EQUAL_OPERATOR_FUNCTION_NAME = new FunctionName("$not_equal");
    public static final FunctionName LESS_THAN_OPERATOR_FUNCTION_NAME = new FunctionName("$less_than");
    public static final FunctionName LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME = new FunctionName("$less_than_or_equal");
    public static final FunctionName GREATER_THAN_OPERATOR_FUNCTION_NAME = new FunctionName("$greater_than");
    public static final FunctionName GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME = new FunctionName("$greater_than_or_equal");
    public static final FunctionName IS_DISTINCT_FROM_OPERATOR_FUNCTION_NAME = new FunctionName("$is_distinct_from");

    public static final FunctionName LIKE_PATTERN_FUNCTION_NAME = new FunctionName("$like_pattern");
}
