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
package io.trino.util;

import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.metadata.ResolvedFunction.extractFunctionName;
import static io.trino.sql.ExpressionUtils.extractConjuncts;

public final class SpatialJoinUtils
{
    public static final String ST_CONTAINS = "st_contains";
    public static final String ST_WITHIN = "st_within";
    public static final String ST_INTERSECTS = "st_intersects";
    public static final String ST_DISTANCE = "st_distance";

    private SpatialJoinUtils() {}

    /**
     * Returns a subset of conjuncts matching one of the following shapes:
     * - ST_Contains(...)
     * - ST_Within(...)
     * - ST_Intersects(...)
     * <p>
     * Doesn't check or guarantee anything about function arguments.
     */
    public static List<FunctionCall> extractSupportedSpatialFunctions(Expression filterExpression)
    {
        return extractConjuncts(filterExpression).stream()
                .filter(FunctionCall.class::isInstance)
                .map(FunctionCall.class::cast)
                .filter(SpatialJoinUtils::isSupportedSpatialFunction)
                .collect(toImmutableList());
    }

    private static boolean isSupportedSpatialFunction(FunctionCall functionCall)
    {
        String functionName = extractFunctionName(functionCall.getName());
        return functionName.equalsIgnoreCase(ST_CONTAINS) ||
                functionName.equalsIgnoreCase(ST_WITHIN) ||
                functionName.equalsIgnoreCase(ST_INTERSECTS);
    }

    /**
     * Returns a subset of conjuncts matching one the following shapes:
     * - ST_Distance(...) <= ...
     * - ST_Distance(...) < ...
     * - ... >= ST_Distance(...)
     * - ... > ST_Distance(...)
     * <p>
     * Doesn't check or guarantee anything about ST_Distance functions arguments
     * or the other side of the comparison.
     */
    public static List<ComparisonExpression> extractSupportedSpatialComparisons(Expression filterExpression)
    {
        return extractConjuncts(filterExpression).stream()
                .filter(ComparisonExpression.class::isInstance)
                .map(ComparisonExpression.class::cast)
                .filter(SpatialJoinUtils::isSupportedSpatialComparison)
                .collect(toImmutableList());
    }

    private static boolean isSupportedSpatialComparison(ComparisonExpression expression)
    {
        switch (expression.getOperator()) {
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
                return isSTDistance(expression.getLeft());
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
                return isSTDistance(expression.getRight());
            default:
                return false;
        }
    }

    private static boolean isSTDistance(Expression expression)
    {
        if (expression instanceof FunctionCall) {
            return extractFunctionName(((FunctionCall) expression).getName()).equalsIgnoreCase(ST_DISTANCE);
        }

        return false;
    }
}
