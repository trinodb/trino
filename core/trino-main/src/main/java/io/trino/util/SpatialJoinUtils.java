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

import io.trino.spi.function.CatalogSchemaFunctionName;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Expression;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.metadata.GlobalFunctionCatalog.builtinFunctionName;
import static io.trino.sql.ir.IrUtils.extractConjuncts;

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
    public static List<Call> extractSupportedSpatialFunctions(Expression filterExpression)
    {
        return extractConjuncts(filterExpression).stream()
                .filter(Call.class::isInstance)
                .map(Call.class::cast)
                .filter(SpatialJoinUtils::isSupportedSpatialFunction)
                .collect(toImmutableList());
    }

    private static boolean isSupportedSpatialFunction(Call call)
    {
        CatalogSchemaFunctionName functionName = call.function().name();
        return functionName.equals(builtinFunctionName(ST_CONTAINS)) ||
                functionName.equals(builtinFunctionName(ST_WITHIN)) ||
                functionName.equals(builtinFunctionName(ST_INTERSECTS));
    }

    /**
     * Returns a subset of conjuncts matching one the following shapes:
     * <ul>
     * <li>{@code ST_Distance(...) <= ...}</li>
     * <li>{@code ST_Distance(...) < ...}</li>
     * <li>{@code ... >= ST_Distance(...)}</li>
     * <li>{@code ... > ST_Distance(...)}</li>
     * </ul>
     * <p>
     * Doesn't check or guarantee anything about ST_Distance functions arguments
     * or the other side of the comparison.
     */
    public static List<Comparison> extractSupportedSpatialComparisons(Expression filterExpression)
    {
        return extractConjuncts(filterExpression).stream()
                .filter(Comparison.class::isInstance)
                .map(Comparison.class::cast)
                .filter(SpatialJoinUtils::isSupportedSpatialComparison)
                .collect(toImmutableList());
    }

    private static boolean isSupportedSpatialComparison(Comparison expression)
    {
        return switch (expression.operator()) {
            case LESS_THAN, LESS_THAN_OR_EQUAL -> isSTDistance(expression.left());
            case GREATER_THAN, GREATER_THAN_OR_EQUAL -> isSTDistance(expression.right());
            default -> false;
        };
    }

    private static boolean isSTDistance(Expression expression)
    {
        if (expression instanceof Call call) {
            return call.function().name().equals(builtinFunctionName(ST_DISTANCE));
        }

        return false;
    }
}
