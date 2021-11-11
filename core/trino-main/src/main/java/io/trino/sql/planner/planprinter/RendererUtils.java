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
package io.trino.sql.planner.planprinter;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;

import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.POSITIVE_INFINITY;
import static java.lang.Double.isFinite;
import static java.lang.Double.isNaN;
import static java.lang.String.format;

public final class RendererUtils
{
    private RendererUtils() {}

    public static String formatAsLong(double value)
    {
        if (isFinite(value)) {
            return format(Locale.US, "%d", Math.round(value));
        }

        return "?";
    }

    public static String formatAsCpuCost(double value)
    {
        return formatAsDataSize(value).replaceAll("B$", "");
    }

    public static String formatAsDataSize(double value)
    {
        if (isNaN(value)) {
            return "?";
        }
        if (value == POSITIVE_INFINITY) {
            return "+\u221E";
        }
        if (value == NEGATIVE_INFINITY) {
            return "-\u221E";
        }

        return DataSize.succinctBytes(Math.round(value)).toString();
    }

    public static String formatDouble(double value)
    {
        if (isFinite(value)) {
            return format(Locale.US, "%.2f", value);
        }

        return "?";
    }

    public static String formatPositions(long positions)
    {
        {
            String noun = (positions == 1) ? "row" : "rows";
            return positions + " " + noun;
        }
    }

    public static Map<String, String> translateOperatorTypes(Set<String> operators)
    {
        if (operators.size() == 1) {
            // don't display operator (plan node) name again
            return ImmutableMap.of(getOnlyElement(operators), "");
        }

        if (operators.contains("LookupJoinOperator") && operators.contains("HashBuilderOperator")) {
            // join plan node
            return ImmutableMap.of(
                    "LookupJoinOperator", "Left (probe) ",
                    "HashBuilderOperator", "Right (build) ");
        }

        if (operators.contains("LookupJoinOperator") && operators.contains("DynamicFilterSourceOperator")) {
            // join plan node
            return ImmutableMap.of(
                    "LookupJoinOperator", "Left (probe) ",
                    "DynamicFilterSourceOperator", "Right (build) ");
        }

        return ImmutableMap.of();
    }
}
