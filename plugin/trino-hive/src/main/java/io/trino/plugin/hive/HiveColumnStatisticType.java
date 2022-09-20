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
package io.trino.plugin.hive;

import io.trino.spi.expression.FunctionName;

import java.util.Map;
import java.util.stream.Stream;

import static com.google.common.base.Verify.verifyNotNull;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.spi.expression.StandardFunctions.MAX_DATA_SIZE_FOR_STATS;
import static io.trino.spi.expression.StandardFunctions.SUM_DATA_SIZE_FOR_STATS;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

public enum HiveColumnStatisticType
{
    MIN_VALUE(new FunctionName("min")),
    MAX_VALUE(new FunctionName("max")),
    NUMBER_OF_DISTINCT_VALUES(new FunctionName("approx_distinct")),
    NUMBER_OF_NON_NULL_VALUES(new FunctionName("count")),
    NUMBER_OF_TRUE_VALUES(new FunctionName("count_if")),
    MAX_VALUE_SIZE_IN_BYTES(MAX_DATA_SIZE_FOR_STATS),
    TOTAL_SIZE_IN_BYTES(SUM_DATA_SIZE_FOR_STATS),
    /**/;

    private static final Map<FunctionName, HiveColumnStatisticType> byAggregationName = Stream.of(values())
            .collect(toImmutableMap(HiveColumnStatisticType::getAggregationName, identity()));

    public static HiveColumnStatisticType byAggregation(FunctionName aggregationName)
    {
        return verifyNotNull(byAggregationName.get(aggregationName), "No entry for %s", aggregationName);
    }

    private final FunctionName aggregationName;

    HiveColumnStatisticType(FunctionName aggregationName)
    {
        this.aggregationName = requireNonNull(aggregationName, "aggregationName is null");
    }

    public FunctionName getAggregationName()
    {
        return aggregationName;
    }
}
