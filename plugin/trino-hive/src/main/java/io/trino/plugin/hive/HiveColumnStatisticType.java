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
import io.trino.spi.statistics.ColumnStatisticMetadata;
import io.trino.spi.statistics.ColumnStatisticType;

import java.util.Optional;

public enum HiveColumnStatisticType
{
    MIN_VALUE(new FunctionName("min")),
    MAX_VALUE(new FunctionName("max")),
    NUMBER_OF_DISTINCT_VALUES(new FunctionName("approx_distinct")),
    NUMBER_OF_NON_NULL_VALUES(new FunctionName("count")),
    NUMBER_OF_TRUE_VALUES(new FunctionName("count_if")),
    MAX_VALUE_SIZE_IN_BYTES(ColumnStatisticType.MAX_VALUE_SIZE_IN_BYTES),
    TOTAL_SIZE_IN_BYTES(ColumnStatisticType.TOTAL_SIZE_IN_BYTES),
    /**/;

    private final Optional<ColumnStatisticType> columnStatisticType;
    private final Optional<FunctionName> aggregationName;

    HiveColumnStatisticType(ColumnStatisticType columnStatisticType)
    {
        this.columnStatisticType = Optional.of(columnStatisticType);
        this.aggregationName = Optional.empty();
    }

    HiveColumnStatisticType(FunctionName aggregationName)
    {
        this.columnStatisticType = Optional.empty();
        this.aggregationName = Optional.of(aggregationName);
    }

    public ColumnStatisticMetadata createColumnStatisticMetadata(String columnName)
    {
        String connectorAggregationId = name();
        if (columnStatisticType.isPresent()) {
            return new ColumnStatisticMetadata(columnName, connectorAggregationId, columnStatisticType.get());
        }
        return new ColumnStatisticMetadata(columnName, connectorAggregationId, aggregationName.orElseThrow());
    }

    public static HiveColumnStatisticType from(ColumnStatisticMetadata columnStatisticMetadata)
    {
        return HiveColumnStatisticType.valueOf(columnStatisticMetadata.getConnectorAggregationId());
    }
}
