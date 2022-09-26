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

import io.trino.spi.statistics.ColumnStatisticMetadata;
import io.trino.spi.statistics.ColumnStatisticType;

import static java.util.Objects.requireNonNull;

public enum HiveColumnStatisticType
{
    MIN_VALUE(ColumnStatisticType.MIN_VALUE),
    MAX_VALUE(ColumnStatisticType.MAX_VALUE),
    NUMBER_OF_DISTINCT_VALUES(ColumnStatisticType.NUMBER_OF_DISTINCT_VALUES),
    NUMBER_OF_NON_NULL_VALUES(ColumnStatisticType.NUMBER_OF_NON_NULL_VALUES),
    NUMBER_OF_TRUE_VALUES(ColumnStatisticType.NUMBER_OF_TRUE_VALUES),
    MAX_VALUE_SIZE_IN_BYTES(ColumnStatisticType.MAX_VALUE_SIZE_IN_BYTES),
    TOTAL_SIZE_IN_BYTES(ColumnStatisticType.TOTAL_SIZE_IN_BYTES),
    /**/;

    private final ColumnStatisticType columnStatisticType;

    HiveColumnStatisticType(ColumnStatisticType columnStatisticType)
    {
        this.columnStatisticType = requireNonNull(columnStatisticType, "columnStatisticType is null");
    }

    public ColumnStatisticMetadata createColumnStatisticMetadata(String columnName)
    {
        return new ColumnStatisticMetadata(columnName, name(), columnStatisticType);
    }

    public static HiveColumnStatisticType from(ColumnStatisticMetadata columnStatisticMetadata)
    {
        return HiveColumnStatisticType.valueOf(columnStatisticMetadata.getConnectorAggregationId());
    }
}
