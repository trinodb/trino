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
package io.trino.plugin.tpch.statistics;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public record ColumnStatisticsData(
        Optional<Long> distinctValuesCount,
        Optional<Object> min,
        Optional<Object> max,
        Optional<Long> dataSize)
{
    public ColumnStatisticsData
    {
        requireNonNull(distinctValuesCount, "distinctValuesCount is null");
        requireNonNull(min, "min is null");
        requireNonNull(max, "max is null");
        requireNonNull(dataSize, "dataSize is null");
    }

    public static ColumnStatisticsData empty()
    {
        return new ColumnStatisticsData(Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
    }

    public static ColumnStatisticsData zero()
    {
        return new ColumnStatisticsData(Optional.of(0L), Optional.empty(), Optional.empty(), Optional.of(0L));
    }
}
