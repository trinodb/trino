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

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.hive.metastore.HiveColumnStatistics;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public record PartitionStatistics(
        HiveBasicStatistics basicStatistics,
        Map<String, HiveColumnStatistics> columnStatistics)
{
    private static final PartitionStatistics EMPTY = new PartitionStatistics(HiveBasicStatistics.createEmptyStatistics(), ImmutableMap.of());

    public static PartitionStatistics empty()
    {
        return EMPTY;
    }

    public PartitionStatistics
    {
        requireNonNull(basicStatistics, "basicStatistics is null");
        columnStatistics = ImmutableMap.copyOf(requireNonNull(columnStatistics, "columnStatistics cannot be null"));
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public PartitionStatistics withBasicStatistics(HiveBasicStatistics basicStatistics)
    {
        return new PartitionStatistics(basicStatistics, columnStatistics);
    }

    public static class Builder
    {
        private HiveBasicStatistics basicStatistics = HiveBasicStatistics.createEmptyStatistics();
        private Map<String, HiveColumnStatistics> columnStatistics = ImmutableMap.of();

        public Builder setBasicStatistics(HiveBasicStatistics basicStatistics)
        {
            this.basicStatistics = requireNonNull(basicStatistics, "basicStatistics is null");
            return this;
        }

        public Builder setColumnStatistics(Map<String, HiveColumnStatistics> columnStatistics)
        {
            this.columnStatistics = ImmutableMap.copyOf(requireNonNull(columnStatistics, "columnStatistics is null"));
            return this;
        }

        public PartitionStatistics build()
        {
            return new PartitionStatistics(basicStatistics, columnStatistics);
        }
    }
}
