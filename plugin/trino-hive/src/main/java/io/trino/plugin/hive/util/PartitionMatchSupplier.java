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
package io.trino.plugin.hive.util;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HivePartition;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.TupleDomain;

import javax.annotation.Nullable;

import java.util.List;
import java.util.function.BooleanSupplier;

import static io.trino.plugin.hive.HivePartitionManager.partitionMatches;
import static java.util.Objects.requireNonNull;

public final class PartitionMatchSupplier
        implements BooleanSupplier
{
    private final DynamicFilter dynamicFilter;
    private final HivePartition hivePartition;
    private final List<HiveColumnHandle> partitionColumns;

    @Nullable
    private volatile Boolean finalResult; // value is null until the dynamic filter no longer needs to be evaluated

    private PartitionMatchSupplier(DynamicFilter dynamicFilter, HivePartition hivePartition, List<HiveColumnHandle> partitionColumns)
    {
        this.dynamicFilter = requireNonNull(dynamicFilter, "dynamicFilter is null");
        this.hivePartition = requireNonNull(hivePartition, "hivePartition is null");
        this.partitionColumns = ImmutableList.copyOf(requireNonNull(partitionColumns, "partitionColumns is null"));
    }

    @Override
    public boolean getAsBoolean()
    {
        // Check for a final memoized result
        Boolean result = this.finalResult;
        if (result != null) {
            return result;
        }
        return evaluateCurrentDynamicFilter();
    }

    private boolean evaluateCurrentDynamicFilter()
    {
        // Must be checked before calling dynamicFilter.getCurrentPredicate()
        boolean filterIsComplete = dynamicFilter.isComplete();
        TupleDomain<ColumnHandle> currentPredicate = dynamicFilter.getCurrentPredicate();
        boolean partitionMatches = partitionMatches(partitionColumns, currentPredicate, hivePartition);
        if (!partitionMatches || filterIsComplete) {
            // Store the result to avoid re-evaluating the filter for each subsequent split
            this.finalResult = (Boolean) partitionMatches;
        }
        return partitionMatches;
    }

    public static BooleanSupplier createPartitionMatchSupplier(DynamicFilter dynamicFilter, HivePartition hivePartition, List<HiveColumnHandle> partitionColumns)
    {
        requireNonNull(dynamicFilter, "dynamicFilter is null");
        requireNonNull(hivePartition, "hivePartition is null");
        requireNonNull(partitionColumns, "partitionColumns is null");

        if (partitionColumns.stream().noneMatch(dynamicFilter.getColumnsCovered()::contains)) {
            return new BooleanValueSupplier(true);
        }
        if (dynamicFilter.isComplete()) {
            // Evaluate the dynamic filter once and use the resulting value
            return new BooleanValueSupplier(partitionMatches(partitionColumns, dynamicFilter.getCurrentPredicate(), hivePartition));
        }
        return new PartitionMatchSupplier(dynamicFilter, hivePartition, partitionColumns);
    }

    // note: this class is defined explicitly instead of with a method lambda so that usage sites of these
    // BooleanSupplier instances can be bi-morphic and not accidentally megamorphic
    private static final class BooleanValueSupplier
            implements BooleanSupplier
    {
        private final boolean value;

        private BooleanValueSupplier(boolean value)
        {
            this.value = value;
        }

        @Override
        public boolean getAsBoolean()
        {
            return value;
        }
    }
}
