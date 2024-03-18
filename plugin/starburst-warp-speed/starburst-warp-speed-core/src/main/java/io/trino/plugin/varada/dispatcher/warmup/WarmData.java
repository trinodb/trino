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
package io.trino.plugin.varada.dispatcher.warmup;

import com.google.common.collect.SetMultimap;
import io.trino.plugin.varada.dispatcher.model.VaradaColumn;
import io.trino.plugin.varada.dispatcher.model.WarmUpElement;
import io.trino.plugin.varada.dispatcher.query.QueryContext;
import io.trino.spi.connector.ColumnHandle;

import java.util.List;

import static java.util.Objects.requireNonNull;

public record WarmData(
        List<ColumnHandle> columnHandleList,
        SetMultimap<VaradaColumn, WarmupProperties> requiredWarmUpTypeMap,
        WarmExecutionState warmExecutionState,
        boolean txMemoryReserved,
        double highestPriority,
        QueryContext queryContext,
        List<WarmUpElement> warmWarmUpElements)
{
    public WarmData(List<ColumnHandle> columnHandleList,
            SetMultimap<VaradaColumn, WarmupProperties> requiredWarmUpTypeMap,
            WarmExecutionState warmExecutionState,
            boolean txMemoryReserved,
            QueryContext queryContext,
            List<WarmUpElement> warmWarmUpElements)
    {
        this(columnHandleList,
                requiredWarmUpTypeMap,
                warmExecutionState,
                txMemoryReserved,
                requiredWarmUpTypeMap.values()
                        .stream()
                        .mapToDouble(WarmupProperties::priority)
                        .max()
                        .orElse(0),
                queryContext,
                warmWarmUpElements);
    }

    public WarmData(List<ColumnHandle> columnHandleList,
            SetMultimap<VaradaColumn, WarmupProperties> requiredWarmUpTypeMap,
            WarmExecutionState warmExecutionState,
            boolean txMemoryReserved,
            double highestPriority,
            QueryContext queryContext,
            List<WarmUpElement> warmWarmUpElements)
    {
        this.columnHandleList = requireNonNull(columnHandleList);
        this.requiredWarmUpTypeMap = requireNonNull(requiredWarmUpTypeMap);
        this.warmExecutionState = requireNonNull(warmExecutionState);
        this.txMemoryReserved = txMemoryReserved;
        this.highestPriority = highestPriority;
        this.queryContext = requireNonNull(queryContext);
        this.warmWarmUpElements = warmWarmUpElements != null ? warmWarmUpElements : List.of();
    }
}
