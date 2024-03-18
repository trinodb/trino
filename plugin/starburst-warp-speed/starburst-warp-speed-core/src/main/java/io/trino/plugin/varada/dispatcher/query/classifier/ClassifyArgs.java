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
package io.trino.plugin.varada.dispatcher.query.classifier;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.varada.dispatcher.DispatcherTableHandle;
import io.trino.plugin.varada.dispatcher.model.RowGroupData;
import io.trino.plugin.warp.gen.constants.PredicateType;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.type.Type;

import java.util.HashMap;
import java.util.Map;

class ClassifyArgs
{
    private final DispatcherTableHandle dispatcherTableHandle;
    private final RowGroupData rowGroupData;
    private final PredicateContextData predicateContextData;
    private final ImmutableMap<Integer, ColumnHandle> collectColumnsByBlockIndex;
    private final WarmedWarmupTypes warmedWarmupTypes;
    private final boolean minMaxFilter;
    private final boolean mappedMatchCollect;
    private final boolean enableInverseWithNulls;

    private final Map<Domain, PredicateType> predicateTypeCache = new HashMap<>();

    ClassifyArgs(DispatcherTableHandle dispatcherTableHandle,
                 RowGroupData rowGroupData,
                 PredicateContextData predicateContextData,
                 ImmutableMap<Integer, ColumnHandle> collectColumnsByBlockIndex,
                 WarmedWarmupTypes warmedWarmupTypes,
                 boolean minMaxFilter,
                 boolean mappedMatchCollect,
                 boolean enableInverseWithNulls)
    {
        this.dispatcherTableHandle = dispatcherTableHandle;
        this.rowGroupData = rowGroupData;
        this.predicateContextData = predicateContextData;
        this.collectColumnsByBlockIndex = collectColumnsByBlockIndex;
        this.warmedWarmupTypes = warmedWarmupTypes;
        this.minMaxFilter = minMaxFilter;
        this.mappedMatchCollect = mappedMatchCollect;
        this.enableInverseWithNulls = enableInverseWithNulls;
    }

    DispatcherTableHandle getDispatcherTableHandle()
    {
        return dispatcherTableHandle;
    }

    RowGroupData getRowGroupData()
    {
        return rowGroupData;
    }

    ImmutableMap<Integer, ColumnHandle> getCollectColumnsByBlockIndex()
    {
        return collectColumnsByBlockIndex;
    }

    ColumnHandle getCollectColumn(int blockIndex)
    {
        return collectColumnsByBlockIndex.get(blockIndex);
    }

    PredicateContextData getPredicateContextData()
    {
        return predicateContextData;
    }

    PredicateType getPredicateTypeFromCache(Domain domain, Type columnType)
    {
        return predicateTypeCache.computeIfAbsent(
                domain,
                d -> PredicateUtil.calcPredicateType(d, columnType));
    }

    public WarmedWarmupTypes getWarmedWarmupTypes()
    {
        return warmedWarmupTypes;
    }

    boolean isMinMaxFilter()
    {
        return minMaxFilter;
    }

    public boolean isMappedMatchCollect()
    {
        return mappedMatchCollect;
    }

    public boolean isEnableInverseWithNulls()
    {
        return enableInverseWithNulls;
    }
}
