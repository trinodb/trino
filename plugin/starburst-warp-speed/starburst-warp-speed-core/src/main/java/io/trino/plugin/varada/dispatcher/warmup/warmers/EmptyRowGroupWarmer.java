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
package io.trino.plugin.varada.dispatcher.warmup.warmers;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.trino.plugin.varada.dispatcher.model.RowGroupData;
import io.trino.plugin.varada.dispatcher.model.RowGroupKey;
import io.trino.plugin.varada.dispatcher.model.VaradaColumn;
import io.trino.plugin.varada.dispatcher.model.WarmUpElement;
import io.trino.plugin.varada.dispatcher.services.RowGroupDataService;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

@Singleton
public class EmptyRowGroupWarmer
{
    private final RowGroupDataService rowGroupDataService;

    @Inject
    public EmptyRowGroupWarmer(RowGroupDataService rowGroupDataService)
    {
        this.rowGroupDataService = requireNonNull(rowGroupDataService);
    }

    void warm(RowGroupKey rowGroupKey, List<WarmUpElement> newWarmUpElements)
    {
        RowGroupData rowGroupData = rowGroupDataService.get(rowGroupKey);
        rowGroupDataService.updateEmptyRowGroup(rowGroupData, newWarmUpElements, Collections.emptyList());
    }

    public void saveImportedEmptyRowGroup(List<WarmUpElement> newWarmupElements, RowGroupKey rowGroupKey, Map<VaradaColumn, String> partitionKeys)
    {
        List<WarmUpElement> updatedWarmupElements = newWarmupElements
                .stream()
                .map(emptyWeElement -> WarmUpElement.builder(emptyWeElement).totalRecords(0).build()).toList();
        rowGroupDataService.save(RowGroupData.builder()
                .isEmpty(true)
                .warmUpElements(updatedWarmupElements)
                .rowGroupKey(rowGroupKey)
                .partitionKeys(partitionKeys)
                .build());
    }
}
