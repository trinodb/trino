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
package io.trino.plugin.varada.execution.debugtools;

import io.trino.plugin.varada.dispatcher.model.RowGroupKey;
import io.trino.plugin.varada.dispatcher.model.WarmUpElement;
import io.trino.plugin.varada.dispatcher.warmup.demoter.TupleFilter;
import io.trino.spi.connector.SchemaTableName;

import java.util.List;

public class ColumnFilter
        implements TupleFilter
{
    private final SchemaTableName schemaTableName;
    private final List<WarmupDemoterWarmupElementData> warmupElementsData;

    public ColumnFilter(SchemaTableName schemaTableName, List<WarmupDemoterWarmupElementData> warmupElementsData)
    {
        this.schemaTableName = schemaTableName;
        this.warmupElementsData = warmupElementsData;
    }

    @Override
    public boolean shouldHandle(WarmUpElement warmUpElement, RowGroupKey rowGroupKey)
    {
        if (schemaTableName == null) {
            return true;
        }
        if (!rowGroupKey.schema().equals(schemaTableName.getSchemaName()) || !rowGroupKey.table().equals(schemaTableName.getTableName())) {
            return false;
        }
        if (warmupElementsData == null || warmupElementsData.isEmpty()) {
            return true;
        }
        return warmupElementsData.stream()
                .filter(warmupElementsData -> warmupElementsData.columnName().equalsIgnoreCase(warmUpElement.getVaradaColumn().getName()))
                .anyMatch(warmupElementsData -> warmupElementsData.warmupTypes().isEmpty() || warmupElementsData.warmupTypes().contains(warmUpElement.getWarmUpType()));
    }

    @Override
    public String toString()
    {
        return "ColumnFilter{" + "schemaTableName=" + schemaTableName + ", warmupElementsData=" + warmupElementsData + '}';
    }
}
