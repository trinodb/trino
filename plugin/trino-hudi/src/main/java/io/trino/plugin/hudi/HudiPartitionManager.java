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

package io.trino.plugin.hudi;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.Table;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.type.TypeManager;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.base.Verify.verify;
import static io.trino.plugin.hive.metastore.MetastoreUtil.computePartitionKeyFilter;
import static io.trino.plugin.hive.util.HiveUtil.getPartitionKeyColumnHandles;
import static java.util.Objects.requireNonNull;

public class HudiPartitionManager
{
    private final TypeManager typeManager;

    @Inject
    public HudiPartitionManager(TypeManager typeManager)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    public List<String> getEffectivePartitions(HudiTableHandle tableHandle, HiveMetastore metastore)
    {
        Optional<Table> table = metastore.getTable(tableHandle.getSchemaName(), tableHandle.getTableName());
        verify(table.isPresent());
        List<Column> partitionColumns = table.get().getPartitionColumns();
        if (partitionColumns.isEmpty()) {
            return ImmutableList.of("");
        }

        List<HiveColumnHandle> partitionColumnHandles = getPartitionKeyColumnHandles(table.get(), typeManager);

        return metastore.getPartitionNamesByFilter(
                        tableHandle.getSchemaName(),
                        tableHandle.getTableName(),
                        partitionColumns.stream().map(Column::getName).collect(Collectors.toList()),
                        computePartitionKeyFilter(partitionColumnHandles, tableHandle.getPartitionPredicates()))
                .orElseThrow(() -> new TableNotFoundException(tableHandle.getSchemaTableName()));
    }
}
