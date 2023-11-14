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

package io.trino.plugin.hive.statistics;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.hive.HivePartition;
import io.trino.plugin.hive.PartitionStatistics;
import io.trino.plugin.hive.metastore.SemiTransactionalHiveMetastore;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.hive.HivePartition.UNPARTITIONED_ID;
import static java.util.Objects.requireNonNull;

public class MetastoreHiveStatisticsProvider
        extends AbstractHiveStatisticsProvider
{
    private final SemiTransactionalHiveMetastore metastore;

    public MetastoreHiveStatisticsProvider(SemiTransactionalHiveMetastore metastore)
    {
        this.metastore = requireNonNull(metastore, "metastore is null");
    }

    @Override
    protected Map<String, PartitionStatistics> getPartitionsStatistics(ConnectorSession session, SchemaTableName table, List<HivePartition> hivePartitions, Set<String> columns)
    {
        if (hivePartitions.isEmpty()) {
            return ImmutableMap.of();
        }
        boolean unpartitioned = hivePartitions.stream().anyMatch(partition -> partition.getPartitionId().equals(UNPARTITIONED_ID));
        if (unpartitioned) {
            checkArgument(hivePartitions.size() == 1, "expected only one hive partition");
            return ImmutableMap.of(UNPARTITIONED_ID, metastore.getTableStatistics(table.getSchemaName(), table.getTableName(), Optional.of(columns)));
        }
        Set<String> partitionNames = hivePartitions.stream()
                .map(HivePartition::getPartitionId)
                .collect(toImmutableSet());
        return metastore.getPartitionStatistics(table.getSchemaName(), table.getTableName(), columns, partitionNames);
    }
}
