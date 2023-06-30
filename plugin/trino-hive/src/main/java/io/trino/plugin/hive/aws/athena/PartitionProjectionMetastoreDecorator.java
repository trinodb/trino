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

package io.trino.plugin.hive.aws.athena;

import com.google.inject.Inject;
import io.trino.plugin.hive.metastore.ForwardingHiveMetastore;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.HiveMetastoreDecorator;
import io.trino.plugin.hive.metastore.Partition;
import io.trino.plugin.hive.metastore.Table;
import io.trino.spi.TrinoException;
import io.trino.spi.predicate.TupleDomain;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.hive.HiveErrorCode.HIVE_TABLE_DROPPED_DURING_QUERY;
import static java.util.Objects.requireNonNull;

public class PartitionProjectionMetastoreDecorator
        implements HiveMetastoreDecorator
{
    private final PartitionProjectionService partitionProjectionService;

    @Inject
    public PartitionProjectionMetastoreDecorator(PartitionProjectionService partitionProjectionService)
    {
        this.partitionProjectionService = requireNonNull(partitionProjectionService, "partitionProjectionService is null");
    }

    @Override
    public int getPriority()
    {
        return PRIORITY_PARTITION_PROJECTION;
    }

    @Override
    public HiveMetastore decorate(HiveMetastore hiveMetastore)
    {
        return new PartitionProjectionMetastore(hiveMetastore, partitionProjectionService);
    }

    private static class PartitionProjectionMetastore
            extends ForwardingHiveMetastore
    {
        private final PartitionProjectionService partitionProjectionService;

        public PartitionProjectionMetastore(HiveMetastore hiveMetastore, PartitionProjectionService partitionProjectionService)
        {
            super(hiveMetastore);
            this.partitionProjectionService = requireNonNull(partitionProjectionService, "partitionProjectionService is null");
        }

        @Override
        public Optional<List<String>> getPartitionNamesByFilter(String databaseName, String tableName, List<String> columnNames, TupleDomain<String> partitionKeysFilter)
        {
            Table table = super.getTable(databaseName, tableName)
                    .orElseThrow(() -> new TrinoException(HIVE_TABLE_DROPPED_DURING_QUERY, "Table does not exists: " + tableName));

            Optional<PartitionProjection> projection = getPartitionProjection(table);
            if (projection.isPresent()) {
                return projection.get().getProjectedPartitionNamesByFilter(columnNames, partitionKeysFilter);
            }

            return super.getPartitionNamesByFilter(databaseName, tableName, columnNames, partitionKeysFilter);
        }

        @Override
        public Map<String, Optional<Partition>> getPartitionsByNames(Table table, List<String> partitionNames)
        {
            Optional<PartitionProjection> projection = getPartitionProjection(table);
            if (projection.isPresent()) {
                return projection.get().getProjectedPartitionsByNames(table, partitionNames);
            }
            return super.getPartitionsByNames(table, partitionNames);
        }

        private Optional<PartitionProjection> getPartitionProjection(Table table)
        {
            return partitionProjectionService.getPartitionProjectionFromTable(table)
                    .filter(PartitionProjection::isEnabled);
        }
    }
}
