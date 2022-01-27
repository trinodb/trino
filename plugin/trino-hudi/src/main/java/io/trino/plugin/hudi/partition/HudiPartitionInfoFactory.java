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

package io.trino.plugin.hudi.partition;

import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.Table;
import io.trino.spi.predicate.TupleDomain;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.hive.PartitionValueExtractor;

import java.util.List;

public final class HudiPartitionInfoFactory
{
    private HudiPartitionInfoFactory() {}

    public static HudiPartitionInfo get(
            boolean shouldSkipMetastoreForPartition,
            Option<String> relativePartitionPath, Option<String> hivePartitionName,
            Option<PartitionValueExtractor> partitionValueExtractor,
            List<Column> partitionColumns, List<HiveColumnHandle> partitionColumnHandles,
            TupleDomain<HiveColumnHandle> constraintSummary,
            Table table, HiveMetastore hiveMetastore)
    {
        if (shouldSkipMetastoreForPartition) {
            return new HudiPartitionInternalInfo(
                    relativePartitionPath.get(), partitionColumns, partitionColumnHandles,
                    constraintSummary, partitionValueExtractor.get(), table);
        }
        return new HudiPartitionHiveInfo(
                hivePartitionName.get(), partitionColumns, partitionColumnHandles,
                constraintSummary, table, hiveMetastore);
    }
}
