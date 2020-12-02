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
package io.trino.plugin.hive.metastore.glue;

import io.trino.plugin.hive.metastore.HiveColumnStatistics;
import io.trino.plugin.hive.metastore.Partition;
import io.trino.plugin.hive.metastore.Table;
import io.trino.spi.statistics.ColumnStatisticType;
import io.trino.spi.type.Type;

import java.util.Map;
import java.util.Set;

public interface GlueColumnStatisticsProvider
{
    Set<ColumnStatisticType> getSupportedColumnStatistics(Type type);

    Map<String, HiveColumnStatistics> getTableColumnStatistics(Table table);

    Map<String, HiveColumnStatistics> getPartitionColumnStatistics(Partition partition);

    void updateTableColumnStatistics(Table table, Map<String, HiveColumnStatistics> columnStatistics);

    void updatePartitionStatistics(Partition partition, Map<String, HiveColumnStatistics> columnStatistics);
}
