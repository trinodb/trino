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
import io.trino.plugin.hive.HivePartitionKey;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.Partition;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.hudi.HudiUtil;
import io.trino.spi.predicate.TupleDomain;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.hive.PartitionValueExtractor;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.Objects.isNull;

public class HudiPartitionInternalInfo
        extends HudiPartitionInfo
{
    private final List<Column> partitionColumns;
    private final PartitionValueExtractor partitionValueExtractor;

    public HudiPartitionInternalInfo(
            String relativePartitionPath, List<Column> partitionColumns,
            List<HiveColumnHandle> partitionColumnHandles,
            TupleDomain<HiveColumnHandle> constraintSummary,
            PartitionValueExtractor partitionValueExtractor,
            Table table)
    {
        super(table, partitionColumnHandles, constraintSummary);
        this.relativePartitionPath = relativePartitionPath;
        this.partitionColumns = partitionColumns;
        this.partitionValueExtractor = partitionValueExtractor;
    }

    @Override
    public String getRelativePartitionPath()
    {
        return relativePartitionPath;
    }

    @Override
    public String getHivePartitionName()
    {
        throw new HoodieException(
                "HudiPartitionInternalInfo::getHivePartitionName() should not be called");
    }

    @Override
    public List<HivePartitionKey> getHivePartitionKeys()
    {
        if (isNull(hivePartitionKeys)) {
            List<String> partitionValues =
                    partitionValueExtractor.extractPartitionValuesInPath(relativePartitionPath);
            hivePartitionKeys = HudiUtil.buildPartitionKeys(partitionColumns, partitionValues);
        }

        return hivePartitionKeys;
    }

    @Override
    public boolean doesMatchPredicates()
    {
        Map<String, String> partitionKeyValueMap =
                getHivePartitionKeys().stream().collect(Collectors.toMap(
                        HivePartitionKey::getName, HivePartitionKey::getValue));
        List<String> partitionValues = partitionColumns.stream()
                .map(column -> partitionKeyValueMap.get(column.getName()))
                .collect(Collectors.toList());
        return HudiUtil.doesPartitionMatchPredicates(
                table.getSchemaTableName(), relativePartitionPath, partitionValues,
                partitionColumnHandles, constraintSummary);
    }

    @Override
    public String getComparingKey()
    {
        return relativePartitionPath;
    }

    @Override
    public void loadPartitionInfo(Optional<Partition> partition)
    {
        throw new HoodieException(
                "HudiPartitionInternalInfo::loadPartitionInfo() should not be called");
    }

    @Override
    public String toString()
    {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("HudiPartitionInternalInfo{");
        stringBuilder.append("relativePartitionPath=");
        stringBuilder.append(relativePartitionPath);
        if (!isNull(hivePartitionKeys)) {
            stringBuilder.append(",hivePartitionKeys=");
            stringBuilder.append(hivePartitionKeys);
        }
        stringBuilder.append("}");
        return stringBuilder.toString();
    }
}
