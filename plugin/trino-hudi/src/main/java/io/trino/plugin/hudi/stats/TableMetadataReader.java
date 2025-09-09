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
package io.trino.plugin.hudi.stats;

import org.apache.hudi.avro.model.HoodieMetadataColumnStats;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieColumnRangeMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.common.util.hash.ColumnIndexID;
import org.apache.hudi.common.util.hash.FileIndexID;
import org.apache.hudi.common.util.hash.PartitionIndexID;
import org.apache.hudi.exception.HoodieMetadataException;
import org.apache.hudi.metadata.HoodieBackedTableMetadata;
import org.apache.hudi.metadata.HoodieMetadataMetrics;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.storage.HoodieStorage;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Reads metadata efficiently from a Hudi metadata table.
 */
public class TableMetadataReader
        extends HoodieBackedTableMetadata
{
    TableMetadataReader(HoodieEngineContext engineContext, HoodieStorage storage,
                               HoodieMetadataConfig metadataConfig, String datasetBasePath, boolean reuse)
    {
        super(engineContext, storage, metadataConfig, datasetBasePath, reuse);
    }

    /**
     * Retrieves column statistics for the specified partition and file names.
     *
     * @param partitionNameFileNameList a list of partition and file name pairs for which column statistics are retrieved
     * @param columnNames a list of column names for which statistics are needed
     * @return a map from column name to their corresponding {@link HoodieColumnRangeMetadata}
     * @throws HoodieMetadataException if an error occurs while fetching the column statistics
     */
    public Map<String, HoodieColumnRangeMetadata> getColumnsRange(List<Pair<String, String>> partitionNameFileNameList, List<String> columnNames)
            throws HoodieMetadataException
    {
        Map<Pair<String, String>, List<HoodieMetadataColumnStats>> columnStatsMap = getColumnStats(partitionNameFileNameList, columnNames);
        return columnStatsMap.values().stream().flatMap(Collection::stream).collect(Collectors.groupingBy(
                HoodieMetadataColumnStats::getColumnName,
                Collectors.mapping(colStats -> colStats, Collectors.toList())))
                .entrySet().stream()
            .collect(Collectors.toMap(
                    Map.Entry::getKey,
                    e -> {
                        long valueCount = 0L;
                        long nullCount = 0L;
                        long totalSize = 0L;
                        long totalUncompressedSize = 0L;
                        for (HoodieMetadataColumnStats stats : e.getValue()) {
                            valueCount += stats.getValueCount();
                            nullCount += stats.getNullCount();
                            totalSize += stats.getTotalSize();
                            totalUncompressedSize += stats.getTotalUncompressedSize();
                        }
                        return HoodieColumnRangeMetadata.create(
                                "", e.getKey(), null, null, nullCount, valueCount, totalSize, totalUncompressedSize);
                    }));
    }
}
