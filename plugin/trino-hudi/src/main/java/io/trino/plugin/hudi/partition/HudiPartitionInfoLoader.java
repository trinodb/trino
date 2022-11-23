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

import io.trino.plugin.hive.metastore.Partition;
import io.trino.plugin.hudi.query.HudiDirectoryLister;
import io.trino.spi.connector.ConnectorSession;
import org.apache.hudi.exception.HoodieIOException;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.stream.Collectors;

import static io.trino.plugin.hudi.HudiSessionProperties.getMaxPartitionBatchSize;
import static io.trino.plugin.hudi.HudiSessionProperties.getMinPartitionBatchSize;

public class HudiPartitionInfoLoader
        implements Runnable
{
    private final HudiDirectoryLister hudiDirectoryLister;
    private final int minPartitionBatchSize;
    private final int maxPartitionBatchSize;
    private final Deque<HudiPartitionInfo> partitionQueue;
    private int currentBatchSize;

    public HudiPartitionInfoLoader(
            ConnectorSession session,
            HudiDirectoryLister hudiDirectoryLister)
    {
        this.hudiDirectoryLister = hudiDirectoryLister;
        this.partitionQueue = new ConcurrentLinkedDeque<>();
        this.minPartitionBatchSize = getMinPartitionBatchSize(session);
        this.maxPartitionBatchSize = getMaxPartitionBatchSize(session);
        this.currentBatchSize = -1;
    }

    @Override
    public void run()
    {
        List<HudiPartitionInfo> hudiPartitionInfoList = hudiDirectoryLister.getPartitionsToScan().stream()
                .sorted(Comparator.comparing(HudiPartitionInfo::getComparingKey))
                .collect(Collectors.toList());

        // empty partitioned table
        if (hudiPartitionInfoList.isEmpty()) {
            return;
        }

        // non-partitioned table
        if (hudiPartitionInfoList.size() == 1 && hudiPartitionInfoList.get(0).getHivePartitionName().isEmpty()) {
            partitionQueue.addAll(hudiPartitionInfoList);
            return;
        }

        boolean shouldUseHiveMetastore = hudiPartitionInfoList.get(0) instanceof HiveHudiPartitionInfo;
        Iterator<HudiPartitionInfo> iterator = hudiPartitionInfoList.iterator();
        while (iterator.hasNext()) {
            int batchSize = updateBatchSize();
            List<HudiPartitionInfo> partitionInfoBatch = new ArrayList<>();
            while (iterator.hasNext() && batchSize > 0) {
                partitionInfoBatch.add(iterator.next());
                batchSize--;
            }

            if (!partitionInfoBatch.isEmpty()) {
                if (shouldUseHiveMetastore) {
                    Map<String, Optional<Partition>> partitions = hudiDirectoryLister.getPartitions(partitionInfoBatch.stream()
                            .map(HudiPartitionInfo::getHivePartitionName)
                            .collect(Collectors.toList()));
                    for (HudiPartitionInfo partitionInfo : partitionInfoBatch) {
                        String hivePartitionName = partitionInfo.getHivePartitionName();
                        if (!partitions.containsKey(hivePartitionName)) {
                            throw new HoodieIOException("Partition does not exist: " + hivePartitionName);
                        }
                        partitionInfo.loadPartitionInfo(partitions.get(hivePartitionName));
                        partitionQueue.add(partitionInfo);
                    }
                }
                else {
                    for (HudiPartitionInfo partitionInfo : partitionInfoBatch) {
                        partitionInfo.getHivePartitionKeys();
                        partitionQueue.add(partitionInfo);
                    }
                }
            }
        }
    }

    public Deque<HudiPartitionInfo> getPartitionQueue()
    {
        return partitionQueue;
    }

    private int updateBatchSize()
    {
        if (currentBatchSize <= 0) {
            currentBatchSize = minPartitionBatchSize;
        }
        else {
            currentBatchSize *= 2;
            currentBatchSize = Math.min(currentBatchSize, maxPartitionBatchSize);
        }
        return currentBatchSize;
    }
}
