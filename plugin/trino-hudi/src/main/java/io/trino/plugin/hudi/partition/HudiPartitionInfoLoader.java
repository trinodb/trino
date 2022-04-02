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

import io.airlift.log.Logger;
import io.trino.plugin.hive.metastore.Partition;
import io.trino.plugin.hudi.query.HudiFileListing;
import io.trino.spi.connector.ConnectorSession;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.exception.HoodieIOException;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.trino.plugin.hudi.HudiSessionProperties.getMaxPartitionBatchSize;
import static io.trino.plugin.hudi.HudiSessionProperties.getMinPartitionBatchSize;
import static java.lang.String.format;

public class HudiPartitionInfoLoader
        implements Runnable
{
    private static final Logger log = Logger.get(HudiPartitionInfoLoader.class);
    private final HudiFileListing hudiFileListing;
    private final int minPartitionBatchSize;
    private final int maxPartitionBatchSize;
    private final Deque<HudiPartitionInfo> partitionQueue;
    private int currBatchSize;

    public HudiPartitionInfoLoader(
            ConnectorSession session,
            HudiFileListing hudiFileListing,
            Deque<HudiPartitionInfo> partitionQueue)
    {
        this.hudiFileListing = hudiFileListing;
        this.partitionQueue = partitionQueue;
        this.minPartitionBatchSize = getMinPartitionBatchSize(session);
        this.maxPartitionBatchSize = getMaxPartitionBatchSize(session);
        this.currBatchSize = -1;
    }

    @Override
    public void run()
    {
        HoodieTimer timer = new HoodieTimer().startTimer();
        List<HudiPartitionInfo> hudiPartitionInfoList = hudiFileListing.getPartitionsToScan().stream()
                .sorted(Comparator.comparing(HudiPartitionInfo::getComparingKey)).collect(Collectors.toList());
        boolean shouldUseHiveMetastore =
                !hudiPartitionInfoList.isEmpty() && hudiPartitionInfoList.get(0) instanceof HudiPartitionHiveInfo;
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
                    Map<String, Optional<Partition>> partitions =
                            hudiFileListing.getPartitions(partitionInfoBatch.stream()
                                    .map(HudiPartitionInfo::getHivePartitionName)
                                    .collect(Collectors.toList()));
                    partitionInfoBatch
                            .forEach(partitionInfo -> {
                                String hivePartitionName = partitionInfo.getHivePartitionName();
                                if (!partitions.containsKey(hivePartitionName)) {
                                    throw new HoodieIOException("Partition does not exist: " + hivePartitionName);
                                }
                                partitionInfo.loadPartitionInfo(partitions.get(hivePartitionName));
                                synchronized (partitionQueue) {
                                    partitionQueue.add(partitionInfo);
                                }
                            });
                }
                else {
                    partitionInfoBatch.forEach(partitionInfo -> {
                        partitionInfo.getHivePartitionKeys();
                        synchronized (partitionQueue) {
                            partitionQueue.add(partitionInfo);
                        }
                    });
                }
            }
        }
        log.debug(format("HudiPartitionInfoLoader finishes in %d ms", timer.endTimer()));
    }

    private int updateBatchSize()
    {
        if (currBatchSize <= 0) {
            currBatchSize = minPartitionBatchSize;
        }
        else {
            currBatchSize *= 2;
            currBatchSize = Math.min(currBatchSize, maxPartitionBatchSize);
        }
        return currBatchSize;
    }
}
