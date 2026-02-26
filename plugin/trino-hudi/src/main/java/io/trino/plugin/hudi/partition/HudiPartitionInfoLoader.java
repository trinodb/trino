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

import io.airlift.concurrent.MoreFutures;
import io.airlift.log.Logger;
import io.trino.plugin.hive.HivePartitionKey;
import io.trino.plugin.hive.util.AsyncQueue;
import io.trino.plugin.hudi.query.HudiDirectoryLister;
import io.trino.plugin.hudi.split.HudiSplitFactory;
import io.trino.spi.connector.ConnectorSplit;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.util.HoodieTimer;

import java.util.Deque;
import java.util.List;
import java.util.stream.Stream;

public class HudiPartitionInfoLoader
        implements Runnable
{
    private static final Logger log = Logger.get(HudiPartitionInfoLoader.class);
    private final String schemaName;
    private final String tableName;
    private final HudiDirectoryLister hudiDirectoryLister;
    private final HudiSplitFactory hudiSplitFactory;
    private final AsyncQueue<ConnectorSplit> asyncQueue;
    private final Deque<HiveHudiPartitionInfo> partitionQueue;
    private final String commitTime;
    private final boolean useIndex;

    private boolean isRunning;

    public HudiPartitionInfoLoader(
            String schemaName,
            String tableName,
            HudiDirectoryLister hudiDirectoryLister,
            String commitTime,
            HudiSplitFactory hudiSplitFactory,
            AsyncQueue<ConnectorSplit> asyncQueue,
            Deque<HiveHudiPartitionInfo> partitionQueue,
            boolean useIndex)
    {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.hudiDirectoryLister = hudiDirectoryLister;
        this.commitTime = commitTime;
        this.hudiSplitFactory = hudiSplitFactory;
        this.asyncQueue = asyncQueue;
        this.partitionQueue = partitionQueue;
        this.isRunning = true;
        this.useIndex = useIndex;
    }

    @Override
    public void run()
    {
        while (isRunning || !partitionQueue.isEmpty()) {
            HiveHudiPartitionInfo hudiPartitionInfo = partitionQueue.poll();

            if (hudiPartitionInfo != null && hudiPartitionInfo.getHivePartitionName() != null) {
                generateSplitsFromPartition(hudiPartitionInfo);
            }
        }
    }

    private void generateSplitsFromPartition(HiveHudiPartitionInfo hudiPartitionInfo)
    {
        HoodieTimer timer = HoodieTimer.start();
        List<HivePartitionKey> partitionKeys = hudiPartitionInfo.getHivePartitionKeys();
        Stream<FileSlice> partitionFileSlices = hudiDirectoryLister.listStatus(hudiPartitionInfo, useIndex);
        partitionFileSlices
                .flatMap(slice -> hudiSplitFactory.createSplits(partitionKeys, slice, this.commitTime))
                .map(asyncQueue::offer)
                .forEachOrdered(MoreFutures::getFutureValue);
        log.debug("Generated splits for partition [%s] on table %s.%s in %s ms",
                hudiPartitionInfo.getHivePartitionName(), schemaName, tableName, timer.endTimer());
    }

    public void stopRunning()
    {
        this.isRunning = false;
    }
}
