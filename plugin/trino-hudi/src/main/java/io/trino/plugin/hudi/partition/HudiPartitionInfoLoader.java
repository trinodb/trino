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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hudi.common.util.HoodieTimer;

import java.util.Deque;
import java.util.List;
import java.util.Optional;

public class HudiPartitionInfoLoader
        implements Runnable
{
    private static final Logger log = Logger.get(HudiPartitionInfoLoader.class);

    private final HudiDirectoryLister hudiDirectoryLister;
    private final HudiSplitFactory hudiSplitFactory;
    private final AsyncQueue<ConnectorSplit> asyncQueue;
    private final Deque<String> partitionQueue;

    private boolean isRunning;

    public HudiPartitionInfoLoader(
            HudiDirectoryLister hudiDirectoryLister,
            HudiSplitFactory hudiSplitFactory,
            AsyncQueue<ConnectorSplit> asyncQueue,
            Deque<String> partitionQueue)
    {
        this.hudiDirectoryLister = hudiDirectoryLister;
        this.hudiSplitFactory = hudiSplitFactory;
        this.asyncQueue = asyncQueue;
        this.partitionQueue = partitionQueue;
        this.isRunning = true;
    }

    @Override
    public void run()
    {
        HoodieTimer timer = new HoodieTimer().startTimer();

        while (isRunning || !partitionQueue.isEmpty()) {
            String partitionName = partitionQueue.poll();

            if (partitionName != null) {
                generateSplitsFromPartition(partitionName);
            }
        }
        log.debug("HudiPartitionInfoLoader %s finishes in %d ms", this, timer.endTimer());
    }

    private void generateSplitsFromPartition(String partitionName)
    {
        Optional<HudiPartitionInfo> partitionInfo = hudiDirectoryLister.getPartitionInfo(partitionName);
        if (partitionInfo.isPresent()) {
            List<HivePartitionKey> partitionKeys = partitionInfo.get().getHivePartitionKeys();
            List<FileStatus> partitionFiles = hudiDirectoryLister.listStatus(partitionInfo.get());
            partitionFiles.stream()
                    .flatMap(fileStatus -> hudiSplitFactory.createSplits(partitionKeys, fileStatus))
                    .map(asyncQueue::offer)
                    .forEachOrdered(MoreFutures::getFutureValue);
        }
    }

    public void stopRunning()
    {
        this.isRunning = false;
    }
}
