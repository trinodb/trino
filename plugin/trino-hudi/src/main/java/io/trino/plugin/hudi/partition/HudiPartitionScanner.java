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
import io.trino.plugin.hudi.query.HudiFileListing;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.collection.ImmutablePair;
import org.apache.hudi.common.util.collection.Pair;

import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.lang.String.format;

public class HudiPartitionScanner
        implements Runnable
{
    private static final Logger log = Logger.get(HudiPartitionScanner.class);
    private final HudiFileListing hudiFileListing;
    private final Deque<HudiPartitionInfo> partitionQueue;
    private final Map<String, HudiPartitionInfo> partitionInfoMap;
    private final Deque<Pair<FileStatus, String>> hoodieFileStatusQueue;
    private boolean isRunning;

    public HudiPartitionScanner(
            HudiFileListing hudiFileListing,
            Deque<HudiPartitionInfo> partitionQueue,
            Map<String, HudiPartitionInfo> partitionInfoMap,
            Deque<Pair<FileStatus, String>> hoodieFileStatusQueue)
    {
        this.hudiFileListing = hudiFileListing;
        this.partitionQueue = partitionQueue;
        this.partitionInfoMap = partitionInfoMap;
        this.hoodieFileStatusQueue = hoodieFileStatusQueue;
        this.isRunning = true;
    }

    @Override
    public void run()
    {
        HoodieTimer timer = new HoodieTimer().startTimer();

        while (isRunning || !partitionQueue.isEmpty()) {
            HudiPartitionInfo partitionInfo = null;
            synchronized (partitionQueue) {
                if (!partitionQueue.isEmpty()) {
                    partitionInfo = partitionQueue.pollFirst();
                }
            }

            if (partitionInfo != null) {
                scanPartition(partitionInfo);
            }
        }
        log.debug(format("HudiPartitionScanner %s finishes in %d ms", this, timer.endTimer()));
    }

    public void stopRunning()
    {
        this.isRunning = false;
    }

    private void scanPartition(HudiPartitionInfo partitionInfo)
    {
        // Load Hive partition keys
        synchronized (partitionInfoMap) {
            partitionInfoMap.put(partitionInfo.getRelativePartitionPath(), partitionInfo);
        }
        final String relativePartitionPath = partitionInfo.getRelativePartitionPath();
        List<Pair<FileStatus, String>> fileStatusList = hudiFileListing.listStatus(partitionInfo).stream()
                .map(fileStatus -> new ImmutablePair<>(fileStatus, relativePartitionPath))
                .collect(Collectors.toList());
        synchronized (hoodieFileStatusQueue) {
            hoodieFileStatusQueue.addAll(fileStatusList);
        }
        log.debug(format("Add %d base files for %s",
                fileStatusList.size(), partitionInfo.getRelativePartitionPath()));
    }
}
