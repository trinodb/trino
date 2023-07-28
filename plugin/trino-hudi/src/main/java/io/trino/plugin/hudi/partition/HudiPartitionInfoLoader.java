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

import io.trino.plugin.hudi.query.HudiDirectoryLister;

import java.util.Comparator;
import java.util.Deque;
import java.util.List;
import java.util.stream.Collectors;

public class HudiPartitionInfoLoader
        implements Runnable
{
    private final HudiDirectoryLister hudiDirectoryLister;
    private final Deque<HudiPartitionInfo> partitionInfoQueue;
    private final Deque<List<String>> partitionNamesQueue;
    private final Deque<Boolean> partitionLoadStatusQueue;

    public HudiPartitionInfoLoader(
            Deque<List<String>> partitionNamesQueues,
            HudiDirectoryLister hudiDirectoryLister,
            Deque<HudiPartitionInfo> partitionInfoQueue,
            Deque<Boolean> partitionLoadStatusQueue)
    {
        this.partitionNamesQueue = partitionNamesQueues;
        this.hudiDirectoryLister = hudiDirectoryLister;
        this.partitionInfoQueue = partitionInfoQueue;
        this.partitionLoadStatusQueue = partitionLoadStatusQueue;
    }

    @Override
    public void run()
    {
        while (!partitionNamesQueue.isEmpty()) {
            List<String> partitionNames = partitionNamesQueue.poll();
            if (partitionNames != null) {
                List<HudiPartitionInfo> hudiPartitionInfoList = hudiDirectoryLister.getPartitionsToScan(partitionNames).stream()
                        .sorted(Comparator.comparing(HudiPartitionInfo::getComparingKey))
                        .collect(Collectors.toList());

                // empty partitioned table
                if (hudiPartitionInfoList.isEmpty()) {
                    return;
                }

                // non-partitioned table
                if (hudiPartitionInfoList.size() == 1 && hudiPartitionInfoList.get(0).getHivePartitionName().isEmpty()) {
                    partitionInfoQueue.addAll(hudiPartitionInfoList);
                    return;
                }
                partitionInfoQueue.addAll(hudiPartitionInfoList);
            }
        }
        partitionLoadStatusQueue.poll();
    }
}
