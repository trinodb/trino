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

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.trino.plugin.hive.HivePartitionKey;
import io.trino.plugin.hudi.HudiSplit;
import io.trino.plugin.hudi.HudiTableHandle;
import io.trino.plugin.hudi.HudiUtil;
import io.trino.plugin.hudi.split.HudiSplitWeightProvider;
import io.trino.spi.connector.ConnectorSplit;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieIOException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.lang.String.format;

public class HudiPartitionSplitGenerator
        implements Runnable
{
    private static final Logger log = Logger.get(HudiPartitionSplitGenerator.class);
    private final FileSystem fileSystem;
    private final HoodieTableMetaClient metaClient;
    private final HudiTableHandle tableHandle;
    private final HudiSplitWeightProvider hudiSplitWeightProvider;
    private final Map<String, HudiPartitionInfo> partitionInfoMap;
    private final Deque<Pair<FileStatus, String>> hoodieFileStatusQueue;
    private final Deque<ConnectorSplit> connectorSplitQueue;
    private boolean isRunning;

    public HudiPartitionSplitGenerator(
            FileSystem fileSystem,
            HoodieTableMetaClient metaClient,
            HudiTableHandle tableHandle,
            HudiSplitWeightProvider hudiSplitWeightProvider,
            Map<String, HudiPartitionInfo> partitionInfoMap,
            Deque<Pair<FileStatus, String>> hoodieFileStatusQueue,
            Deque<ConnectorSplit> connectorSplitQueue)
    {
        this.fileSystem = fileSystem;
        this.metaClient = metaClient;
        this.tableHandle = tableHandle;
        this.hudiSplitWeightProvider = hudiSplitWeightProvider;
        this.partitionInfoMap = partitionInfoMap;
        this.hoodieFileStatusQueue = hoodieFileStatusQueue;
        this.connectorSplitQueue = connectorSplitQueue;
        this.isRunning = true;
    }

    @Override
    public void run()
    {
        HoodieTimer timer = new HoodieTimer().startTimer();
        while (isRunning || !hoodieFileStatusQueue.isEmpty()) {
            Pair<FileStatus, String> fileStatusPartitionPair = null;
            synchronized (hoodieFileStatusQueue) {
                if (!hoodieFileStatusQueue.isEmpty()) {
                    fileStatusPartitionPair = hoodieFileStatusQueue.pollFirst();
                }
            }
            if (fileStatusPartitionPair != null) {
                try {
                    String relativePartitionPath = fileStatusPartitionPair.getValue();
                    final List<HivePartitionKey> hivePartitionKeys;
                    synchronized (partitionInfoMap) {
                        hivePartitionKeys = partitionInfoMap.get(relativePartitionPath).getHivePartitionKeys();
                    }
                    List<HudiSplit> hudiSplits = HudiUtil.getSplits(fileSystem, fileStatusPartitionPair.getKey())
                            .stream()
                            .flatMap(fileSplit -> {
                                List<HudiSplit> result = new ArrayList<>();
                                try {
                                    result.add(new HudiSplit(
                                            fileSplit.getPath().toString(),
                                            fileSplit.getStart(),
                                            fileSplit.getLength(),
                                            metaClient.getFs().getLength(fileSplit.getPath()),
                                            ImmutableList.of(),
                                            tableHandle.getRegularPredicates(),
                                            hivePartitionKeys,
                                            hudiSplitWeightProvider.weightForSplitSizeInBytes(fileSplit.getLength())));
                                }
                                catch (IOException e) {
                                    throw new HoodieIOException(format(
                                            "Unable to get Hudi split for %s, start=%d len=%d",
                                            fileSplit.getPath(), fileSplit.getStart(), fileSplit.getLength()), e);
                                }
                                return result.stream();
                            })
                            .collect(Collectors.toList());
                    synchronized (connectorSplitQueue) {
                        connectorSplitQueue.addAll(hudiSplits);
                    }
                }
                catch (IOException e) {
                    throw new HoodieIOException("Unable to get splits for " + fileStatusPartitionPair.getKey().getPath(), e);
                }
            }
        }
        log.debug(format("HudiPartitionSplitGenerator finishes in %d ms", timer.endTimer()));
    }

    public void stopRunning()
    {
        this.isRunning = false;
    }
}
