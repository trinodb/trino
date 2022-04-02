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

package io.trino.plugin.hudi.split;

import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.trino.plugin.hudi.HudiTableHandle;
import io.trino.plugin.hudi.partition.HudiPartitionInfo;
import io.trino.plugin.hudi.partition.HudiPartitionInfoLoader;
import io.trino.plugin.hudi.partition.HudiPartitionScanner;
import io.trino.plugin.hudi.partition.HudiPartitionSplitGenerator;
import io.trino.plugin.hudi.query.HudiFileListing;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.collection.Pair;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static io.trino.plugin.hudi.HudiSessionProperties.getMinimumAssignedSplitWeight;
import static io.trino.plugin.hudi.HudiSessionProperties.getPartitionScannerParallelism;
import static io.trino.plugin.hudi.HudiSessionProperties.getSplitGeneratorParallelism;
import static io.trino.plugin.hudi.HudiSessionProperties.getStandardSplitWeightSize;
import static io.trino.plugin.hudi.HudiSessionProperties.isSizeBasedSplitWeightsEnabled;
import static java.lang.String.format;

public class HudiSplitBackgroundLoader
        implements Runnable
{
    private static final Logger log = Logger.get(HudiSplitBackgroundLoader.class);
    private final ConnectorSession session;
    private final HudiTableHandle tableHandle;
    private final HoodieTableMetaClient metaClient;
    private final HudiFileListing hudiFileListing;
    private final Deque<ConnectorSplit> connectorSplitQueue;
    private final Deque<HudiPartitionInfo> partitionQueue;
    private final Map<String, HudiPartitionInfo> partitionInfoMap;
    private final Deque<Pair<FileStatus, String>> hoodieFileStatusQueue;
    private final ExecutorService partitionInfoLoaderExecutorService;
    private final ExecutorService partitionScannerExecutorService;
    private final ExecutorService splitGeneratorExecutorService;
    private final int partitionScannerNumThreads;
    private final int splitGeneratorNumThreads;
    private final boolean sizeBasedSplitWeightsEnabled;
    private final DataSize standardSplitWeightSize;
    private final double minimumAssignedSplitWeight;

    public HudiSplitBackgroundLoader(
            ConnectorSession session,
            HudiTableHandle tableHandle,
            HoodieTableMetaClient metaClient,
            HudiFileListing hudiFileListing,
            Deque<ConnectorSplit> connectorSplitQueue)
    {
        this.session = session;
        this.tableHandle = tableHandle;
        this.metaClient = metaClient;
        this.hudiFileListing = hudiFileListing;
        this.connectorSplitQueue = connectorSplitQueue;
        this.partitionQueue = new ArrayDeque<>();
        this.partitionInfoMap = new HashMap<>();
        this.hoodieFileStatusQueue = new ArrayDeque<>();
        this.partitionScannerNumThreads = getPartitionScannerParallelism(session);
        this.splitGeneratorNumThreads = getSplitGeneratorParallelism(session);
        this.partitionInfoLoaderExecutorService = Executors.newSingleThreadExecutor();
        this.partitionScannerExecutorService = Executors.newCachedThreadPool();
        this.splitGeneratorExecutorService = Executors.newCachedThreadPool();
        this.sizeBasedSplitWeightsEnabled = isSizeBasedSplitWeightsEnabled(session);
        this.standardSplitWeightSize = getStandardSplitWeightSize(session);
        this.minimumAssignedSplitWeight = getMinimumAssignedSplitWeight(session);
    }

    @Override
    public void run()
    {
        HoodieTimer timer = new HoodieTimer().startTimer();
        FileSystem fileSystem = metaClient.getFs();
        // Step 1: fetch partitions info that need to be read for file listing.
        HudiPartitionInfoLoader partitionInfoLoader =
                new HudiPartitionInfoLoader(session, hudiFileListing, partitionQueue);
        Future partitionInfoLoaderFuture = partitionInfoLoaderExecutorService.submit(partitionInfoLoader);
        // Step 2: scan partitions to list files concurrently.
        List<HudiPartitionScanner> partitionScannerList = new ArrayList<>();
        List<Future> partitionScannerFutures = new ArrayList<>();

        for (int i = 0; i < partitionScannerNumThreads; i++) {
            HudiPartitionScanner scanner = new HudiPartitionScanner(hudiFileListing,
                    partitionQueue, partitionInfoMap, hoodieFileStatusQueue);
            partitionScannerList.add(scanner);
            partitionScannerFutures.add(partitionScannerExecutorService.submit(scanner));
        }
        // Step 3: Generate splits from the files listed in the second step.
        List<HudiPartitionSplitGenerator> splitGeneratorList = new ArrayList<>();
        List<Future> splitGeneratorFutures = new ArrayList<>();

        for (int i = 0; i < splitGeneratorNumThreads; i++) {
            HudiSplitWeightProvider splitWeightProvider = sizeBasedSplitWeightsEnabled
                    ? new SizeBasedSplitWeightProvider(minimumAssignedSplitWeight, standardSplitWeightSize)
                    : HudiSplitWeightProvider.uniformStandardWeightProvider();
            HudiPartitionSplitGenerator generator = new HudiPartitionSplitGenerator(
                    fileSystem,
                    metaClient,
                    tableHandle,
                    splitWeightProvider,
                    partitionInfoMap,
                    hoodieFileStatusQueue,
                    connectorSplitQueue);
            splitGeneratorList.add(generator);
            splitGeneratorFutures.add(splitGeneratorExecutorService.submit(generator));
        }

        // Wait for partition info loader to finish
        try {
            partitionInfoLoaderFuture.get();
        }
        catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("Partition loader interrupted", e);
        }

        for (HudiPartitionScanner scanner : partitionScannerList) {
            scanner.stopRunning();
        }

        // Wait for all partition scanners to finish
        for (Future future : partitionScannerFutures) {
            try {
                future.get();
            }
            catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException("Partition scanner interrupted", e);
            }
        }

        for (HudiPartitionSplitGenerator generator : splitGeneratorList) {
            generator.stopRunning();
        }

        // Wait for all split generators to finish
        for (Future future : splitGeneratorFutures) {
            try {
                future.get();
            }
            catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException("Split generator interrupted", e);
            }
        }
        log.debug(format("Finish getting all splits in %d ms", timer.endTimer()));
    }
}
