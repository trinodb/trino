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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.trino.plugin.hive.util.AsyncQueue;
import io.trino.plugin.hudi.HudiTableHandle;
import io.trino.plugin.hudi.partition.HudiPartitionInfoLoader;
import io.trino.plugin.hudi.query.HudiDirectoryLister;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;

import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.addExceptionCallback;
import static io.trino.plugin.hudi.HudiErrorCode.HUDI_CANNOT_OPEN_SPLIT;
import static io.trino.plugin.hudi.HudiSessionProperties.getSplitGeneratorParallelism;
import static java.util.Objects.requireNonNull;

public class HudiBackgroundSplitLoader
        implements Runnable
{
    private final HudiDirectoryLister hudiDirectoryLister;
    private final AsyncQueue<ConnectorSplit> asyncQueue;
    private final Executor splitGeneratorExecutor;
    private final int splitGeneratorNumThreads;
    private final HudiSplitFactory hudiSplitFactory;
    private final List<String> partitions;
    private final Consumer<Throwable> errorListener;
    private final String commitTime;

    public HudiBackgroundSplitLoader(
            ConnectorSession session,
            HudiTableHandle tableHandle,
            HudiDirectoryLister hudiDirectoryLister,
            AsyncQueue<ConnectorSplit> asyncQueue,
            Executor splitGeneratorExecutor,
            HudiSplitWeightProvider hudiSplitWeightProvider,
            List<String> partitions,
            Consumer<Throwable> errorListener,
            String commitTime)
    {
        this.hudiDirectoryLister = requireNonNull(hudiDirectoryLister, "hudiDirectoryLister is null");
        this.asyncQueue = requireNonNull(asyncQueue, "asyncQueue is null");
        this.splitGeneratorExecutor = requireNonNull(splitGeneratorExecutor, "splitGeneratorExecutorService is null");
        this.splitGeneratorNumThreads = getSplitGeneratorParallelism(session);
        this.hudiSplitFactory = new HudiSplitFactory(tableHandle, hudiSplitWeightProvider);
        this.partitions = requireNonNull(partitions, "partitions is null");
        this.errorListener = requireNonNull(errorListener, "errorListener is null");
        this.commitTime = requireNonNull(commitTime, "commitTime is null");
    }

    @Override
    public void run()
    {
        Deque<String> partitionQueue = new ConcurrentLinkedDeque<>(partitions);
        List<HudiPartitionInfoLoader> splitGeneratorList = new ArrayList<>();
        List<ListenableFuture<Void>> splitGeneratorFutures = new ArrayList<>();

        // Start a number of partition split generators to generate the splits in parallel
        for (int i = 0; i < splitGeneratorNumThreads; i++) {
            HudiPartitionInfoLoader generator = new HudiPartitionInfoLoader(hudiDirectoryLister, hudiSplitFactory, asyncQueue, partitionQueue, commitTime);
            splitGeneratorList.add(generator);
            ListenableFuture<Void> future = Futures.submit(generator, splitGeneratorExecutor);
            addExceptionCallback(future, errorListener);
            splitGeneratorFutures.add(future);
        }

        for (HudiPartitionInfoLoader generator : splitGeneratorList) {
            // Let the split generator stop once the partition queue is empty
            generator.stopRunning();
        }

        try {
            // Wait for all split generators to finish
            Futures.whenAllComplete(splitGeneratorFutures)
                    .run(asyncQueue::finish, directExecutor())
                    .get();
        }
        catch (InterruptedException | ExecutionException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new TrinoException(HUDI_CANNOT_OPEN_SPLIT, "Error generating Hudi split", e);
        }
    }
}
