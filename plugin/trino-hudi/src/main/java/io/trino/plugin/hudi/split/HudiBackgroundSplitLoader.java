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

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.concurrent.MoreFutures;
import io.trino.plugin.hive.HivePartitionKey;
import io.trino.plugin.hive.util.AsyncQueue;
import io.trino.plugin.hudi.HudiTableHandle;
import io.trino.plugin.hudi.partition.HudiPartitionInfo;
import io.trino.plugin.hudi.partition.HudiPartitionInfoLoader;
import io.trino.plugin.hudi.query.HudiDirectoryLister;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import org.apache.hadoop.fs.FileStatus;

import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.function.Consumer;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.trino.plugin.hudi.HudiSessionProperties.getPartitionInfoLoaderParallelism;
import static io.trino.plugin.hudi.HudiSessionProperties.getSplitLoaderParallelism;
import static java.util.Objects.requireNonNull;

public class HudiBackgroundSplitLoader
{
    private final HudiDirectoryLister hudiDirectoryLister;
    private final AsyncQueue<ConnectorSplit> asyncQueue;
    private final Executor splitLoaderExecutor;
    private final Consumer<Throwable> splitErrorListener;
    private final Consumer<Throwable> partitionErrorListener;
    private final HudiSplitFactory hudiSplitFactory;
    private final Deque<List<String>> partitionNamesQueue;
    private final Deque<HudiPartitionInfo> partitionInfoQueue;
    private final Executor partitionInfoLoaderExecutor;
    private final Deque<Boolean> partitionInfoLoaderStatusQueue;
    private final int partitionInfoLoaderNumThreads;
    private final int splitLoaderNumThreads;

    public HudiBackgroundSplitLoader(
            ConnectorSession session,
            HudiTableHandle tableHandle,
            HudiDirectoryLister hudiDirectoryLister,
            AsyncQueue<ConnectorSplit> asyncQueue,
            Executor splitLoaderExecutor,
            HudiSplitWeightProvider hudiSplitWeightProvider,
            Deque<List<String>> partitionNamesQueue,
            Deque<HudiPartitionInfo> partitionInfoQueue,
            Executor partitionInfoLoaderExecutor,
            Consumer<Throwable> partitionErrorListener,
            Consumer<Throwable> splitErrorListener)
    {
        this.hudiDirectoryLister = requireNonNull(hudiDirectoryLister, "hudiDirectoryLister is null");
        this.asyncQueue = requireNonNull(asyncQueue, "asyncQueue is null");
        this.splitLoaderExecutor = requireNonNull(splitLoaderExecutor, "splitLoaderExecutor is null");
        this.partitionInfoLoaderExecutor = requireNonNull(partitionInfoLoaderExecutor, "partitionInfoLoaderExecutor is null");
        this.splitErrorListener = requireNonNull(splitErrorListener, "splitErrorListener is null");
        this.partitionErrorListener = requireNonNull(partitionErrorListener, "partitionErrorListener is null");
        this.hudiSplitFactory = new HudiSplitFactory(tableHandle, hudiSplitWeightProvider);
        this.partitionNamesQueue = requireNonNull(partitionNamesQueue, "partitionNamesQueue is null");
        this.partitionInfoQueue = requireNonNull(partitionInfoQueue, "partitionInfoDeque is null");
        this.partitionInfoLoaderStatusQueue = new ConcurrentLinkedDeque<>();
        this.partitionInfoLoaderNumThreads = getPartitionInfoLoaderParallelism(session);
        this.splitLoaderNumThreads = getSplitLoaderParallelism(session);
    }


    public void start()
    {
        List<ListenableFuture<Void>> splitFutures = new ArrayList<>();
        for (int i = 0; i < partitionInfoLoaderNumThreads; i++) {
            HudiPartitionInfoLoader partitionInfoLoader = new HudiPartitionInfoLoader(partitionNamesQueue, hudiDirectoryLister, partitionInfoQueue, partitionInfoLoaderStatusQueue);
            hookPartitionErrorListener(Futures.submit(partitionInfoLoader, partitionInfoLoaderExecutor));
        }

        for (int i = 0; i < splitLoaderNumThreads; i++) {
            ListenableFuture<Void> splitsFuture = Futures.submit(this::loadSplits, splitLoaderExecutor);
            splitFutures.add(splitsFuture);
            hookSplitErrorListener(splitsFuture);
        }

        Futures.whenAllComplete(splitFutures).run(asyncQueue::finish, directExecutor());
    }


    private void loadSplits()
    {
        while (!partitionInfoLoaderStatusQueue.isEmpty() || !partitionInfoQueue.isEmpty()) {
            HudiPartitionInfo partition = partitionInfoQueue.poll();
            if (partition != null) {
                List<HivePartitionKey> partitionKeys = partition.getHivePartitionKeys();
                List<FileStatus> partitionFiles = hudiDirectoryLister.listStatus(partition);
                partitionFiles.stream()
                        .flatMap(fileStatus -> hudiSplitFactory.createSplits(partitionKeys, fileStatus))
                        .map(asyncQueue::offer)
                        .forEachOrdered(MoreFutures::getFutureValue);
            }
        }
    }

    private <T> void hookPartitionErrorListener(ListenableFuture<T> future)
    {
        Futures.addCallback(future, new FutureCallback<T>() {
            @Override
            public void onSuccess(T result) {}

            @Override
            public void onFailure(Throwable t) {
                partitionErrorListener.accept(t);
            }
        }, directExecutor());
    }

    private <T> void hookSplitErrorListener(ListenableFuture<T> future)
    {
        Futures.addCallback(future, new FutureCallback<T>()
        {
            @Override
            public void onSuccess(T result) {}

            @Override
            public void onFailure(Throwable t)
            {
                splitErrorListener.accept(t);
            }
        }, directExecutor());
    }
}
