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
package io.trino.plugin.exchange.filesystem;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.trino.plugin.exchange.filesystem.FileSystemExchangeSourceHandle.SourceFile;
import io.trino.spi.exchange.Exchange;
import io.trino.spi.exchange.ExchangeContext;
import io.trino.spi.exchange.ExchangeId;
import io.trino.spi.exchange.ExchangeSinkHandle;
import io.trino.spi.exchange.ExchangeSinkInstanceHandle;
import io.trino.spi.exchange.ExchangeSourceHandle;
import io.trino.spi.exchange.ExchangeSourceHandleSource;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.AsyncSemaphore.processAll;
import static io.trino.plugin.exchange.filesystem.FileSystemExchangeManager.PATH_SEPARATOR;
import static io.trino.plugin.exchange.filesystem.FileSystemExchangeSink.COMMITTED_MARKER_FILE_NAME;
import static io.trino.plugin.exchange.filesystem.FileSystemExchangeSink.DATA_FILE_SUFFIX;
import static java.lang.Integer.parseInt;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class FileSystemExchange
        implements Exchange
{
    private static final Pattern PARTITION_FILE_NAME_PATTERN = Pattern.compile("(\\d+)_(\\d+)\\.data");
    private static final char[] RANDOMIZED_HEX_PREFIX_ALPHABET = "abcdef0123456789".toCharArray();
    private static final int RANDOMIZED_HEX_PREFIX_LENGTH = 6;

    private final List<URI> baseDirectories;
    private final FileSystemExchangeStorage exchangeStorage;
    private final FileSystemExchangeStats stats;
    private final ExchangeContext exchangeContext;
    private final int outputPartitionCount;
    private final boolean preserveOrderWithinPartition;
    private final int fileListingParallelism;
    private final long exchangeSourceHandleTargetDataSizeInBytes;
    private final ExecutorService executor;

    private final Map<Integer, URI> outputDirectories = new ConcurrentHashMap<>();

    @GuardedBy("this")
    private final Set<Integer> allSinks = new HashSet<>();
    @GuardedBy("this")
    private final Map<Integer, Integer> finishedSinks = new HashMap<>();
    @GuardedBy("this")
    private boolean noMoreSinks;
    @GuardedBy("this")
    private boolean exchangeSourceHandlesCreationStarted;

    private final CompletableFuture<List<ExchangeSourceHandle>> exchangeSourceHandlesFuture = new CompletableFuture<>();

    public FileSystemExchange(
            List<URI> baseDirectories,
            FileSystemExchangeStorage exchangeStorage,
            FileSystemExchangeStats stats,
            ExchangeContext exchangeContext,
            int outputPartitionCount,
            boolean preserveOrderWithinPartition,
            int fileListingParallelism,
            long exchangeSourceHandleTargetDataSizeInBytes,
            ExecutorService executor)
    {
        List<URI> directories = new ArrayList<>(requireNonNull(baseDirectories, "baseDirectories is null"));
        Collections.shuffle(directories);

        this.baseDirectories = ImmutableList.copyOf(directories);
        this.exchangeStorage = requireNonNull(exchangeStorage, "exchangeStorage is null");
        this.stats = requireNonNull(stats, "stats is null");
        this.exchangeContext = requireNonNull(exchangeContext, "exchangeContext is null");
        this.outputPartitionCount = outputPartitionCount;
        this.preserveOrderWithinPartition = preserveOrderWithinPartition;

        this.fileListingParallelism = fileListingParallelism;
        this.exchangeSourceHandleTargetDataSizeInBytes = exchangeSourceHandleTargetDataSizeInBytes;
        this.executor = requireNonNull(executor, "executor is null");
    }

    @Override
    public ExchangeId getId()
    {
        return exchangeContext.getExchangeId();
    }

    @Override
    public synchronized ExchangeSinkHandle addSink(int taskPartition)
    {
        FileSystemExchangeSinkHandle sinkHandle = new FileSystemExchangeSinkHandle(taskPartition);
        allSinks.add(taskPartition);
        return sinkHandle;
    }

    @Override
    public void noMoreSinks()
    {
        synchronized (this) {
            noMoreSinks = true;
        }
    }

    @Override
    public CompletableFuture<ExchangeSinkInstanceHandle> instantiateSink(ExchangeSinkHandle sinkHandle, int taskAttemptId)
    {
        FileSystemExchangeSinkHandle fileSystemExchangeSinkHandle = (FileSystemExchangeSinkHandle) sinkHandle;
        int taskPartitionId = fileSystemExchangeSinkHandle.getPartitionId();
        URI outputDirectory = getTaskOutputDirectory(taskPartitionId).resolve(taskAttemptId + PATH_SEPARATOR);
        try {
            exchangeStorage.createDirectories(outputDirectory);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        return completedFuture(new FileSystemExchangeSinkInstanceHandle(fileSystemExchangeSinkHandle, outputDirectory, outputPartitionCount, preserveOrderWithinPartition));
    }

    @Override
    public CompletableFuture<ExchangeSinkInstanceHandle> updateSinkInstanceHandle(ExchangeSinkHandle sinkHandle, int taskAttemptId)
    {
        // this implementation never requests an update
        throw new UnsupportedOperationException();
    }

    @Override
    public void sinkFinished(ExchangeSinkHandle handle, int taskAttemptId)
    {
        synchronized (this) {
            FileSystemExchangeSinkHandle sinkHandle = (FileSystemExchangeSinkHandle) handle;
            finishedSinks.putIfAbsent(sinkHandle.getPartitionId(), taskAttemptId);
        }
    }

    @Override
    public void allRequiredSinksFinished()
    {
        verify(!Thread.holdsLock(this));
        ListenableFuture<List<ExchangeSourceHandle>> exchangeSourceHandlesCreationFuture;
        synchronized (this) {
            if (exchangeSourceHandlesCreationStarted) {
                return;
            }
            verify(noMoreSinks, "noMoreSinks is expected to be set");
            verify(finishedSinks.keySet().containsAll(allSinks), "all sinks are expected to be finished");
            // input is ready, create exchange source handles
            exchangeSourceHandlesCreationStarted = true;
            exchangeSourceHandlesCreationFuture = stats.getCreateExchangeSourceHandles().record(this::createExchangeSourceHandles);
        }
        Futures.addCallback(exchangeSourceHandlesCreationFuture, new FutureCallback<>()
        {
            @Override
            public void onSuccess(List<ExchangeSourceHandle> exchangeSourceHandles)
            {
                exchangeSourceHandlesFuture.complete(exchangeSourceHandles);
            }

            @Override
            public void onFailure(Throwable throwable)
            {
                exchangeSourceHandlesFuture.completeExceptionally(throwable);
            }
        }, directExecutor());
    }

    private ListenableFuture<List<ExchangeSourceHandle>> createExchangeSourceHandles()
    {
        List<CommittedTaskAttempt> committedTaskAttempts;
        synchronized (this) {
            committedTaskAttempts = finishedSinks.entrySet().stream()
                    .map(entry -> new CommittedTaskAttempt(entry.getKey(), entry.getValue()))
                    .collect(toImmutableList());
        }

        return Futures.transform(
                processAll(committedTaskAttempts, this::getCommittedPartitions, fileListingParallelism, executor),
                partitionsList -> {
                    Multimap<Integer, SourceFile> sourceFiles = ArrayListMultimap.create();
                    partitionsList.forEach(partitions -> partitions.forEach(sourceFiles::put));
                    ImmutableList.Builder<ExchangeSourceHandle> result = ImmutableList.builder();
                    for (Integer partitionId : sourceFiles.keySet()) {
                        Collection<SourceFile> files = sourceFiles.get(partitionId);
                        long currentExchangeHandleDataSizeInBytes = 0;
                        ImmutableList.Builder<SourceFile> currentExchangeHandleFiles = ImmutableList.builder();
                        for (SourceFile file : files) {
                            if (currentExchangeHandleDataSizeInBytes > 0 && currentExchangeHandleDataSizeInBytes + file.getFileSize() > exchangeSourceHandleTargetDataSizeInBytes) {
                                result.add(new FileSystemExchangeSourceHandle(exchangeContext.getExchangeId(), partitionId, currentExchangeHandleFiles.build()));
                                currentExchangeHandleDataSizeInBytes = 0;
                                currentExchangeHandleFiles = ImmutableList.builder();
                            }
                            currentExchangeHandleDataSizeInBytes += file.getFileSize();
                            currentExchangeHandleFiles.add(file);
                        }
                        if (currentExchangeHandleDataSizeInBytes > 0) {
                            result.add(new FileSystemExchangeSourceHandle(exchangeContext.getExchangeId(), partitionId, currentExchangeHandleFiles.build()));
                        }
                    }
                    return result.build();
                },
                executor);
    }

    private ListenableFuture<Multimap<Integer, SourceFile>> getCommittedPartitions(CommittedTaskAttempt committedTaskAttempt)
    {
        URI sinkOutputPath = getTaskOutputDirectory(committedTaskAttempt.partitionId());
        return stats.getGetCommittedPartitions().record(Futures.transform(
                exchangeStorage.listFilesRecursively(sinkOutputPath),
                sinkOutputFiles -> {
                    List<String> committedMarkerFilePaths = sinkOutputFiles.stream()
                            .map(FileStatus::getFilePath)
                            .filter(filePath -> filePath.endsWith(COMMITTED_MARKER_FILE_NAME))
                            .collect(toImmutableList());

                    if (committedMarkerFilePaths.isEmpty()) {
                        throw new IllegalStateException(format("No committed attempts found under sink output path %s", sinkOutputPath));
                    }

                    for (String committedMarkerFilePath : committedMarkerFilePaths) {
                        // Committed marker file path format: {sinkOutputPath}/{attemptId}/committed
                        String[] parts = committedMarkerFilePath.split(PATH_SEPARATOR);
                        checkState(parts.length >= 3, "committedMarkerFilePath %s is malformed", committedMarkerFilePath);
                        String stringCommittedAttemptId = parts[parts.length - 2];
                        if (parseInt(stringCommittedAttemptId) != committedTaskAttempt.attemptId()) {
                            // skip other successful attempts
                            continue;
                        }
                        int attemptIdOffset = committedMarkerFilePath.length() - stringCommittedAttemptId.length()
                                - PATH_SEPARATOR.length() - COMMITTED_MARKER_FILE_NAME.length();

                        // Data output file path format: {sinkOutputPath}/{attemptId}/{sourcePartitionId}_{splitId}.data
                        List<FileStatus> partitionFiles = sinkOutputFiles.stream()
                                .filter(file -> file.getFilePath().startsWith(stringCommittedAttemptId + PATH_SEPARATOR, attemptIdOffset) && file.getFilePath().endsWith(DATA_FILE_SUFFIX))
                                .collect(toImmutableList());

                        ImmutableMultimap.Builder<Integer, SourceFile> result = ImmutableMultimap.builder();
                        for (FileStatus partitionFile : partitionFiles) {
                            Matcher matcher = PARTITION_FILE_NAME_PATTERN.matcher(new File(partitionFile.getFilePath()).getName());
                            checkState(matcher.matches(), "Unexpected partition file: %s", partitionFile);
                            int partitionId = parseInt(matcher.group(1));
                            result.put(partitionId, new SourceFile(partitionFile.getFilePath(), partitionFile.getFileSize(), committedTaskAttempt.partitionId(), committedTaskAttempt.attemptId()));
                        }
                        return result.build();
                    }

                    throw new IllegalArgumentException("committed attempt %s for task %s not found".formatted(committedTaskAttempt.attemptId(), committedTaskAttempt.partitionId()));
                },
                executor));
    }

    private URI getTaskOutputDirectory(int taskPartitionId)
    {
        // Add a randomized prefix to evenly distribute data into different S3 shards
        // Data output file path format: {randomizedHexPrefix}.{queryId}.{stageId}.{sinkPartitionId}/{attemptId}/{sourcePartitionId}_{splitId}.data
        return outputDirectories.computeIfAbsent(taskPartitionId, ignored -> baseDirectories.get(ThreadLocalRandom.current().nextInt(baseDirectories.size()))
                .resolve(generateRandomizedHexPrefix() + "." + exchangeContext.getQueryId() + "." + exchangeContext.getExchangeId() + "." + taskPartitionId + PATH_SEPARATOR));
    }

    @Override
    public ExchangeSourceHandleSource getSourceHandles()
    {
        return new ExchangeSourceHandleSource()
        {
            @Override
            public CompletableFuture<ExchangeSourceHandleBatch> getNextBatch()
            {
                return exchangeSourceHandlesFuture.thenApply(handles -> new ExchangeSourceHandleBatch(handles, true));
            }

            @Override
            public void close() {}
        };
    }

    @Override
    public void close()
    {
        stats.getCloseExchange().record(exchangeStorage.deleteRecursively(allSinks.stream().map(this::getTaskOutputDirectory).collect(toImmutableList())));
    }

    /**
     * Some storage systems prefer the prefix to be hexadecimal characters
     */
    private static String generateRandomizedHexPrefix()
    {
        char[] value = new char[RANDOMIZED_HEX_PREFIX_LENGTH];
        for (int i = 0; i < value.length; i++) {
            value[i] = RANDOMIZED_HEX_PREFIX_ALPHABET[ThreadLocalRandom.current().nextInt(RANDOMIZED_HEX_PREFIX_ALPHABET.length)];
        }
        return new String(value);
    }

    private record CommittedTaskAttempt(int partitionId, int attemptId)
    {
        public CommittedTaskAttempt
        {
            checkArgument(partitionId >= 0, "partitionId is expected to be greater than or equal to zero: %s", partitionId);
            checkArgument(attemptId >= 0, "attemptId is expected to be greater than or equal to zero: %s", attemptId);
        }
    }
}
