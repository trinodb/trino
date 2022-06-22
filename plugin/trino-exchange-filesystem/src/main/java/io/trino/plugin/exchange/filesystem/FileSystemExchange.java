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
import io.trino.spi.exchange.Exchange;
import io.trino.spi.exchange.ExchangeContext;
import io.trino.spi.exchange.ExchangeSinkHandle;
import io.trino.spi.exchange.ExchangeSinkInstanceHandle;
import io.trino.spi.exchange.ExchangeSourceHandle;
import io.trino.spi.exchange.ExchangeSourceSplitter;
import io.trino.spi.exchange.ExchangeSourceStatistics;

import javax.annotation.concurrent.GuardedBy;
import javax.crypto.SecretKey;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.security.Key;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.AsyncSemaphore.processAll;
import static io.trino.plugin.exchange.filesystem.FileSystemExchangeManager.PATH_SEPARATOR;
import static io.trino.plugin.exchange.filesystem.FileSystemExchangeSink.COMMITTED_MARKER_FILE_NAME;
import static io.trino.plugin.exchange.filesystem.FileSystemExchangeSink.DATA_FILE_SUFFIX;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class FileSystemExchange
        implements Exchange
{
    private static final Pattern PARTITION_FILE_NAME_PATTERN = Pattern.compile("(\\d+)_(\\d+)\\.data");
    private static final char[] RANDOMIZED_PREFIX_ALPHABET = "abcdefghijklmnopqrstuvwzyz0123456789".toCharArray();
    private static final int RANDOMIZED_PREFIX_LENGTH = 6;

    private final List<URI> baseDirectories;
    private final FileSystemExchangeStorage exchangeStorage;
    private final FileSystemExchangeStats stats;
    private final ExchangeContext exchangeContext;
    private final int outputPartitionCount;
    private final int fileListingParallelism;
    private final Optional<SecretKey> secretKey;
    private final ExecutorService executor;

    private final Map<Integer, String> randomizedPrefixes = new ConcurrentHashMap<>();

    @GuardedBy("this")
    private final Set<Integer> allSinks = new HashSet<>();
    @GuardedBy("this")
    private final Set<Integer> finishedSinks = new HashSet<>();
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
            int fileListingParallelism,
            Optional<SecretKey> secretKey,
            ExecutorService executor)
    {
        List<URI> directories = new ArrayList<>(requireNonNull(baseDirectories, "baseDirectories is null"));
        Collections.shuffle(directories);

        this.baseDirectories = ImmutableList.copyOf(directories);
        this.exchangeStorage = requireNonNull(exchangeStorage, "exchangeStorage is null");
        this.stats = requireNonNull(stats, "stats is null");
        this.exchangeContext = requireNonNull(exchangeContext, "exchangeContext is null");
        this.outputPartitionCount = outputPartitionCount;
        this.fileListingParallelism = fileListingParallelism;
        this.secretKey = requireNonNull(secretKey, "secretKey is null");
        this.executor = requireNonNull(executor, "executor is null");
    }

    @Override
    public synchronized ExchangeSinkHandle addSink(int taskPartition)
    {
        FileSystemExchangeSinkHandle sinkHandle = new FileSystemExchangeSinkHandle(taskPartition, secretKey.map(Key::getEncoded));
        allSinks.add(taskPartition);
        return sinkHandle;
    }

    @Override
    public void noMoreSinks()
    {
        synchronized (this) {
            noMoreSinks = true;
        }
        checkInputReady();
    }

    @Override
    public ExchangeSinkInstanceHandle instantiateSink(ExchangeSinkHandle sinkHandle, int taskAttemptId)
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

        return new FileSystemExchangeSinkInstanceHandle(fileSystemExchangeSinkHandle, outputDirectory, outputPartitionCount);
    }

    @Override
    public void sinkFinished(ExchangeSinkInstanceHandle handle)
    {
        synchronized (this) {
            FileSystemExchangeSinkInstanceHandle instanceHandle = (FileSystemExchangeSinkInstanceHandle) handle;
            finishedSinks.add(instanceHandle.getSinkHandle().getPartitionId());
        }
        checkInputReady();
    }

    private void checkInputReady()
    {
        verify(!Thread.holdsLock(this));
        ListenableFuture<List<ExchangeSourceHandle>> exchangeSourceHandlesCreationFuture = null;
        synchronized (this) {
            if (exchangeSourceHandlesCreationStarted) {
                return;
            }
            if (noMoreSinks && finishedSinks.containsAll(allSinks)) {
                // input is ready, create exchange source handles
                exchangeSourceHandlesCreationStarted = true;
                exchangeSourceHandlesCreationFuture = stats.getCreateExchangeSourceHandles().record(this::createExchangeSourceHandles);
                exchangeSourceHandlesFuture.whenComplete((value, failure) -> {
                    if (exchangeSourceHandlesFuture.isCancelled()) {
                        exchangeSourceHandlesFuture.cancel(true);
                    }
                });
            }
        }
        if (exchangeSourceHandlesCreationFuture != null) {
            Futures.addCallback(exchangeSourceHandlesCreationFuture, new FutureCallback<>() {
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
    }

    private ListenableFuture<List<ExchangeSourceHandle>> createExchangeSourceHandles()
    {
        List<Integer> finishedTaskPartitions;
        synchronized (this) {
            finishedTaskPartitions = ImmutableList.copyOf(finishedSinks);
        }

        return Futures.transform(
                processAll(finishedTaskPartitions, this::getCommittedPartitions, fileListingParallelism, executor),
                partitionsList -> {
                    Multimap<Integer, FileStatus> partitionFiles = ArrayListMultimap.create();
                    partitionsList.forEach(partitions -> partitions.forEach(partitionFiles::put));
                    ImmutableList.Builder<ExchangeSourceHandle> result = ImmutableList.builder();
                    for (Integer partitionId : partitionFiles.keySet()) {
                        result.add(new FileSystemExchangeSourceHandle(partitionId, ImmutableList.copyOf(partitionFiles.get(partitionId)), secretKey.map(SecretKey::getEncoded)));
                    }
                    return result.build();
                },
                executor);
    }

    private ListenableFuture<Multimap<Integer, FileStatus>> getCommittedPartitions(int taskPartitionId)
    {
        URI sinkOutputPath = getTaskOutputDirectory(taskPartitionId);
        return stats.getGetCommittedPartitions().record(Futures.transform(
                exchangeStorage.listFilesRecursively(sinkOutputPath),
                sinkOutputFiles -> {
                    String committedMarkerFilePath = sinkOutputFiles.stream()
                            .map(FileStatus::getFilePath)
                            .filter(filePath -> filePath.endsWith(COMMITTED_MARKER_FILE_NAME))
                            .findFirst()
                            .orElseThrow(() -> new IllegalStateException(format("No committed attempts found under sink output path %s", sinkOutputPath)));
                    // Committed marker file path format: {sinkOutputPath}/{attemptId}/committed
                    String[] parts = committedMarkerFilePath.split(PATH_SEPARATOR);
                    checkState(parts.length >= 3, "committedMarkerFilePath %s is malformed", committedMarkerFilePath);
                    String committedAttemptId = parts[parts.length - 2];
                    int attemptIdOffset = committedMarkerFilePath.length() - committedAttemptId.length()
                            - PATH_SEPARATOR.length() - COMMITTED_MARKER_FILE_NAME.length();

                    // Data output file path format: {sinkOutputPath}/{attemptId}/{sourcePartitionId}_{splitId}.data
                    List<FileStatus> partitionFiles = sinkOutputFiles.stream()
                            .filter(file -> file.getFilePath().startsWith(committedAttemptId + PATH_SEPARATOR, attemptIdOffset) && file.getFilePath().endsWith(DATA_FILE_SUFFIX))
                            .collect(toImmutableList());

                    ImmutableMultimap.Builder<Integer, FileStatus> result = ImmutableMultimap.builder();
                    for (FileStatus partitionFile : partitionFiles) {
                        Matcher matcher = PARTITION_FILE_NAME_PATTERN.matcher(new File(partitionFile.getFilePath()).getName());
                        checkState(matcher.matches(), "Unexpected partition file: %s", partitionFile);
                        int partitionId = Integer.parseInt(matcher.group(1));
                        result.put(partitionId, partitionFile);
                    }
                    return result.build();
                },
                executor));
    }

    private URI getTaskOutputDirectory(int taskPartitionId)
    {
        URI baseDirectory = baseDirectories.get(taskPartitionId % baseDirectories.size());
        String randomizedPrefix = randomizedPrefixes.computeIfAbsent(taskPartitionId, ignored -> generateRandomizedPrefix());

        // Add a randomized prefix to evenly distribute data into different S3 shards
        // Data output file path format: {randomizedPrefix}.{queryId}.{stageId}.{sinkPartitionId}/{attemptId}/{sourcePartitionId}_{splitId}.data
        return baseDirectory.resolve(randomizedPrefix + "." + exchangeContext.getQueryId() + "." + exchangeContext.getExchangeId() + "." + taskPartitionId + PATH_SEPARATOR);
    }

    @Override
    public CompletableFuture<List<ExchangeSourceHandle>> getSourceHandles()
    {
        return exchangeSourceHandlesFuture;
    }

    @Override
    public ExchangeSourceSplitter split(ExchangeSourceHandle handle, long targetSizeInBytes)
    {
        // Currently we only split at the file level, and external logic groups sources that are not large enough
        FileSystemExchangeSourceHandle sourceHandle = (FileSystemExchangeSourceHandle) handle;
        Iterator<FileStatus> filesIterator = sourceHandle.getFiles().iterator();
        return new ExchangeSourceSplitter()
        {
            @Override
            public CompletableFuture<Void> isBlocked()
            {
                return completedFuture(null);
            }

            @Override
            public Optional<ExchangeSourceHandle> getNext()
            {
                if (filesIterator.hasNext()) {
                    return Optional.of(new FileSystemExchangeSourceHandle(sourceHandle.getPartitionId(), ImmutableList.of(filesIterator.next()), secretKey.map(SecretKey::getEncoded)));
                }
                return Optional.empty();
            }

            @Override
            public void close()
            {
            }
        };
    }

    @Override
    public ExchangeSourceStatistics getExchangeSourceStatistics(ExchangeSourceHandle handle)
    {
        FileSystemExchangeSourceHandle sourceHandle = (FileSystemExchangeSourceHandle) handle;
        long sizeInBytes = sourceHandle.getFiles().stream().mapToLong(FileStatus::getFileSize).sum();
        return new ExchangeSourceStatistics(sizeInBytes);
    }

    @Override
    public void close()
    {
        stats.getCloseExchange().record(exchangeStorage.deleteRecursively(allSinks.stream().map(this::getTaskOutputDirectory).collect(toImmutableList())));
    }

    private static String generateRandomizedPrefix()
    {
        char[] value = new char[RANDOMIZED_PREFIX_LENGTH];
        for (int i = 0; i < value.length; i++) {
            value[i] = RANDOMIZED_PREFIX_ALPHABET[ThreadLocalRandom.current().nextInt(RANDOMIZED_PREFIX_ALPHABET.length)];
        }
        return new String(value);
    }
}
