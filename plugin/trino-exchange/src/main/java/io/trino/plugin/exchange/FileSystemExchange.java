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
package io.trino.plugin.exchange;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.exchange.FileSystemExchangeManager.PATH_SEPARATOR;
import static io.trino.plugin.exchange.FileSystemExchangeSink.COMMITTED_MARKER_FILE_NAME;
import static io.trino.plugin.exchange.FileSystemExchangeSink.DATA_FILE_SUFFIX;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.supplyAsync;

public class FileSystemExchange
        implements Exchange
{
    private static final Pattern PARTITION_FILE_NAME_PATTERN = Pattern.compile("(\\d+)\\.data");

    private final URI baseDirectory;
    private final FileSystemExchangeStorage exchangeStorage;
    private final ExchangeContext exchangeContext;
    private final int outputPartitionCount;
    private final Optional<SecretKey> secretKey;
    private final ExecutorService executor;

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
            URI baseDirectory,
            FileSystemExchangeStorage exchangeStorage,
            ExchangeContext exchangeContext,
            int outputPartitionCount,
            Optional<SecretKey> secretKey,
            ExecutorService executor)
    {
        this.baseDirectory = requireNonNull(baseDirectory, "baseDirectory is null");
        this.exchangeStorage = requireNonNull(exchangeStorage, "exchangeStorage is null");
        this.exchangeContext = requireNonNull(exchangeContext, "exchangeContext is null");
        this.outputPartitionCount = outputPartitionCount;
        this.secretKey = requireNonNull(secretKey, "secretKey is null");
        this.executor = requireNonNull(executor, "executor is null");
    }

    public void initialize()
    {
        try {
            exchangeStorage.createDirectories(getExchangeDirectory());
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
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
        URI outputDirectory = getExchangeDirectory()
                .resolve(fileSystemExchangeSinkHandle.getPartitionId() + PATH_SEPARATOR)
                .resolve(taskAttemptId + PATH_SEPARATOR);
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
        CompletableFuture<List<ExchangeSourceHandle>> exchangeSourceHandlesCreationFuture = null;
        synchronized (this) {
            if (exchangeSourceHandlesCreationStarted) {
                return;
            }
            if (noMoreSinks && finishedSinks.containsAll(allSinks)) {
                // input is ready, create exchange source handles
                exchangeSourceHandlesCreationStarted = true;
                exchangeSourceHandlesCreationFuture = supplyAsync(this::createExchangeSourceHandles, executor);
            }
        }
        if (exchangeSourceHandlesCreationFuture != null) {
            exchangeSourceHandlesCreationFuture.whenComplete((exchangeSourceHandles, throwable) -> {
                if (throwable != null) {
                    exchangeSourceHandlesFuture.completeExceptionally(throwable);
                }
                else {
                    exchangeSourceHandlesFuture.complete(exchangeSourceHandles);
                }
            });
        }
    }

    private List<ExchangeSourceHandle> createExchangeSourceHandles()
    {
        Multimap<Integer, FileStatus> partitionFiles = ArrayListMultimap.create();
        List<Integer> finishedTaskPartitions;
        synchronized (this) {
            finishedTaskPartitions = ImmutableList.copyOf(finishedSinks);
        }
        for (Integer taskPartition : finishedTaskPartitions) {
            URI committedAttemptPath = getCommittedAttemptPath(taskPartition);
            Map<Integer, FileStatus> partitions = getCommittedPartitions(committedAttemptPath);
            partitions.forEach(partitionFiles::put);
        }

        ImmutableList.Builder<ExchangeSourceHandle> result = ImmutableList.builder();
        for (Integer partitionId : partitionFiles.keySet()) {
            result.add(new FileSystemExchangeSourceHandle(partitionId, ImmutableList.copyOf(partitionFiles.get(partitionId)), secretKey.map(SecretKey::getEncoded)));
        }
        return result.build();
    }

    private URI getCommittedAttemptPath(Integer taskPartition)
    {
        URI sinkOutputBasePath = getExchangeDirectory().resolve(taskPartition + PATH_SEPARATOR);
        try {
            List<URI> attemptPaths = exchangeStorage.listDirectories(sinkOutputBasePath);
            checkState(!attemptPaths.isEmpty(), "No attempts found under sink output path %s", sinkOutputBasePath);

            return attemptPaths.stream()
                    .filter(this::isCommitted)
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException(format("No committed attempts found under sink output path %s", sinkOutputBasePath)));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private boolean isCommitted(URI attemptPath)
    {
        URI commitMarkerFilePath = attemptPath.resolve(COMMITTED_MARKER_FILE_NAME);
        try {
            return exchangeStorage.exists(commitMarkerFilePath);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Map<Integer, FileStatus> getCommittedPartitions(URI committedAttemptPath)
    {
        try {
            List<FileStatus> partitionFiles = exchangeStorage.listFiles(committedAttemptPath)
                    .stream()
                    .filter(file -> file.getFilePath().endsWith(DATA_FILE_SUFFIX))
                    .collect(toImmutableList());
            ImmutableMap.Builder<Integer, FileStatus> result = ImmutableMap.builder();
            for (FileStatus partitionFile : partitionFiles) {
                Matcher matcher = PARTITION_FILE_NAME_PATTERN.matcher(new File(partitionFile.getFilePath()).getName());
                checkState(matcher.matches(), "Unexpected partition file: %s", partitionFile);
                int partitionId = Integer.parseInt(matcher.group(1));
                result.put(partitionId, partitionFile);
            }
            return result.build();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private URI getExchangeDirectory()
    {
        return baseDirectory.resolve(exchangeContext.getQueryId() + "." + exchangeContext.getExchangeId() + PATH_SEPARATOR);
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
        exchangeStorage.deleteRecursively(getExchangeDirectory());
    }
}
