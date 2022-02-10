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
package io.trino.server.testing.exchange;

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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.server.testing.exchange.LocalFileSystemExchangeSink.COMMITTED_MARKER_FILE_NAME;
import static io.trino.server.testing.exchange.LocalFileSystemExchangeSink.DATA_FILE_SUFFIX;
import static java.nio.file.Files.createDirectories;
import static java.nio.file.Files.exists;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class LocalFileSystemExchange
        implements Exchange
{
    private static final Pattern PARTITION_FILE_NAME_PATTERN = Pattern.compile("(\\d+)\\.data");

    private final Path baseDirectory;
    private final ExchangeContext exchangeContext;
    private final int outputPartitionCount;

    @GuardedBy("this")
    private final Set<LocalFileSystemExchangeSinkHandle> allSinks = new HashSet<>();
    @GuardedBy("this")
    private final Set<LocalFileSystemExchangeSinkHandle> finishedSinks = new HashSet<>();
    @GuardedBy("this")
    private boolean noMoreSinks;

    private final CompletableFuture<List<ExchangeSourceHandle>> exchangeSourceHandlesFuture = new CompletableFuture<>();
    @GuardedBy("this")
    private boolean exchangeSourceHandlesCreated;

    public LocalFileSystemExchange(Path baseDirectory, ExchangeContext exchangeContext, int outputPartitionCount)
    {
        this.baseDirectory = requireNonNull(baseDirectory, "baseDirectory is null");
        this.exchangeContext = requireNonNull(exchangeContext, "exchangeContext is null");
        this.outputPartitionCount = outputPartitionCount;
    }

    public void initialize()
    {
        try {
            createDirectories(getExchangeDirectory());
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public synchronized ExchangeSinkHandle addSink(int taskPartitionId)
    {
        LocalFileSystemExchangeSinkHandle sinkHandle = new LocalFileSystemExchangeSinkHandle(
                exchangeContext.getQueryId(),
                exchangeContext.getExchangeId(),
                taskPartitionId);
        allSinks.add(sinkHandle);
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
        LocalFileSystemExchangeSinkHandle localFileSystemSinkHandle = (LocalFileSystemExchangeSinkHandle) sinkHandle;
        Path outputDirectory = getExchangeDirectory()
                .resolve(Integer.toString(localFileSystemSinkHandle.getTaskPartitionId()))
                .resolve(Integer.toString(taskAttemptId));
        return new LocalFileSystemExchangeSinkInstanceHandle(localFileSystemSinkHandle, outputDirectory, outputPartitionCount);
    }

    @Override
    public void sinkFinished(ExchangeSinkInstanceHandle handle)
    {
        synchronized (this) {
            LocalFileSystemExchangeSinkInstanceHandle localHandle = (LocalFileSystemExchangeSinkInstanceHandle) handle;
            finishedSinks.add(localHandle.getSinkHandle());
        }
        checkInputReady();
    }

    private void checkInputReady()
    {
        verify(!Thread.holdsLock(this));
        List<ExchangeSourceHandle> exchangeSourceHandles = null;
        synchronized (this) {
            if (exchangeSourceHandlesCreated) {
                return;
            }
            if (noMoreSinks && finishedSinks.containsAll(allSinks)) {
                // input is ready, create exchange source handles
                exchangeSourceHandles = createExchangeSourceHandles();
                exchangeSourceHandlesCreated = true;
            }
        }
        if (exchangeSourceHandles != null) {
            exchangeSourceHandlesFuture.complete(exchangeSourceHandles);
        }
    }

    private synchronized List<ExchangeSourceHandle> createExchangeSourceHandles()
    {
        Multimap<Integer, String> partitionFiles = ArrayListMultimap.create();
        for (LocalFileSystemExchangeSinkHandle sinkHandle : finishedSinks) {
            Path committedAttemptPath = getCommittedAttemptPath(sinkHandle);
            Map<Integer, Path> partitions = getCommittedPartitions(committedAttemptPath);
            partitions.forEach((partition, file) -> partitionFiles.put(partition, file.toAbsolutePath().toString()));
        }

        ImmutableList.Builder<ExchangeSourceHandle> result = ImmutableList.builder();
        for (Integer partitionId : partitionFiles.keySet()) {
            result.add(new LocalFileSystemExchangeSourceHandle(partitionId, ImmutableList.copyOf(partitionFiles.get(partitionId))));
        }
        return result.build();
    }

    private Path getCommittedAttemptPath(LocalFileSystemExchangeSinkHandle sinkHandle)
    {
        Path sinkOutputBasePath = getExchangeDirectory()
                .resolve(Integer.toString(sinkHandle.getTaskPartitionId()));
        try {
            List<Path> attemptPaths = listFiles(sinkOutputBasePath, Files::isDirectory);
            checkState(!attemptPaths.isEmpty(), "no attempts found for sink %s", sinkHandle);

            List<Path> committedAttemptPaths = attemptPaths.stream()
                    .filter(LocalFileSystemExchange::isCommitted)
                    .collect(toImmutableList());
            checkState(!committedAttemptPaths.isEmpty(), "no committed attempts found for %s", sinkHandle);

            return committedAttemptPaths.get(0);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static boolean isCommitted(Path attemptPath)
    {
        Path commitMarkerFilePath = attemptPath.resolve(COMMITTED_MARKER_FILE_NAME);
        return Files.exists(commitMarkerFilePath);
    }

    private static Map<Integer, Path> getCommittedPartitions(Path committedAttemptPath)
    {
        try {
            List<Path> partitionFiles = listFiles(committedAttemptPath, path -> path.toString().endsWith(DATA_FILE_SUFFIX));
            ImmutableMap.Builder<Integer, Path> result = ImmutableMap.builder();
            for (Path partitionFile : partitionFiles) {
                Matcher matcher = PARTITION_FILE_NAME_PATTERN.matcher(partitionFile.getFileName().toString());
                checkState(matcher.matches(), "unexpected partition file: %s", partitionFile);
                int partitionId = Integer.parseInt(matcher.group(1));
                result.put(partitionId, partitionFile);
            }
            return result.build();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Path getExchangeDirectory()
    {
        return baseDirectory.resolve(exchangeContext.getQueryId() + "." + exchangeContext.getExchangeId());
    }

    @Override
    public CompletableFuture<List<ExchangeSourceHandle>> getSourceHandles()
    {
        return exchangeSourceHandlesFuture;
    }

    @Override
    public ExchangeSourceSplitter split(ExchangeSourceHandle handle, long targetSizeInBytes)
    {
        // always split for testing
        LocalFileSystemExchangeSourceHandle localHandle = (LocalFileSystemExchangeSourceHandle) handle;
        Iterator<String> filesIterator = localHandle.getFiles().iterator();
        return new ExchangeSourceSplitter()
        {
            @Override
            public CompletableFuture<?> isBlocked()
            {
                return completedFuture(null);
            }

            @Override
            public Optional<ExchangeSourceHandle> getNext()
            {
                if (filesIterator.hasNext()) {
                    return Optional.of(new LocalFileSystemExchangeSourceHandle(localHandle.getPartitionId(), ImmutableList.of(filesIterator.next())));
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
        LocalFileSystemExchangeSourceHandle localHandle = (LocalFileSystemExchangeSourceHandle) handle;
        long sizeInBytes = 0;
        for (String file : localHandle.getFiles()) {
            try {
                sizeInBytes += Files.size(Paths.get(file));
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return new ExchangeSourceStatistics(sizeInBytes);
    }

    @Override
    public void close()
    {
        try {
            Path exchangeDirectory = getExchangeDirectory();
            if (exists(exchangeDirectory)) {
                deleteRecursively(exchangeDirectory, ALLOW_INSECURE);
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static List<Path> listFiles(Path directory, Predicate<Path> predicate)
            throws IOException
    {
        ImmutableList.Builder<Path> builder = ImmutableList.builder();
        try (Stream<Path> dir = Files.list(directory)) {
            dir.filter(predicate).forEach(builder::add);
        }
        return builder.build();
    }
}
