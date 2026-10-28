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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.tracing.Tracing;
import io.opentelemetry.api.trace.Span;
import io.trino.plugin.exchange.filesystem.local.LocalFileSystemExchangeStorage;
import io.trino.spi.QueryId;
import io.trino.spi.exchange.ExchangeContext;
import io.trino.spi.exchange.ExchangeId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.slice.Slices.utf8Slice;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestFileSystemExchangeCommitManifest
{
    @Test
    void testDiscoveryAndCleanupWithoutListing(@TempDir Path temporaryDirectory)
            throws Exception
    {
        var storage = new RecordingStorage();
        FileSystemExchangeSinkInstanceHandle instance;
        try (ExecutorService executor = newDirectExecutorService();
                FileSystemExchange exchange = createExchange(temporaryDirectory, storage, executor, true)) {
            var sinkHandle = exchange.addSink(0);
            instance = (FileSystemExchangeSinkInstanceHandle) exchange.instantiateSink(sinkHandle, 0).get();
            writeData(storage, instance);
            exchange.noMoreSinks();
            exchange.sinkFinished(sinkHandle, 0);
            exchange.allRequiredSinksFinished();

            var handles = exchange.getSourceHandles().getNextBatch().get(10, SECONDS).handles();
            assertThat(handles).extracting(handle -> handle.getPartitionId()).containsExactlyInAnyOrder(0, 1);
        }
        assertThat(storage.deletedDirectories).isEmpty();
        assertThat(storage.deletedFiles).containsExactlyInAnyOrder(
                instance.getOutputDirectory().resolve("0_0.data"),
                instance.getOutputDirectory().resolve("1_0.data"),
                instance.getOutputDirectory().resolve(CommitManifest.FILE_NAME));
        for (URI file : storage.deletedFiles) {
            assertThat(Path.of(file)).doesNotExist();
        }
    }

    @Test
    void testCleanupOfUncommittedAttempt(@TempDir Path temporaryDirectory)
            throws Exception
    {
        var storage = new RecordingStorage();
        FileSystemExchangeSinkInstanceHandle uncommitted;
        try (ExecutorService executor = newDirectExecutorService();
                FileSystemExchange exchange = createExchange(temporaryDirectory, storage, executor, true)) {
            var sinkHandle = exchange.addSink(0);
            uncommitted = (FileSystemExchangeSinkInstanceHandle) exchange.instantiateSink(sinkHandle, 0).get();
            getFutureValue(storage.createEmptyFile(uncommitted.getOutputDirectory().resolve("0_0.data")));
            var committed = (FileSystemExchangeSinkInstanceHandle) exchange.instantiateSink(sinkHandle, 1).get();
            writeData(storage, committed);
            exchange.noMoreSinks();
            exchange.sinkFinished(sinkHandle, 1);
            exchange.allRequiredSinksFinished();
            exchange.getSourceHandles().getNextBatch().get(10, SECONDS);
        }
        assertThat(storage.deletedDirectories).containsExactly(uncommitted.getOutputDirectory());
        assertThat(Path.of(uncommitted.getOutputDirectory())).doesNotExist();
        assertThat(storage.deletedFiles).hasSize(3);
    }

    @Test
    void testMissingManifestFailsWithoutListing(@TempDir Path temporaryDirectory)
            throws Exception
    {
        var storage = new RecordingStorage();
        try (ExecutorService executor = newDirectExecutorService();
                FileSystemExchange exchange = createExchange(temporaryDirectory, storage, executor, true)) {
            var sinkHandle = exchange.addSink(0);
            var instance = (FileSystemExchangeSinkInstanceHandle) exchange.instantiateSink(sinkHandle, 0).get();
            writeData(storage, instance);
            Files.delete(Path.of(instance.getOutputDirectory().resolve(CommitManifest.FILE_NAME)));
            exchange.noMoreSinks();
            exchange.sinkFinished(sinkHandle, 0);
            exchange.allRequiredSinksFinished();

            assertThatThrownBy(() -> exchange.getSourceHandles().getNextBatch().get(10, SECONDS))
                    .hasRootCauseInstanceOf(NoSuchFileException.class);
        }
    }

    @Test
    void testCleanupWithoutManifestRemovesTaskDirectory(@TempDir Path temporaryDirectory)
            throws Exception
    {
        var storage = new RecordingStorage();
        URI taskDirectory;
        try (ExecutorService executor = newDirectExecutorService();
                FileSystemExchange exchange = createExchange(temporaryDirectory, storage, executor, false)) {
            var sinkHandle = exchange.addSink(0);
            var firstAttempt = (FileSystemExchangeSinkInstanceHandle) exchange.instantiateSink(sinkHandle, 0).get();
            var secondAttempt = (FileSystemExchangeSinkInstanceHandle) exchange.instantiateSink(sinkHandle, 1).get();
            getFutureValue(storage.createEmptyFile(firstAttempt.getOutputDirectory().resolve("0_0.data")));
            getFutureValue(storage.createEmptyFile(secondAttempt.getOutputDirectory().resolve("0_0.data")));
            taskDirectory = firstAttempt.getOutputDirectory().resolve("../");
        }
        assertThat(storage.deletedDirectories).containsExactly(taskDirectory);
        assertThat(storage.deletedFiles).isEmpty();
        assertThat(Path.of(taskDirectory)).doesNotExist();
    }

    @Test
    void testUnsupportedManifestStorage()
    {
        for (String baseDirectory : ImmutableList.of("gs://bucket/exchange", "abfs://container@account.dfs.core.windows.net/exchange", "abfss://container@account.dfs.core.windows.net/exchange")) {
            assertThatThrownBy(() -> new FileSystemExchangeManagerFactory().create(
                    ImmutableMap.of("exchange.base-directories", baseDirectory, "exchange.commit-manifest-enabled", "true"),
                    new TestExchangeManagerContext()))
                    .hasMessageContaining("Commit manifests are not supported for exchange spooling storage scheme " + URI.create(baseDirectory).getScheme());
        }
    }

    private static FileSystemExchange createExchange(Path baseDirectory, FileSystemExchangeStorage storage, ExecutorService executor, boolean commitManifestEnabled)
    {
        return new FileSystemExchange(
                ImmutableList.of(baseDirectory.toUri()),
                storage,
                new FileSystemExchangeStats(),
                Tracing.noopTracer(),
                new TestExchangeContext(),
                2,
                false,
                commitManifestEnabled,
                1,
                1024,
                executor);
    }

    private static void writeData(FileSystemExchangeStorage storage, FileSystemExchangeSinkInstanceHandle instance)
    {
        var sink = new FileSystemExchangeSink(storage, new FileSystemExchangeStats(), instance.getOutputDirectory(), 2, false, 1024, 1, 1, 1024, true);
        sink.add(0, utf8Slice("first"));
        sink.add(1, utf8Slice("second"));
        getFutureValue(sink.finish());
    }

    private static class RecordingStorage
            extends LocalFileSystemExchangeStorage
    {
        private final List<URI> deletedFiles = new ArrayList<>();
        private final List<URI> deletedDirectories = new ArrayList<>();

        @Override
        public ListenableFuture<List<FileStatus>> listFilesRecursively(URI directory)
        {
            throw new AssertionError("Manifest discovery must not list files");
        }

        @Override
        public ListenableFuture<Void> deleteFiles(List<URI> files)
        {
            deletedFiles.addAll(files);
            return super.deleteFiles(files);
        }

        @Override
        public ListenableFuture<Void> deleteRecursively(List<URI> directories)
        {
            deletedDirectories.addAll(directories);
            return super.deleteRecursively(directories);
        }
    }

    private static class TestExchangeContext
            implements ExchangeContext
    {
        private final ExchangeId exchangeId = ExchangeId.createRandomExchangeId();

        @Override
        public QueryId getQueryId()
        {
            return new QueryId("query");
        }

        @Override
        public ExchangeId getExchangeId()
        {
            return exchangeId;
        }

        @Override
        public Span getParentSpan()
        {
            return Span.getInvalid();
        }
    }
}
