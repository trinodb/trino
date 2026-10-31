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
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.trino.plugin.exchange.filesystem.local.LocalFileSystemExchangeStorage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.slice.Slices.utf8Slice;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestFileSystemExchangeSink
{
    @Test
    void testAbortDeletesOutputAndPreventsFinish(@TempDir Path temporaryDirectory)
            throws IOException
    {
        for (boolean commitManifestEnabled : ImmutableList.of(false, true)) {
            Path outputDirectory = Files.createDirectory(temporaryDirectory.resolve("sink-" + commitManifestEnabled));
            FileSystemExchangeSink sink = createSink(new LocalFileSystemExchangeStorage(), outputDirectory, commitManifestEnabled);
            sink.add(0, utf8Slice("data"));

            getFutureValue(sink.abort());
            assertThat(outputDirectory).doesNotExist();
            assertThatThrownBy(() -> getFutureValue(sink.finish()))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("Exchange sink has already closed");
        }
    }

    @Test
    void testAbortDuringFinishDoesNotDeleteCommittedOutput(@TempDir Path temporaryDirectory)
            throws IOException
    {
        for (boolean commitManifestEnabled : ImmutableList.of(false, true)) {
            Path outputDirectory = Files.createDirectory(temporaryDirectory.resolve("sink-" + commitManifestEnabled));
            BlockingCommitStorage storage = new BlockingCommitStorage();
            FileSystemExchangeSink sink = createSink(storage, outputDirectory, commitManifestEnabled);
            sink.add(0, utf8Slice("data"));

            CompletableFuture<Void> finish = sink.finish();
            assertThat(finish).isNotDone();
            assertThat(outputDirectory.resolve(CommitManifest.FILE_NAME)).exists();

            getFutureValue(sink.abort());
            assertThat(outputDirectory).exists();

            storage.completeCommit();
            getFutureValue(finish);
            assertThat(outputDirectory.resolve("0_0.data")).exists();
            assertThat(outputDirectory.resolve(CommitManifest.FILE_NAME)).exists();
            if (commitManifestEnabled) {
                assertThat(getFutureValue(storage.readMarkerFile(outputDirectory.toUri())).files())
                        .containsExactly(new FileStatus(outputDirectory.resolve("0_0.data").toUri().toString(), 8));
            }
        }
    }

    @Test
    void testFinishFailureDeletesOutput(@TempDir Path temporaryDirectory)
            throws IOException
    {
        for (boolean commitManifestEnabled : ImmutableList.of(false, true)) {
            Path outputDirectory = Files.createDirectory(temporaryDirectory.resolve("sink-" + commitManifestEnabled));
            BlockingCommitStorage storage = new BlockingCommitStorage();
            FileSystemExchangeSink sink = createSink(storage, outputDirectory, commitManifestEnabled);
            sink.add(0, utf8Slice("data"));

            CompletableFuture<Void> finish = sink.finish();
            assertThat(finish).isNotDone();
            assertThat(outputDirectory.resolve(CommitManifest.FILE_NAME)).exists();

            storage.failCommit(new IOException("commit failed"));

            assertThatThrownBy(finish::join)
                    .hasRootCauseInstanceOf(IOException.class)
                    .hasRootCauseMessage("commit failed");
            assertThat(outputDirectory).doesNotExist();
        }
    }

    private static FileSystemExchangeSink createSink(FileSystemExchangeStorage storage, Path outputDirectory, boolean commitManifestEnabled)
    {
        return new FileSystemExchangeSink(
                storage,
                new FileSystemExchangeStats(),
                outputDirectory.toUri(),
                1,
                false,
                1024,
                1,
                1,
                1024,
                commitManifestEnabled);
    }

    private static final class BlockingCommitStorage
            implements FileSystemExchangeStorage
    {
        private final LocalFileSystemExchangeStorage delegate = new LocalFileSystemExchangeStorage();
        private final SettableFuture<Void> commitFuture = SettableFuture.create();

        @Override
        public void createDirectories(URI directory)
                throws IOException
        {
            delegate.createDirectories(directory);
        }

        @Override
        public ExchangeStorageReader createExchangeStorageReader(
                List<ExchangeSourceFile> sourceFiles,
                int maxPageStorageSize,
                MetricsBuilder metricsBuilder)
        {
            return delegate.createExchangeStorageReader(sourceFiles, maxPageStorageSize, metricsBuilder);
        }

        @Override
        public ExchangeStorageWriter createExchangeStorageWriter(URI file)
        {
            return delegate.createExchangeStorageWriter(file);
        }

        @Override
        public ListenableFuture<Void> createEmptyFile(URI file)
        {
            getFutureValue(delegate.createEmptyFile(file));
            return commitFuture;
        }

        @Override
        public ListenableFuture<Void> writeMarkerFile(URI directory, CommitManifest manifest)
        {
            getFutureValue(delegate.writeMarkerFile(directory, manifest));
            return commitFuture;
        }

        @Override
        public ListenableFuture<CommitManifest> readMarkerFile(URI uri)
        {
            return delegate.readMarkerFile(uri);
        }

        @Override
        public ListenableFuture<Void> deleteFiles(List<URI> files)
        {
            return delegate.deleteFiles(files);
        }

        @Override
        public ListenableFuture<Void> deleteRecursively(List<URI> directories)
        {
            return delegate.deleteRecursively(directories);
        }

        @Override
        public ListenableFuture<List<FileStatus>> listFilesRecursively(URI directory)
        {
            return delegate.listFilesRecursively(directory);
        }

        @Override
        public int getWriteBufferSize()
        {
            return delegate.getWriteBufferSize();
        }

        @Override
        public void close()
                throws IOException
        {
            delegate.close();
        }

        public void completeCommit()
        {
            commitFuture.set(null);
        }

        public void failCommit(Throwable failure)
        {
            commitFuture.setException(failure);
        }
    }
}
