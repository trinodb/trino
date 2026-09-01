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
package io.trino.plugin.deltalake.transactionlog;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.trino.filesystem.Location;
import io.trino.filesystem.memory.MemoryFileSystem;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static com.google.common.util.concurrent.Uninterruptibles.awaitUninterruptibly;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeTransactionLogEntry.addFileEntry;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogAccess.activeAddEntries;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestActiveAddEntriesIterator
{
    @Test
    void testCloseBeforeIterationDoesNotAcquireCheckpoint()
    {
        AtomicBoolean checkpointOpened = new AtomicBoolean();
        Supplier<Stream<DeltaLakeTransactionLogEntry>> checkpointEntriesSupplier = () -> {
            checkpointOpened.set(true);
            return Stream.empty();
        };

        try (Stream<AddFileEntry> activeFiles = activeAddEntries(checkpointEntriesSupplier, ImmutableList.of(), new MemoryFileSystem())) {
            activeFiles.close();
            assertThat(checkpointOpened.get()).isFalse();
        }
    }

    @Test
    void testJsonReplayFailureDoesNotAcquireCheckpoint()
            throws IOException
    {
        MemoryFileSystem fileSystem = new MemoryFileSystem();
        Transaction transaction = createTransaction(fileSystem, "{invalid-json}\n");
        AtomicBoolean checkpointOpened = new AtomicBoolean();
        Supplier<Stream<DeltaLakeTransactionLogEntry>> checkpointEntriesSupplier = () -> {
            checkpointOpened.set(true);
            return Stream.empty();
        };

        try (Stream<AddFileEntry> activeFiles = activeAddEntries(checkpointEntriesSupplier, ImmutableList.of(transaction), fileSystem)) {
            assertThatThrownBy(() -> activeFiles.iterator().hasNext())
                    .isInstanceOf(RuntimeException.class);
            assertThat(checkpointOpened.get()).isFalse();
        }
    }

    @Test
    void testCheckpointAcquisitionFailure()
    {
        RuntimeException acquisitionFailure = new RuntimeException("checkpoint acquisition failed");
        Supplier<Stream<DeltaLakeTransactionLogEntry>> checkpointEntriesSupplier = () -> {
            throw acquisitionFailure;
        };

        try (Stream<AddFileEntry> activeFiles = activeAddEntries(checkpointEntriesSupplier, ImmutableList.of(), new MemoryFileSystem())) {
            assertThatThrownBy(() -> activeFiles.iterator().hasNext())
                    .isSameAs(acquisitionFailure);
        }
    }

    @Test
    void testCheckpointReadFailureSuppressesCloseFailure()
    {
        RuntimeException readFailure = new RuntimeException("checkpoint read failed");
        RuntimeException closeFailure = new RuntimeException("checkpoint close failed");
        Stream<DeltaLakeTransactionLogEntry> checkpoint = Stream.<DeltaLakeTransactionLogEntry>generate(() -> {
            throw readFailure;
        }).onClose(() -> {
            throw closeFailure;
        });

        try (Stream<AddFileEntry> activeFiles = activeAddEntries(() -> checkpoint, ImmutableList.of(), new MemoryFileSystem())) {
            assertThatThrownBy(() -> activeFiles.iterator().hasNext())
                    .isSameAs(readFailure);
            assertThat(readFailure.getSuppressed()).containsExactly(closeFailure);
        }
    }

    @Test
    void testCloseAfterFirstCheckpointEntryClosesCheckpointStream()
    {
        AtomicBoolean checkpointClosed = new AtomicBoolean();
        AddFileEntry file = createAddFileEntry(1, Optional.empty());
        Stream<DeltaLakeTransactionLogEntry> checkpoint = Stream.generate(() -> addFileEntry(file))
                .onClose(() -> checkpointClosed.set(true));

        try (Stream<AddFileEntry> activeFiles = activeAddEntries(() -> checkpoint, ImmutableList.of(), new MemoryFileSystem())) {
            assertThat(activeFiles.findFirst()).contains(file);
            assertThat(checkpointClosed.get()).isFalse();
        }
        assertThat(checkpointClosed.get()).isTrue();
    }

    @Test
    void testCheckpointExhaustionClosesCheckpointStream()
    {
        AtomicBoolean checkpointClosed = new AtomicBoolean();
        AddFileEntry file = createAddFileEntry(1, Optional.empty());
        Stream<DeltaLakeTransactionLogEntry> checkpoint = Stream.of(addFileEntry(file))
                .onClose(() -> checkpointClosed.set(true));

        try (Stream<AddFileEntry> activeFiles = activeAddEntries(() -> checkpoint, ImmutableList.of(), new MemoryFileSystem())) {
            assertThat(activeFiles.toList()).containsExactly(file);
            assertThat(checkpointClosed.get()).isTrue();
        }
    }

    @Test
    void testCloseDuringCheckpointAcquisitionClosesCheckpointStream()
            throws Exception
    {
        CountDownLatch checkpointAcquisitionStarted = new CountDownLatch(1);
        CountDownLatch finishCheckpointAcquisition = new CountDownLatch(1);
        AtomicBoolean checkpointClosed = new AtomicBoolean();

        Supplier<Stream<DeltaLakeTransactionLogEntry>> checkpointEntriesSupplier = () -> {
            checkpointAcquisitionStarted.countDown();
            assertThat(awaitUninterruptibly(finishCheckpointAcquisition, 10, SECONDS)).isTrue();
            return Stream.<DeltaLakeTransactionLogEntry>empty()
                    .onClose(() -> checkpointClosed.set(true));
        };

        try (ExecutorService iterationExecutor = newSingleThreadExecutor(daemonThreadsNamed("test-active-add-entries-iteration-%s"));
                ExecutorService closeExecutor = newSingleThreadExecutor(daemonThreadsNamed("test-active-add-entries-close-%s"));
                Stream<AddFileEntry> activeFiles = activeAddEntries(checkpointEntriesSupplier, ImmutableList.of(), new MemoryFileSystem())) {
            Future<Boolean> checkpointIteration = iterationExecutor.submit(() -> activeFiles.iterator().hasNext());

            try {
                assertThat(checkpointAcquisitionStarted.await(10, SECONDS)).isTrue();

                closeExecutor.submit(activeFiles::close).get(10, SECONDS);
                finishCheckpointAcquisition.countDown();

                assertThat(checkpointIteration.get(10, SECONDS)).isFalse();
                assertThat(checkpointClosed.get()).isTrue();
            }
            finally {
                finishCheckpointAcquisition.countDown();
            }
        }
    }

    @Test
    void testAddWinsOverRemoveInSameTransaction()
            throws IOException
    {
        assertAddWinsOverRemoveInSameTransaction(true);
        assertAddWinsOverRemoveInSameTransaction(false);
    }

    private static void assertAddWinsOverRemoveInSameTransaction(boolean addFirst)
            throws IOException
    {
        MemoryFileSystem fileSystem = new MemoryFileSystem();
        String add =
                """
                {"add":{"path":"file.parquet","partitionValues":{},"size":1,"modificationTime":2,"dataChange":true}}
                """;
        String remove =
                """
                {"remove":{"path":"file.parquet","deletionTimestamp":1,"dataChange":true}}
                """;
        Transaction transaction = createTransaction(fileSystem, addFirst ? add + remove : remove + add);
        AddFileEntry checkpointFile = createAddFileEntry(1, Optional.empty());

        try (Stream<AddFileEntry> activeFiles = activeAddEntries(() -> Stream.of(addFileEntry(checkpointFile)), ImmutableList.of(transaction), fileSystem)) {
            assertThat(activeFiles.toList())
                    .extracting(AddFileEntry::getModificationTime)
                    .containsExactly(2L);
        }
    }

    @Test
    void testRemovalMatchesDeletionVectorIdentity()
            throws IOException
    {
        MemoryFileSystem fileSystem = new MemoryFileSystem();
        DeletionVectorEntry removedDeletionVector = new DeletionVectorEntry("u", "vector", OptionalInt.of(1), 1, 1);
        DeletionVectorEntry retainedDeletionVector = new DeletionVectorEntry("u", "vector", OptionalInt.of(2), 1, 1);
        Transaction transaction = createTransaction(fileSystem,
                """
                {"remove":{"path":"file.parquet","deletionTimestamp":1,"dataChange":true,"deletionVector":{"storageType":"u","pathOrInlineDv":"vector","offset":1,"sizeInBytes":1,"cardinality":1}}}
                """);
        AddFileEntry removedFile = createAddFileEntry(1, Optional.of(removedDeletionVector));
        AddFileEntry retainedFile = createAddFileEntry(1, Optional.of(retainedDeletionVector));

        try (Stream<AddFileEntry> activeFiles = activeAddEntries(
                () -> Stream.of(addFileEntry(removedFile), addFileEntry(retainedFile)),
                ImmutableList.of(transaction),
                fileSystem)) {
            assertThat(activeFiles.toList()).containsExactly(retainedFile);
        }
    }

    private static AddFileEntry createAddFileEntry(long modificationTime, Optional<DeletionVectorEntry> deletionVector)
    {
        return new AddFileEntry(
                "file.parquet",
                ImmutableMap.of(),
                1,
                modificationTime,
                true,
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of(),
                deletionVector);
    }

    private static Transaction createTransaction(MemoryFileSystem fileSystem, String contents)
            throws IOException
    {
        Location transactionLogPath = Location.of("memory:///delta/00000000000000000001.json");
        fileSystem.newOutputFile(transactionLogPath).createOrOverwrite(contents.getBytes(UTF_8));
        TransactionLogEntries transactionLogEntries = new TransactionLogEntries(
                1,
                fileSystem.newInputFile(transactionLogPath),
                DataSize.ofBytes(0));
        return new Transaction(1, transactionLogEntries);
    }
}
