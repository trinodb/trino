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
package io.trino.plugin.iceberg.delete;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.plugin.iceberg.IcebergColumnHandle;
import io.trino.plugin.iceberg.delete.DeleteManager.SerializedDeletionVector;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.FixedPageSource;
import io.trino.spi.connector.MemoryContext;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.predicate.TupleDomain;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Verify.verify;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.google.common.util.concurrent.Uninterruptibles.awaitUninterruptibly;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.MoreFutures.whenAnyComplete;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_BAD_DATA;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_CANNOT_OPEN_SPLIT;
import static io.trino.plugin.iceberg.IcebergTestUtils.BLOCKS_HASH_FACTORY;
import static io.trino.plugin.iceberg.IcebergUtil.getColumnHandle;
import static io.trino.spi.StandardErrorCode.EXCEEDED_LOCAL_MEMORY_LIMIT;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.assertions.Assert.assertEventually;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.apache.iceberg.MetadataColumns.ROW_POSITION;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestDeleteManager
{
    private static final String DATA_FILE_PATH = "s3://bucket/data.parquet";
    private static final int KEY_FIELD_ID = 1;
    private static final Schema SCHEMA = new Schema(optional(KEY_FIELD_ID, "key", Types.LongType.get()));
    private static final IcebergColumnHandle ROW_POSITION_HANDLE = getColumnHandle(ROW_POSITION, TESTING_TYPE_MANAGER);

    private ExecutorService executor;

    @BeforeEach
    void setUp()
    {
        executor = Executors.newFixedThreadPool(4);
    }

    @AfterEach
    void tearDown()
    {
        executor.shutdownNow();
    }

    @Test
    void testDeletionVectorMemoryTracked()
    {
        DeletionVector deletionVector = DeletionVector.builder()
                .add(0)
                .add(3)
                .build()
                .orElseThrow();

        DeleteFile deletionVectorFile = new DeleteFile(
                FileContent.POSITION_DELETES,
                "s3://bucket/deletion-vector.puffin",
                FileFormat.PUFFIN,
                2,
                100,
                ImmutableList.of(),
                OptionalLong.empty(),
                OptionalLong.empty(),
                1,
                OptionalLong.of(4),
                Optional.of(40),
                Optional.empty());

        LocalMemoryContext memoryContext = newSimpleAggregatedMemoryContext().newLocalMemoryContext("test");
        DeleteManager manager = new DeleteManager(TESTING_TYPE_MANAGER, BLOCKS_HASH_FACTORY, () -> {}, executor);
        Optional<PageFilter> pageFilter = getFutureValue(manager.createDeletePageFilter(
                DATA_FILE_PATH,
                OptionalLong.of(1),
                ImmutableList.of(deletionVectorFile),
                ImmutableList.of(ROW_POSITION_HANDLE),
                SCHEMA,
                OptionalLong.empty(),
                OptionalLong.empty(),
                _ -> new SerializedDeletionVector(deletionVector.serialize(), () -> {}),
                (_, _, _) -> {
                    throw new UnsupportedOperationException();
                },
                (_, _, _) -> {
                    throw new UnsupportedOperationException();
                },
                directExecutor(),
                memoryContext::setBytes));

        assertThat(memoryContext.getBytes()).isEqualTo(DeletionVector.builder().add(0).add(3).build().orElseThrow().retainedSizeInBytes());
        assertThat(filterRowPositions(pageFilter.orElseThrow(), 0, 1, 2, 3, 4)).isEqualTo(3);
    }

    @Test
    void testPositionDeleteMemoryTracked()
    {
        DeleteFile positionDeleteFile = new DeleteFile(
                FileContent.POSITION_DELETES,
                "s3://bucket/delete-1.parquet",
                FileFormat.PARQUET,
                2,
                100,
                ImmutableList.of(),
                OptionalLong.empty(),
                OptionalLong.empty(),
                1,
                OptionalLong.empty(),
                Optional.empty(),
                Optional.empty());

        LocalMemoryContext memoryContext = newSimpleAggregatedMemoryContext().newLocalMemoryContext("test");
        DeleteManager manager = new DeleteManager(TESTING_TYPE_MANAGER, BLOCKS_HASH_FACTORY, () -> {}, executor);
        Optional<PageFilter> pageFilter = getFutureValue(manager.createDeletePageFilter(
                DATA_FILE_PATH,
                OptionalLong.of(1),
                ImmutableList.of(positionDeleteFile),
                ImmutableList.of(ROW_POSITION_HANDLE),
                SCHEMA,
                OptionalLong.empty(),
                OptionalLong.empty(),
                _ -> {
                    throw new UnsupportedOperationException();
                },
                (_, _, _) -> positionDeletePageSource(0, 3),
                (_, _, _) -> {
                    throw new UnsupportedOperationException();
                },
                directExecutor(),
                memoryContext::setBytes));

        assertThat(memoryContext.getBytes()).isEqualTo(DeletionVector.builder().add(0).add(3).build().orElseThrow().retainedSizeInBytes());
        assertThat(filterRowPositions(pageFilter.orElseThrow(), 0, 1, 2, 3, 4)).isEqualTo(3);
    }

    /**
     * Position delete files, deletion vectors and equality delete files are read on the pool and decoded on the decode
     * executor, which the page source runs on the thread reading the split.
     */
    @Test
    void testDeletesOpenOnPoolAndDecodeOnDecodeExecutor()
            throws Exception
    {
        DeleteManager manager = new DeleteManager(TESTING_TYPE_MANAGER, BLOCKS_HASH_FACTORY, () -> {}, executor);
        AtomicReference<Thread> openThread = new AtomicReference<>();
        AtomicReference<Thread> decodeThread = new AtomicReference<>();

        ThreadlessExecutor split = new ThreadlessExecutor();
        ListenableFuture<Optional<PageFilter>> positionFilter = manager.createDeletePageFilter(
                DATA_FILE_PATH,
                OptionalLong.empty(),
                ImmutableList.of(positionDeleteFile("delete-1", 1L)),
                ImmutableList.of(ROW_POSITION_HANDLE),
                SCHEMA,
                OptionalLong.empty(),
                OptionalLong.empty(),
                _ -> { throw new UnsupportedOperationException(); },
                (_, _, _) -> {
                    openThread.set(Thread.currentThread());
                    return new RecordingPageSource(positionDeletePages(1), decodeThread);
                },
                (_, _, _) -> { throw new UnsupportedOperationException(); },
                split,
                MemoryContext.NO_LIMIT);

        // the file is opened, but nothing is decoded until the split runs its decodes
        split.taskAvailable().get(5, TimeUnit.SECONDS);
        assertThat(decodeThread.get()).isNull();
        Optional<PageFilter> pageFilter = runUntilDone(split, positionFilter);
        assertThat(filterRowPositions(pageFilter.orElseThrow(), 0, 1, 2)).isEqualTo(2);
        assertThat(openThread.get()).isNotNull().isNotSameAs(Thread.currentThread());
        assertThat(decodeThread.get()).isSameAs(Thread.currentThread());

        AtomicReference<Thread> deletionVectorReadThread = new AtomicReference<>();
        // the serialized vector is released once it is deserialized
        AtomicReference<Thread> deserializeThread = new AtomicReference<>();
        DeleteFile deletionVectorFile = new DeleteFile(
                FileContent.POSITION_DELETES,
                "dv",
                FileFormat.PUFFIN,
                1,
                100,
                ImmutableList.of(),
                OptionalLong.empty(),
                OptionalLong.empty(),
                1,
                OptionalLong.of(4),
                Optional.of(40),
                Optional.empty());
        ThreadlessExecutor deletionVectorSplit = new ThreadlessExecutor();
        ListenableFuture<Optional<PageFilter>> deletionVectorFilter = manager.createDeletePageFilter(
                DATA_FILE_PATH,
                OptionalLong.empty(),
                ImmutableList.of(deletionVectorFile),
                ImmutableList.of(ROW_POSITION_HANDLE),
                SCHEMA,
                OptionalLong.empty(),
                OptionalLong.empty(),
                _ -> {
                    deletionVectorReadThread.set(Thread.currentThread());
                    return new SerializedDeletionVector(DeletionVector.builder().add(0).build().orElseThrow().serialize(), () -> deserializeThread.set(Thread.currentThread()));
                },
                (_, _, _) -> { throw new UnsupportedOperationException(); },
                (_, _, _) -> { throw new UnsupportedOperationException(); },
                deletionVectorSplit,
                MemoryContext.NO_LIMIT);

        // the deletion vector is read, but nothing is deserialized until the split runs its decodes
        deletionVectorSplit.taskAvailable().get(5, TimeUnit.SECONDS);
        assertThat(deletionVectorFilter.isDone()).isFalse();
        assertThat(filterRowPositions(runUntilDone(deletionVectorSplit, deletionVectorFilter).orElseThrow(), 0, 1)).isEqualTo(1);
        assertThat(deletionVectorReadThread.get()).isNotNull().isNotSameAs(Thread.currentThread());
        assertThat(deserializeThread.get()).isSameAs(Thread.currentThread());

        // the equality delete file is shared by the splits, and its hash is built on the decode executor too
        AtomicReference<Thread> equalityOpenThread = new AtomicReference<>();
        AtomicReference<Thread> equalityDecodeThread = new AtomicReference<>();
        ThreadlessExecutor equalitySplit = new ThreadlessExecutor();
        ListenableFuture<Optional<PageFilter>> equalityFilter = manager.createDeletePageFilter(
                DATA_FILE_PATH,
                OptionalLong.of(5L),
                ImmutableList.of(equalityDeleteFile("eq-delete", 10L, ImmutableList.of(KEY_FIELD_ID))),
                ImmutableList.of(getColumnHandle(SCHEMA.findField(KEY_FIELD_ID), TESTING_TYPE_MANAGER)),
                SCHEMA,
                OptionalLong.empty(),
                OptionalLong.empty(),
                _ -> { throw new UnsupportedOperationException(); },
                (_, _, _) -> { throw new UnsupportedOperationException(); },
                (_, _, _) -> {
                    equalityOpenThread.set(Thread.currentThread());
                    return new RecordingPageSource(ImmutableList.of(keyPage()), equalityDecodeThread);
                },
                equalitySplit,
                MemoryContext.NO_LIMIT);

        // the file is opened, but its hash is not built until the split runs its decodes
        equalitySplit.taskAvailable().get(5, TimeUnit.SECONDS);
        assertThat(equalityDecodeThread.get()).isNull();
        assertThat(filterKeys(runUntilDone(equalitySplit, equalityFilter).orElseThrow(), 1, 2)).isEqualTo(1);
        assertThat(equalityOpenThread.get()).isNotNull().isNotSameAs(Thread.currentThread());
        assertThat(equalityDecodeThread.get()).isSameAs(Thread.currentThread());
    }

    /**
     * A split that gives up after its delete file was opened, but before it decoded it, closes the opened page source.
     */
    @Test
    void testCanceledLoadClosesOpenedPageSource()
            throws Exception
    {
        DeleteManager manager = new DeleteManager(TESTING_TYPE_MANAGER, BLOCKS_HASH_FACTORY, () -> {}, executor);
        AtomicReference<Thread> decodeThread = new AtomicReference<>();
        RecordingPageSource pageSource = new RecordingPageSource(positionDeletePages(1), decodeThread);

        ThreadlessExecutor split = new ThreadlessExecutor();
        ListenableFuture<Optional<PageFilter>> future = manager.createDeletePageFilter(
                DATA_FILE_PATH,
                OptionalLong.empty(),
                ImmutableList.of(positionDeleteFile("delete-1", 1L)),
                ImmutableList.of(ROW_POSITION_HANDLE),
                SCHEMA,
                OptionalLong.empty(),
                OptionalLong.empty(),
                _ -> { throw new UnsupportedOperationException(); },
                (_, _, _) -> pageSource,
                (_, _, _) -> { throw new UnsupportedOperationException(); },
                split,
                MemoryContext.NO_LIMIT);

        // the file is opened and its decode is queued, then the split is closed before running it
        split.taskAvailable().get(5, TimeUnit.SECONDS);
        future.cancel(true);
        split.close();

        assertThat(pageSource.isClosed()).isTrue();
        assertThat(decodeThread.get()).isNull();
    }

    /**
     * A split that gives up while its delete file is still being opened closes the page source once the open completes,
     * even when the open ignores the interrupt, as one blocked in IO can.
     */
    @Test
    void testLoadCanceledWhileOpeningClosesOpenedPageSource()
            throws Exception
    {
        DeleteManager manager = new DeleteManager(TESTING_TYPE_MANAGER, BLOCKS_HASH_FACTORY, () -> {}, executor);
        AtomicReference<Thread> decodeThread = new AtomicReference<>();
        RecordingPageSource pageSource = new RecordingPageSource(positionDeletePages(1), decodeThread);
        CountDownLatch opening = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);

        ThreadlessExecutor split = new ThreadlessExecutor();
        ListenableFuture<Optional<PageFilter>> future = manager.createDeletePageFilter(
                DATA_FILE_PATH,
                OptionalLong.empty(),
                ImmutableList.of(positionDeleteFile("delete-1", 1L)),
                ImmutableList.of(ROW_POSITION_HANDLE),
                SCHEMA,
                OptionalLong.empty(),
                OptionalLong.empty(),
                _ -> { throw new UnsupportedOperationException(); },
                (_, _, _) -> {
                    opening.countDown();
                    awaitUninterruptibly(release);
                    return pageSource;
                },
                (_, _, _) -> { throw new UnsupportedOperationException(); },
                split,
                MemoryContext.NO_LIMIT);

        assertThat(opening.await(5, TimeUnit.SECONDS)).isTrue();
        future.cancel(true);
        split.close();
        release.countDown();

        assertEventually(() -> assertThat(pageSource.isClosed()).isTrue());
        assertThat(decodeThread.get()).isNull();
    }

    /**
     * A split that gives up while its deletion vector is still being read releases the memory of the read once it completes.
     */
    @Test
    void testLoadCanceledWhileReadingDeletionVectorReleasesItsMemory()
            throws Exception
    {
        DeleteManager manager = new DeleteManager(TESTING_TYPE_MANAGER, BLOCKS_HASH_FACTORY, () -> {}, executor);
        AtomicBoolean released = new AtomicBoolean();
        CountDownLatch reading = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        DeleteFile deletionVectorFile = new DeleteFile(
                FileContent.POSITION_DELETES,
                "dv",
                FileFormat.PUFFIN,
                1,
                100,
                ImmutableList.of(),
                OptionalLong.empty(),
                OptionalLong.empty(),
                1,
                OptionalLong.of(4),
                Optional.of(40),
                Optional.empty());

        ThreadlessExecutor split = new ThreadlessExecutor();
        ListenableFuture<Optional<PageFilter>> future = manager.createDeletePageFilter(
                DATA_FILE_PATH,
                OptionalLong.empty(),
                ImmutableList.of(deletionVectorFile),
                ImmutableList.of(ROW_POSITION_HANDLE),
                SCHEMA,
                OptionalLong.empty(),
                OptionalLong.empty(),
                _ -> {
                    reading.countDown();
                    awaitUninterruptibly(release);
                    return new SerializedDeletionVector(DeletionVector.builder().add(0).build().orElseThrow().serialize(), () -> released.set(true));
                },
                (_, _, _) -> { throw new UnsupportedOperationException(); },
                (_, _, _) -> { throw new UnsupportedOperationException(); },
                split,
                MemoryContext.NO_LIMIT);

        assertThat(reading.await(5, TimeUnit.SECONDS)).isTrue();
        future.cancel(true);
        split.close();
        release.countDown();

        assertEventually(() -> assertThat(released.get()).isTrue());
    }

    @Test
    void testPositionDeleteLoadFailurePropagates()
    {
        DeleteManager manager = new DeleteManager(TESTING_TYPE_MANAGER, BLOCKS_HASH_FACTORY, () -> {}, executor);

        ListenableFuture<Optional<PageFilter>> future = manager.createDeletePageFilter(
                "data-file",
                OptionalLong.empty(),
                ImmutableList.of(positionDeleteFile("pos-delete", 1L)),
                ImmutableList.of(),
                SCHEMA,
                OptionalLong.empty(),
                OptionalLong.empty(),
                _ -> { throw new RuntimeException("unexpected DV read"); },
                (_, _, _) -> { throw new TrinoException(ICEBERG_CANNOT_OPEN_SPLIT, "pos delete open failed"); },
                (_, _, _) -> {
                    throw new UnsupportedOperationException();
                },
                directExecutor(),
                MemoryContext.NO_LIMIT);

        assertThatThrownBy(future::get)
                .hasCauseInstanceOf(TrinoException.class)
                .hasMessageContaining("pos delete open failed");
    }

    @Test
    void testEqualityDeleteLoadFailurePropagates()
    {
        DeleteManager manager = new DeleteManager(TESTING_TYPE_MANAGER, BLOCKS_HASH_FACTORY, () -> {}, executor);

        ListenableFuture<Optional<PageFilter>> future = manager.createDeletePageFilter(
                "data-file",
                OptionalLong.of(5L),
                ImmutableList.of(equalityDeleteFile("eq-delete", 10L, ImmutableList.of(KEY_FIELD_ID))),
                ImmutableList.of(),
                SCHEMA,
                OptionalLong.empty(),
                OptionalLong.empty(),
                _ -> { throw new RuntimeException("unexpected DV read"); },
                (_, _, _) -> {
                    throw new UnsupportedOperationException();
                },
                (_, _, _) -> { throw new TrinoException(ICEBERG_CANNOT_OPEN_SPLIT, "eq delete open failed"); },
                directExecutor(),
                MemoryContext.NO_LIMIT);

        assertThatThrownBy(future::get)
                .hasCauseInstanceOf(TrinoException.class)
                .hasMessageContaining("eq delete open failed");
    }

    @Test
    void testAsyncLoadFailureIsReportedAsBadData()
    {
        DeleteManager manager = new DeleteManager(TESTING_TYPE_MANAGER, BLOCKS_HASH_FACTORY, () -> {}, executor);

        ListenableFuture<Optional<PageFilter>> future = manager.createDeletePageFilter(
                "data-file",
                OptionalLong.empty(),
                ImmutableList.of(positionDeleteFile("pos-delete", 1L)),
                ImmutableList.of(),
                SCHEMA,
                OptionalLong.empty(),
                OptionalLong.empty(),
                _ -> { throw new RuntimeException("unexpected DV read"); },
                // a failure that is not already a TrinoException
                (_, _, _) -> { throw new UncheckedIOException(new IOException("delete file is truncated")); },
                (_, _, _) -> {
                    throw new UnsupportedOperationException();
                },
                directExecutor(),
                MemoryContext.NO_LIMIT);

        assertThatThrownBy(future::get)
                .cause()
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(ICEBERG_BAD_DATA.toErrorCode()))
                .hasRootCauseMessage("delete file is truncated");
    }

    @Test
    void testFailureBeforeReadingIsReportedThroughTheFuture()
    {
        DeleteManager manager = new DeleteManager(TESTING_TYPE_MANAGER, BLOCKS_HASH_FACTORY, () -> {}, executor);

        // two deletion vectors for one data file is rejected before any delete file is read
        ListenableFuture<Optional<PageFilter>> future = manager.createDeletePageFilter(
                "data-file",
                OptionalLong.empty(),
                ImmutableList.of(deletionVectorFile("dv-1"), deletionVectorFile("dv-2")),
                ImmutableList.of(),
                SCHEMA,
                OptionalLong.empty(),
                OptionalLong.empty(),
                _ -> { throw new RuntimeException("unexpected DV read"); },
                (_, _, _) -> { throw new RuntimeException("unexpected delete open"); },
                (_, _, _) -> { throw new RuntimeException("unexpected delete open"); },
                directExecutor(),
                MemoryContext.NO_LIMIT);

        assertThatThrownBy(future::get)
                .cause()
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(ICEBERG_BAD_DATA.toErrorCode()))
                .hasMessageContaining("Multiple deletion vector files found for data file: data-file");
    }

    /**
     * An equality delete file whose field IDs are not in the table schema is rejected before any delete file is read,
     * so that no read is left open when the split fails.
     */
    @Test
    void testInvalidEqualityFieldIdsAreRejectedBeforeReading()
    {
        // opens run inline, so a read started before the failure would be recorded by the time the future fails
        DeleteManager manager = new DeleteManager(TESTING_TYPE_MANAGER, BLOCKS_HASH_FACTORY, () -> {}, directExecutor());
        AtomicInteger reads = new AtomicInteger();

        ListenableFuture<Optional<PageFilter>> future = manager.createDeletePageFilter(
                "data-file",
                OptionalLong.of(5L),
                ImmutableList.of(deletionVectorFile("dv"), positionDeleteFile("delete-1", 1L), equalityDeleteFile("eq-delete", 10L, ImmutableList.of(KEY_FIELD_ID + 1))),
                ImmutableList.of(ROW_POSITION_HANDLE),
                SCHEMA,
                OptionalLong.empty(),
                OptionalLong.empty(),
                _ -> {
                    reads.incrementAndGet();
                    throw new RuntimeException("unexpected DV read");
                },
                (_, _, _) -> {
                    reads.incrementAndGet();
                    throw new RuntimeException("unexpected delete open");
                },
                (_, _, _) -> {
                    reads.incrementAndGet();
                    throw new RuntimeException("unexpected delete open");
                },
                directExecutor(),
                MemoryContext.NO_LIMIT);

        assertThatThrownBy(future::get)
                .cause()
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(ICEBERG_BAD_DATA.toErrorCode()));
        assertThat(reads.get()).isZero();
    }

    /**
     * A delete read that fails must interrupt the other delete reads of the same split that are still running,
     * rather than leaving them to run to completion on the shared executor.
     */
    @ParameterizedTest(name = "{0} delete fails while {1} delete is still reading")
    @CsvSource({
            "POSITION,        POSITION",
            "POSITION,        EQUALITY",
            "EQUALITY,        POSITION",
            "EQUALITY,        EQUALITY",
            "EQUALITY,        DELETION_VECTOR",
            "DELETION_VECTOR, EQUALITY",
    })
    void testDeleteFailureCancelsTheOtherReads(DeleteKind failing, DeleteKind slow)
            throws InterruptedException
    {
        CountDownLatch slowReadStarted = new CountDownLatch(1);
        CountDownLatch slowReadInterrupted = new CountDownLatch(1);

        DeleteManager manager = new DeleteManager(TESTING_TYPE_MANAGER, BLOCKS_HASH_FACTORY, () -> {}, executor);

        ListenableFuture<Optional<PageFilter>> future = manager.createDeletePageFilter(
                "data-file",
                OptionalLong.of(5L),
                ImmutableList.of(deleteFile(slow, "slow-delete"), deleteFile(failing, "failing-delete")),
                ImmutableList.of(ROW_POSITION_HANDLE, getColumnHandle(SCHEMA.findField(KEY_FIELD_ID), TESTING_TYPE_MANAGER)),
                SCHEMA,
                OptionalLong.empty(),
                OptionalLong.empty(),
                deleteFile -> slowOrFailingRead(deleteFile, slowReadStarted, slowReadInterrupted),
                (deleteFile, _, _) -> slowOrFailingRead(deleteFile, slowReadStarted, slowReadInterrupted),
                (deleteFile, _, _) -> slowOrFailingRead(deleteFile, slowReadStarted, slowReadInterrupted),
                directExecutor(),
                MemoryContext.NO_LIMIT);

        assertThatThrownBy(() -> future.get(5, TimeUnit.SECONDS))
                .cause()
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(ICEBERG_CANNOT_OPEN_SPLIT.toErrorCode()))
                .hasMessageContaining("delete read failed");
        assertThat(slowReadInterrupted.await(5, TimeUnit.SECONDS)).isTrue();
    }

    enum DeleteKind
    {
        POSITION,
        DELETION_VECTOR,
        EQUALITY,
    }

    private static DeleteFile deleteFile(DeleteKind kind, String path)
    {
        return switch (kind) {
            case POSITION -> positionDeleteFile(path, 1L);
            case DELETION_VECTOR -> deletionVectorFile(path);
            case EQUALITY -> equalityDeleteFile(path, 10L, ImmutableList.of(KEY_FIELD_ID));
        };
    }

    /**
     * Blocks the read of the slow delete file until it is interrupted. Fails the read of the failing delete file
     * once the slow read has started, so that the slow read is still running when the failure happens.
     */
    private static <T> T slowOrFailingRead(DeleteFile deleteFile, CountDownLatch slowReadStarted, CountDownLatch slowReadInterrupted)
    {
        if (deleteFile.path().equals("slow-delete")) {
            slowReadStarted.countDown();
            try {
                new CountDownLatch(1).await();
            }
            catch (InterruptedException e) {
                slowReadInterrupted.countDown();
                Thread.currentThread().interrupt();
            }
            throw new RuntimeException("slow read interrupted");
        }
        verify(deleteFile.path().equals("failing-delete"), "unexpected read of %s", deleteFile.path());
        awaitUninterruptibly(slowReadStarted);
        throw new TrinoException(ICEBERG_CANNOT_OPEN_SPLIT, "delete read failed");
    }

    @Test
    void testEqualityDeleteFailureIsCached()
    {
        DeleteManager manager = new DeleteManager(TESTING_TYPE_MANAGER, BLOCKS_HASH_FACTORY, () -> {}, executor);

        ListenableFuture<Optional<PageFilter>> future1 = manager.createDeletePageFilter(
                "data-file",
                OptionalLong.of(5L),
                ImmutableList.of(equalityDeleteFile("eq-delete", 10L, ImmutableList.of(KEY_FIELD_ID))),
                ImmutableList.of(),
                SCHEMA,
                OptionalLong.empty(),
                OptionalLong.empty(),
                _ -> { throw new RuntimeException("unexpected DV read"); },
                (_, _, _) -> {
                    throw new UnsupportedOperationException();
                },
                (_, _, _) -> { throw new TrinoException(ICEBERG_CANNOT_OPEN_SPLIT, "eq delete open failed"); },
                directExecutor(),
                MemoryContext.NO_LIMIT);

        assertThatThrownBy(future1::get)
                .hasCauseInstanceOf(TrinoException.class)
                .hasMessageContaining("eq delete open failed");

        // A second split requesting the same equality delete file must get the cached failure without re-opening the file.
        AtomicBoolean openedAgain = new AtomicBoolean();
        ListenableFuture<Optional<PageFilter>> future2 = manager.createDeletePageFilter(
                "data-file-2",
                OptionalLong.of(5L),
                ImmutableList.of(equalityDeleteFile("eq-delete", 10L, ImmutableList.of(KEY_FIELD_ID))),
                ImmutableList.of(),
                SCHEMA,
                OptionalLong.empty(),
                OptionalLong.empty(),
                _ -> { throw new RuntimeException("unexpected DV read"); },
                (_, _, _) -> {
                    throw new UnsupportedOperationException();
                },
                (_, _, _) -> {
                    openedAgain.set(true);
                    return new FixedPageSource(ImmutableList.of());
                },
                directExecutor(),
                MemoryContext.NO_LIMIT);

        assertThatThrownBy(future2::get)
                .hasCauseInstanceOf(TrinoException.class)
                .hasMessageContaining("eq delete open failed");
        assertThat(openedAgain.get()).isFalse();
    }

    @Test
    void testPositionDeleteFutureCancellable()
            throws InterruptedException
    {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch interrupted = new CountDownLatch(1);
        CountDownLatch blocking = new CountDownLatch(1);

        DeleteManager manager = new DeleteManager(TESTING_TYPE_MANAGER, BLOCKS_HASH_FACTORY, () -> {}, executor);

        ListenableFuture<Optional<PageFilter>> future = manager.createDeletePageFilter(
                "data-file",
                OptionalLong.empty(),
                ImmutableList.of(positionDeleteFile("slow-delete", 1L)),
                ImmutableList.of(),
                SCHEMA,
                OptionalLong.empty(),
                OptionalLong.empty(),
                _ -> { throw new RuntimeException("unexpected DV read"); },
                // the open on the pool is what cancellation interrupts
                new BlockingOpen(started, blocking, interrupted),
                (_, _, _) -> {
                    throw new UnsupportedOperationException();
                },
                directExecutor(),
                MemoryContext.NO_LIMIT);

        assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
        future.cancel(true);
        assertThat(interrupted.await(5, TimeUnit.SECONDS)).isTrue();
    }

    /**
     * Equality delete loading is shared by the splits of a scan, so one split giving up must not cancel a load
     * that another split is still waiting on.
     */
    @Test
    void testEqualityDeleteNotCancelledWhileAnotherSplitNeedsIt()
            throws InterruptedException, ExecutionException, TimeoutException
    {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch interrupted = new CountDownLatch(1);
        DeleteManager manager = new DeleteManager(TESTING_TYPE_MANAGER, BLOCKS_HASH_FACTORY, () -> {}, executor);

        BlockingOpen blockingOnce = new BlockingOpen(started, release, interrupted, ImmutableSet.of("eq-delete"), ImmutableList.of(keyPage()));

        ListenableFuture<Optional<PageFilter>> cancelled = equalityDeleteFuture(manager, "data-file", blockingOnce);
        assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();

        // A second split waits on the same load. startLoad runs inline, so its reference is taken by now.
        ListenableFuture<Optional<PageFilter>> survivor = equalityDeleteFuture(manager, "data-file-2", blockingOnce);

        cancelled.cancel(true);
        release.countDown();

        // The load was not abandoned, so it completes and the surviving split gets its filter.
        assertThat(filterKeys(survivor.get(5, TimeUnit.SECONDS).orElseThrow(), 1, 2)).isEqualTo(1);
        assertThat(interrupted.getCount()).isEqualTo(1);
        assertThat(blockingOnce.opens()).isEqualTo(1);

        // A later split still reuses the completed load rather than re-reading the file.
        assertThat(filterKeys(equalityDeleteFuture(manager, "data-file-3", failingProvider()).get(5, TimeUnit.SECONDS).orElseThrow(), 1, 2)).isEqualTo(1);
        assertThat(blockingOnce.opens()).isEqualTo(1);
    }

    /**
     * Once the last split waiting on an equality delete load gives up, an open that has already started is interrupted,
     * as opening writes nothing to the shared filter. The load is dropped and a later split loads the file again.
     */
    @Test
    void testStartedEqualityDeleteOpenIsInterruptedWhenLastSplitCancels()
            throws Exception
    {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch interrupted = new CountDownLatch(1);
        DeleteManager manager = new DeleteManager(TESTING_TYPE_MANAGER, BLOCKS_HASH_FACTORY, () -> {}, executor);

        BlockingOpen blockingOpen = new BlockingOpen(started, release, interrupted, ImmutableSet.of("eq-delete"), ImmutableList.of(keyPage()));
        ListenableFuture<Optional<PageFilter>> future = equalityDeleteFuture(manager, "data-file", blockingOpen);

        assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();

        // the only split waiting on the load gives up, which interrupts the running open
        future.cancel(true);
        assertThat(interrupted.await(5, TimeUnit.SECONDS)).isTrue();

        // nothing was decoded, so the load was abandoned and a later split loads the file again
        AtomicInteger laterOpens = new AtomicInteger();
        Optional<PageFilter> laterFilter = equalityDeleteFuture(manager, "data-file-2", (_, _, _) -> {
            laterOpens.incrementAndGet();
            return new FixedPageSource(ImmutableList.of(keyPage()));
        }).get(5, TimeUnit.SECONDS);
        assertThat(filterKeys(laterFilter.orElseThrow(), 1, 2)).isEqualTo(1);
        assertThat(blockingOpen.opens()).isEqualTo(1);
        assertThat(laterOpens.get()).isEqualTo(1);
    }

    /**
     * When the load cannot be scheduled the task never runs, so it must not be left in the cache. Otherwise a
     * later split reusing the same equality delete file waits on it forever instead of failing.
     */
    @Test
    void testEqualityDeleteRejectedExecutionDoesNotStrandLaterSplits()
    {
        ExecutorService shutdownExecutor = Executors.newFixedThreadPool(1);
        shutdownExecutor.shutdown();
        DeleteManager manager = new DeleteManager(TESTING_TYPE_MANAGER, BLOCKS_HASH_FACTORY, () -> {}, shutdownExecutor);

        assertThatThrownBy(() -> equalityDeleteFuture(manager, "data-file").get(5, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .cause()
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(ICEBERG_BAD_DATA.toErrorCode()));

        assertThatThrownBy(() -> equalityDeleteFuture(manager, "data-file-2").get(5, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .cause()
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(ICEBERG_BAD_DATA.toErrorCode()));
    }

    private static ListenableFuture<Optional<PageFilter>> equalityDeleteFuture(DeleteManager manager, String dataFilePath)
    {
        return manager.createDeletePageFilter(
                dataFilePath,
                OptionalLong.of(5L),
                ImmutableList.of(equalityDeleteFile("eq-delete", 10L, ImmutableList.of(KEY_FIELD_ID))),
                ImmutableList.of(),
                SCHEMA,
                OptionalLong.empty(),
                OptionalLong.empty(),
                _ -> { throw new RuntimeException("unexpected DV read"); },
                (_, _, _) -> {
                    throw new UnsupportedOperationException();
                },
                (_, _, _) -> new FixedPageSource(ImmutableList.of()),
                directExecutor(),
                MemoryContext.NO_LIMIT);
    }

    /**
     * Exceeding the memory limit while reporting its size must fail the split with EXCEEDED_LOCAL_MEMORY_LIMIT
     */
    @Test
    void testMemoryUsageReporterFailurePropagates()
    {
        DeleteManager manager = new DeleteManager(
                TESTING_TYPE_MANAGER,
                BLOCKS_HASH_FACTORY,
                () -> { throw new TrinoException(EXCEEDED_LOCAL_MEMORY_LIMIT, "delete filter cache exceeded the limit"); },
                executor);

        DeletePageSourceProvider succeedingProvider = (_, _, _) -> new FixedPageSource(ImmutableList.of(keyPage()));
        assertThatThrownBy(() -> equalityDeleteFuture(manager, "data-file", succeedingProvider).get(5, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .cause()
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(EXCEEDED_LOCAL_MEMORY_LIMIT.toErrorCode()))
                .hasMessageContaining("delete filter cache exceeded the limit");
    }

    private static ListenableFuture<Optional<PageFilter>> equalityDeleteFuture(
            DeleteManager manager,
            String dataFilePath,
            DeletePageSourceProvider equalityDeletePageSourceProvider)
    {
        return manager.createDeletePageFilter(
                dataFilePath,
                OptionalLong.of(5L),
                ImmutableList.of(equalityDeleteFile("eq-delete", 10L, ImmutableList.of(KEY_FIELD_ID))),
                ImmutableList.of(getColumnHandle(SCHEMA.findField(KEY_FIELD_ID), TESTING_TYPE_MANAGER)),
                SCHEMA,
                OptionalLong.empty(),
                OptionalLong.empty(),
                _ -> { throw new RuntimeException("unexpected DV read"); },
                (_, _, _) -> {
                    throw new UnsupportedOperationException();
                },
                equalityDeletePageSourceProvider,
                directExecutor(),
                MemoryContext.NO_LIMIT);
    }

    /**
     * Opens the files it is given, blocking each open until released or interrupted, then returns {@code pages}.
     * Fails on any other file with {@code otherFileFailure} if present.
     */
    private static class BlockingOpen
            implements DeletePageSourceProvider
    {
        private final CountDownLatch started;
        private final CountDownLatch release;
        private final CountDownLatch interrupted;
        private final Set<String> blockingPaths;
        private final List<Page> pages;
        private final AtomicInteger opens = new AtomicInteger();

        BlockingOpen(CountDownLatch started, CountDownLatch release, CountDownLatch interrupted)
        {
            this(started, release, interrupted, ImmutableSet.of("slow-delete"), ImmutableList.of());
        }

        BlockingOpen(CountDownLatch started, CountDownLatch release, CountDownLatch interrupted, Set<String> blockingPaths, List<Page> pages)
        {
            this.started = started;
            this.release = release;
            this.interrupted = interrupted;
            this.blockingPaths = ImmutableSet.copyOf(blockingPaths);
            this.pages = ImmutableList.copyOf(pages);
        }

        @Override
        public ConnectorPageSource openDeletes(DeleteFile delete, List<IcebergColumnHandle> deleteColumns, TupleDomain<IcebergColumnHandle> tupleDomain)
        {
            if (!blockingPaths.contains(delete.path())) {
                return openOther(delete);
            }
            opens.incrementAndGet();
            started.countDown();
            try {
                release.await();
            }
            catch (InterruptedException e) {
                interrupted.countDown();
                Thread.currentThread().interrupt();
            }
            return new FixedPageSource(pages);
        }

        protected ConnectorPageSource openOther(DeleteFile delete)
        {
            throw new UnsupportedOperationException("unexpected open of " + delete.path());
        }

        public int opens()
        {
            return opens.get();
        }
    }

    /**
     * Records the thread that decodes the page source, and whether it was closed.
     */
    private static final class RecordingPageSource
            extends FixedPageSource
    {
        private final AtomicReference<Thread> decodeThread;
        private final AtomicBoolean closed = new AtomicBoolean();

        RecordingPageSource(List<Page> pages, AtomicReference<Thread> decodeThread)
        {
            super(pages);
            this.decodeThread = decodeThread;
        }

        @Override
        public SourcePage getNextSourcePage()
        {
            decodeThread.set(Thread.currentThread());
            return super.getNextSourcePage();
        }

        @Override
        public void close()
        {
            closed.set(true);
            super.close();
        }

        public boolean isClosed()
        {
            return closed.get();
        }
    }

    private static DeletePageSourceProvider failingProvider()
    {
        return (_, _, _) -> {
            throw new TrinoException(ICEBERG_CANNOT_OPEN_SPLIT, "delete file must not be opened again");
        };
    }

    private static Page keyPage()
    {
        return new Page(new LongArrayBlock(1, Optional.empty(), new long[] {1L}));
    }

    private static ConnectorPageSource positionDeletePageSource(long... deletedPositions)
    {
        return new FixedPageSource(positionDeletePages(deletedPositions));
    }

    private static List<Page> positionDeletePages(long... deletedPositions)
    {
        BlockBuilder pathBuilder = VARCHAR.createBlockBuilder(null, deletedPositions.length);
        BlockBuilder positionBuilder = BIGINT.createBlockBuilder(null, deletedPositions.length);
        for (long deletedPosition : deletedPositions) {
            VARCHAR.writeSlice(pathBuilder, utf8Slice(DATA_FILE_PATH));
            BIGINT.writeLong(positionBuilder, deletedPosition);
        }
        return ImmutableList.of(new Page(pathBuilder.build(), positionBuilder.build()));
    }

    private static int filterRowPositions(PageFilter pageFilter, long... rowPositions)
    {
        BlockBuilder positionBuilder = BIGINT.createBlockBuilder(null, rowPositions.length);
        for (long rowPosition : rowPositions) {
            BIGINT.writeLong(positionBuilder, rowPosition);
        }
        SourcePage page = SourcePage.create(new Page(positionBuilder.build()));
        pageFilter.applyFilter(page);
        return page.getPositionCount();
    }

    private static int filterKeys(PageFilter pageFilter, long... keys)
    {
        SourcePage page = SourcePage.create(new Page(new LongArrayBlock(keys.length, Optional.empty(), keys)));
        pageFilter.applyFilter(page);
        return page.getPositionCount();
    }

    private static DeleteFile positionDeleteFile(String path, long sequenceNumber)
    {
        return positionDeleteFile(path, sequenceNumber, 0L);
    }

    private static DeleteFile positionDeleteFile(String path, long sequenceNumber, long recordCount)
    {
        return new DeleteFile(
                FileContent.POSITION_DELETES,
                path,
                FileFormat.PARQUET,
                recordCount,
                0L,
                ImmutableList.of(),
                OptionalLong.empty(),
                OptionalLong.empty(),
                sequenceNumber,
                OptionalLong.empty(),
                Optional.empty(),
                Optional.empty());
    }

    private static DeleteFile deletionVectorFile(String path)
    {
        return new DeleteFile(
                FileContent.POSITION_DELETES,
                path,
                FileFormat.PUFFIN,
                0L,
                0L,
                ImmutableList.of(),
                OptionalLong.empty(),
                OptionalLong.empty(),
                1L,
                OptionalLong.of(0),
                Optional.of(1),
                Optional.empty());
    }

    private static DeleteFile equalityDeleteFile(String path, long sequenceNumber, List<Integer> equalityFieldIds)
    {
        return new DeleteFile(
                FileContent.EQUALITY_DELETES,
                path,
                FileFormat.PARQUET,
                0L,
                0L,
                equalityFieldIds,
                OptionalLong.empty(),
                OptionalLong.empty(),
                sequenceNumber,
                OptionalLong.empty(),
                Optional.empty(),
                Optional.empty());
    }

    /**
     * Runs the decodes of a split on the calling thread, as the page source does, until the load completes.
     */
    private static <T> T runUntilDone(ThreadlessExecutor split, ListenableFuture<T> future)
            throws Exception
    {
        while (!future.isDone()) {
            whenAnyComplete(ImmutableList.of(split.taskAvailable(), future)).get(5, TimeUnit.SECONDS);
            split.runPending();
        }
        return future.get();
    }
}
