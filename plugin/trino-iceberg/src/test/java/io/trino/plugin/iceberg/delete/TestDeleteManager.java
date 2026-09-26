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
import com.google.common.util.concurrent.ListenableFuture;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.plugin.iceberg.IcebergColumnHandle;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.FixedPageSource;
import io.trino.spi.connector.MemoryContext;
import io.trino.spi.connector.SourcePage;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.util.concurrent.Uninterruptibles.awaitUninterruptibly;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_BAD_DATA;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_CANNOT_OPEN_SPLIT;
import static io.trino.plugin.iceberg.IcebergTestUtils.BLOCKS_HASH_FACTORY;
import static io.trino.plugin.iceberg.IcebergUtil.getColumnHandle;
import static io.trino.spi.StandardErrorCode.EXCEEDED_LOCAL_MEMORY_LIMIT;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
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
                _ -> deletionVector,
                (_, _, _) -> {
                    throw new UnsupportedOperationException();
                },
                (_, _, _) -> {
                    throw new UnsupportedOperationException();
                },
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
                memoryContext::setBytes));

        assertThat(memoryContext.getBytes()).isEqualTo(DeletionVector.builder().add(0).add(3).build().orElseThrow().retainedSizeInBytes());
        assertThat(filterRowPositions(pageFilter.orElseThrow(), 0, 1, 2, 3, 4)).isEqualTo(3);
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
                MemoryContext.NO_LIMIT);

        assertThatThrownBy(future::get)
                .cause()
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(ICEBERG_BAD_DATA.toErrorCode()))
                .hasMessageContaining("Multiple deletion vector files found for data file: data-file");
    }

    /**
     * A position delete read that fails must interrupt the reads of the other delete files of the same split,
     * rather than leaving them to run to completion on the shared executor.
     */
    @Test
    void testPositionDeleteFailureCancelsTheOtherReads()
            throws InterruptedException
    {
        CountDownLatch slowReadStarted = new CountDownLatch(1);
        CountDownLatch slowReadInterrupted = new CountDownLatch(1);
        CountDownLatch blocking = new CountDownLatch(1);

        DeleteManager manager = new DeleteManager(TESTING_TYPE_MANAGER, BLOCKS_HASH_FACTORY, () -> {}, executor);

        ListenableFuture<Optional<PageFilter>> future = manager.createDeletePageFilter(
                "data-file",
                OptionalLong.empty(),
                ImmutableList.of(positionDeleteFile("slow-delete", 1L), positionDeleteFile("failing-delete", 1L)),
                ImmutableList.of(),
                SCHEMA,
                OptionalLong.empty(),
                OptionalLong.empty(),
                _ -> { throw new RuntimeException("unexpected DV read"); },
                (deleteFile, _, _) -> {
                    if (deleteFile.path().equals("failing-delete")) {
                        // let the other read get going first, so it is still in flight when this one fails
                        awaitUninterruptibly(slowReadStarted);
                        throw new TrinoException(ICEBERG_CANNOT_OPEN_SPLIT, "delete open failed");
                    }
                    slowReadStarted.countDown();
                    try {
                        blocking.await();
                    }
                    catch (InterruptedException e) {
                        slowReadInterrupted.countDown();
                        Thread.currentThread().interrupt();
                    }
                    return new FixedPageSource(ImmutableList.of());
                },
                (_, _, _) -> {
                    throw new UnsupportedOperationException();
                },
                MemoryContext.NO_LIMIT);

        assertThatThrownBy(future::get)
                .hasCauseInstanceOf(TrinoException.class)
                .hasMessageContaining("delete open failed");
        assertThat(slowReadInterrupted.await(5, TimeUnit.SECONDS)).isTrue();
    }

    /**
     * A failing equality delete read must cancel the position delete reads.
     */
    @Test
    void testEqualityDeleteFailureCancelsTheOtherReads()
            throws InterruptedException
    {
        CountDownLatch slowReadStarted = new CountDownLatch(1);
        CountDownLatch slowReadInterrupted = new CountDownLatch(1);
        CountDownLatch blocking = new CountDownLatch(1);

        DeleteManager manager = new DeleteManager(TESTING_TYPE_MANAGER, BLOCKS_HASH_FACTORY, () -> {}, executor);

        ListenableFuture<Optional<PageFilter>> future = manager.createDeletePageFilter(
                "data-file",
                OptionalLong.of(5L),
                ImmutableList.of(positionDeleteFile("slow-delete", 1L), equalityDeleteFile("eq-delete", 10L, ImmutableList.of(KEY_FIELD_ID))),
                ImmutableList.of(getColumnHandle(SCHEMA.findField(KEY_FIELD_ID), TESTING_TYPE_MANAGER)),
                SCHEMA,
                OptionalLong.empty(),
                OptionalLong.empty(),
                _ -> { throw new RuntimeException("unexpected DV read"); },
                (_, _, _) -> {
                    slowReadStarted.countDown();
                    try {
                        blocking.await();
                    }
                    catch (InterruptedException e) {
                        slowReadInterrupted.countDown();
                        Thread.currentThread().interrupt();
                    }
                    return new FixedPageSource(ImmutableList.of());
                },
                (_, _, _) -> {
                    // let the position delete read start first, so it is still in flight when this one fails
                    awaitUninterruptibly(slowReadStarted);
                    throw new TrinoException(ICEBERG_CANNOT_OPEN_SPLIT, "equality delete open failed");
                },
                MemoryContext.NO_LIMIT);

        assertThatThrownBy(() -> future.get(5, TimeUnit.SECONDS))
                .hasCauseInstanceOf(TrinoException.class)
                .hasMessageContaining("equality delete open failed");
        assertThat(slowReadInterrupted.await(5, TimeUnit.SECONDS)).isTrue();
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
                ImmutableList.of(positionDeleteFile("pos-delete", 1L)),
                ImmutableList.of(),
                SCHEMA,
                OptionalLong.empty(),
                OptionalLong.empty(),
                _ -> { throw new RuntimeException("unexpected DV read"); },
                (_, _, _) -> {
                    started.countDown();
                    try {
                        blocking.await();
                    }
                    catch (InterruptedException e) {
                        interrupted.countDown();
                        Thread.currentThread().interrupt();
                    }
                    return new FixedPageSource(ImmutableList.of());
                },
                (_, _, _) -> {
                    throw new UnsupportedOperationException();
                },
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
        AtomicInteger opened = new AtomicInteger();

        DeleteManager manager = new DeleteManager(TESTING_TYPE_MANAGER, BLOCKS_HASH_FACTORY, () -> {}, executor);

        DeletePageSourceProvider blockingOnce = (_, _, _) -> {
            opened.incrementAndGet();
            started.countDown();
            try {
                release.await();
            }
            catch (InterruptedException e) {
                interrupted.countDown();
                Thread.currentThread().interrupt();
            }
            return new FixedPageSource(ImmutableList.of(keyPage()));
        };

        ListenableFuture<Optional<PageFilter>> cancelled = equalityDeleteFuture(manager, "data-file", blockingOnce);
        assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();

        // A second split waits on the same load. startLoad runs inline, so its reference is taken by now.
        ListenableFuture<Optional<PageFilter>> survivor = equalityDeleteFuture(manager, "data-file-2", blockingOnce);

        cancelled.cancel(true);
        release.countDown();

        // The load was not abandoned, so it completes and the surviving split gets its filter.
        assertThat(survivor.get(5, TimeUnit.SECONDS)).isPresent();
        assertThat(interrupted.getCount()).isEqualTo(1);
        assertThat(opened.get()).isEqualTo(1);

        // A later split still reuses the completed load rather than re-reading the file.
        assertThat(equalityDeleteFuture(manager, "data-file-3", failingProvider()).get(5, TimeUnit.SECONDS)).isPresent();
        assertThat(opened.get()).isEqualTo(1);
    }

    /**
     * Once the last split waiting on an equality delete load gives up, a load that has already started is still
     * left to run to completion, so it cannot leave a partially read delete file behind in the shared filter.
     * The completed load stays cached for later splits.
     */
    @Test
    void testStartedEqualityDeleteLoadCompletesWhenLastSplitCancels()
            throws Exception
    {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch interrupted = new CountDownLatch(1);
        AtomicInteger opened = new AtomicInteger();

        DeleteManager manager = new DeleteManager(TESTING_TYPE_MANAGER, BLOCKS_HASH_FACTORY, () -> {}, executor);

        ListenableFuture<Optional<PageFilter>> future = equalityDeleteFuture(manager, "data-file", (_, _, _) -> {
            opened.incrementAndGet();
            started.countDown();
            try {
                release.await();
            }
            catch (InterruptedException e) {
                interrupted.countDown();
                Thread.currentThread().interrupt();
            }
            return new FixedPageSource(ImmutableList.of(keyPage()));
        });

        assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();

        // the only split waiting on the load gives up, but the load has started so it is not interrupted
        future.cancel(true);
        release.countDown();

        // a later split reuses the completed load rather than re-reading the delete file
        assertThat(equalityDeleteFuture(manager, "data-file-2", failingProvider()).get(5, TimeUnit.SECONDS)).isPresent();
        assertThat(interrupted.getCount()).isEqualTo(1);
        assertThat(opened.get()).isEqualTo(1);
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
                MemoryContext.NO_LIMIT);
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
        BlockBuilder pathBuilder = VARCHAR.createBlockBuilder(null, deletedPositions.length);
        BlockBuilder positionBuilder = BIGINT.createBlockBuilder(null, deletedPositions.length);
        for (long deletedPosition : deletedPositions) {
            VARCHAR.writeSlice(pathBuilder, utf8Slice(DATA_FILE_PATH));
            BIGINT.writeLong(positionBuilder, deletedPosition);
        }
        return new FixedPageSource(ImmutableList.of(new Page(pathBuilder.build(), positionBuilder.build())));
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

    private static DeleteFile positionDeleteFile(String path, long sequenceNumber)
    {
        return new DeleteFile(
                FileContent.POSITION_DELETES,
                path,
                FileFormat.PARQUET,
                0L,
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
}
