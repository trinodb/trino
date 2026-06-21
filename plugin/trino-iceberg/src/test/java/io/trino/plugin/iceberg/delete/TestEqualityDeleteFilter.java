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
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.trino.operator.FlatHashStrategyCompiler;
import io.trino.operator.NullSafeHashCompiler;
import io.trino.plugin.iceberg.ColumnIdentity;
import io.trino.plugin.iceberg.IcebergColumnHandle;
import io.trino.spi.BlocksHashFactory;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.connector.FixedPageSource;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.iceberg.ColumnIdentity.TypeCategory.PRIMITIVE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.assertj.core.api.Assertions.assertThat;

class TestEqualityDeleteFilter
{
    private static final int KEY_FIELD_ID = 1;
    private static final int VALUE_FIELD_ID = 2;

    private static final ColumnIdentity KEY_IDENTITY = new ColumnIdentity(KEY_FIELD_ID, "key", PRIMITIVE, ImmutableList.of());
    private static final ColumnIdentity VALUE_IDENTITY = new ColumnIdentity(VALUE_FIELD_ID, "value", PRIMITIVE, ImmutableList.of());

    private static final IcebergColumnHandle VARCHAR_KEY_HANDLE = IcebergColumnHandle.optional(KEY_IDENTITY).columnType(VARCHAR).build();
    private static final IcebergColumnHandle BIGINT_KEY_HANDLE = IcebergColumnHandle.optional(KEY_IDENTITY).columnType(BIGINT).build();
    private static final IcebergColumnHandle VALUE_HANDLE = IcebergColumnHandle.optional(VALUE_IDENTITY).columnType(BIGINT).build();

    private static final Schema VARCHAR_KEY_SCHEMA = new Schema(optional(KEY_FIELD_ID, "key", Types.StringType.get()));
    private static final Schema BIGINT_KEY_SCHEMA = new Schema(optional(KEY_FIELD_ID, "key", Types.LongType.get()));
    private static final Schema TWO_COLUMN_SCHEMA = new Schema(
            optional(KEY_FIELD_ID, "key", Types.StringType.get()),
            optional(VALUE_FIELD_ID, "value", Types.LongType.get()));

    private static final BlocksHashFactory BLOCKS_HASH_FACTORY =
            new FlatHashStrategyCompiler(new TypeOperators(), new NullSafeHashCompiler(new TypeOperators())).createBlocksHashFactory();

    private static final AtomicInteger DELETE_FILE_COUNTER = new AtomicInteger();

    // deleteFileSequenceNumber > splitDataSequenceNumber ensures delete applies to data file
    private static final long SPLIT_DATA_SEQUENCE_NUMBER = 3L;
    private static final long DELETE_FILE_SEQUENCE_NUMBER = 5L;

    @Test
    void testVarcharKeyDeletesMatchingRows()
    {
        EqualityDeleteFilterBuilder builder = newBuilder(VARCHAR_KEY_SCHEMA, ImmutableList.of(VARCHAR));
        loadDeleteFile(builder, ImmutableList.of(VARCHAR_KEY_HANDLE), DELETE_FILE_SEQUENCE_NUMBER, varcharPage("key-1"));

        PageFilter predicate = builder.build().createPageFilter(
                ImmutableList.of(VARCHAR_KEY_HANDLE, VALUE_HANDLE), SPLIT_DATA_SEQUENCE_NUMBER);

        SourcePage dataPage = SourcePage.create(new Page(
                varcharBlock("key-1", "key-2", "key-3"),
                bigintBlock(10L, 20L, 30L)));
        predicate.applyFilter(dataPage);

        assertThat(dataPage.getPositionCount()).isEqualTo(2);
        assertThat(BIGINT.getLong(dataPage.getBlock(1), 0)).isEqualTo(20L);
        assertThat(BIGINT.getLong(dataPage.getBlock(1), 1)).isEqualTo(30L);
    }

    @Test
    void testBigintKeyDeletesMatchingRows()
    {
        EqualityDeleteFilterBuilder builder = newBuilder(BIGINT_KEY_SCHEMA, ImmutableList.of(BIGINT));
        loadDeleteFile(builder, ImmutableList.of(BIGINT_KEY_HANDLE), DELETE_FILE_SEQUENCE_NUMBER, bigintPage(42L));

        PageFilter predicate = builder.build().createPageFilter(
                ImmutableList.of(BIGINT_KEY_HANDLE, VALUE_HANDLE), SPLIT_DATA_SEQUENCE_NUMBER);

        SourcePage dataPage = SourcePage.create(new Page(
                bigintBlock(42L, 43L, 44L),
                bigintBlock(1L, 2L, 3L)));
        predicate.applyFilter(dataPage);

        assertThat(dataPage.getPositionCount()).isEqualTo(2);
        assertThat(BIGINT.getLong(dataPage.getBlock(1), 0)).isEqualTo(2L);
        assertThat(BIGINT.getLong(dataPage.getBlock(1), 1)).isEqualTo(3L);
    }

    @Test
    void testMultiColumnKeyRequiresBothColumnsToMatch()
    {
        EqualityDeleteFilterBuilder builder = newBuilder(TWO_COLUMN_SCHEMA, ImmutableList.of(VARCHAR, BIGINT));

        Page deleteRows = new Page(varcharBlock("key-1"), bigintBlock(10L));
        loadDeleteFile(
                builder,
                ImmutableList.of(VARCHAR_KEY_HANDLE, VALUE_HANDLE),
                DELETE_FILE_SEQUENCE_NUMBER,
                deleteRows);

        PageFilter predicate = builder.build().createPageFilter(
                ImmutableList.of(VARCHAR_KEY_HANDLE, VALUE_HANDLE), SPLIT_DATA_SEQUENCE_NUMBER);

        SourcePage dataPage = SourcePage.create(new Page(
                varcharBlock("key-1", "key-1", "key-2"),
                bigintBlock(10L, 99L, 10L)));
        predicate.applyFilter(dataPage);

        assertThat(dataPage.getPositionCount())
                .describedAs("only exact (key, value) pair match should be deleted")
                .isEqualTo(2);
        assertThat(VARCHAR.getSlice(dataPage.getBlock(0), 0).toStringUtf8()).isEqualTo("key-1");
        assertThat(BIGINT.getLong(dataPage.getBlock(1), 0)).isEqualTo(99L);
        assertThat(VARCHAR.getSlice(dataPage.getBlock(0), 1).toStringUtf8()).isEqualTo("key-2");
        assertThat(BIGINT.getLong(dataPage.getBlock(1), 1)).isEqualTo(10L);
    }

    @Test
    void testNonMatchingRowsAreNotDeleted()
    {
        EqualityDeleteFilterBuilder builder = newBuilder(VARCHAR_KEY_SCHEMA, ImmutableList.of(VARCHAR));
        loadDeleteFile(builder, ImmutableList.of(VARCHAR_KEY_HANDLE), DELETE_FILE_SEQUENCE_NUMBER, varcharPage("key-deleted"));

        PageFilter predicate = builder.build().createPageFilter(
                ImmutableList.of(VARCHAR_KEY_HANDLE), SPLIT_DATA_SEQUENCE_NUMBER);

        SourcePage dataPage = SourcePage.create(new Page(varcharBlock("key-a", "key-b", "key-c")));
        predicate.applyFilter(dataPage);

        assertThat(dataPage.getPositionCount()).isEqualTo(3);
    }

    @Test
    void testNullKeyDeletesNullRow()
    {
        EqualityDeleteFilterBuilder builder = newBuilder(VARCHAR_KEY_SCHEMA, ImmutableList.of(VARCHAR));
        loadDeleteFile(builder, ImmutableList.of(VARCHAR_KEY_HANDLE), DELETE_FILE_SEQUENCE_NUMBER, varcharPageWithNulls((String) null));

        PageFilter predicate = builder.build().createPageFilter(
                ImmutableList.of(VARCHAR_KEY_HANDLE), SPLIT_DATA_SEQUENCE_NUMBER);

        SourcePage dataPage = SourcePage.create(new Page(nullableVarcharBlock(null, "some-key")));
        predicate.applyFilter(dataPage);

        assertThat(dataPage.getPositionCount()).isEqualTo(1);
        assertThat(dataPage.getBlock(0).isNull(0)).isFalse();
    }

    @Test
    void testDeleteAppliesWhenDeleteSequenceNumberIsGreaterThanDataSequenceNumber()
    {
        EqualityDeleteFilterBuilder builder = newBuilder(VARCHAR_KEY_SCHEMA, ImmutableList.of(VARCHAR));
        loadDeleteFile(builder, ImmutableList.of(VARCHAR_KEY_HANDLE), 10L, varcharPage("target"));

        PageFilter predicate = builder.build().createPageFilter(
                ImmutableList.of(VARCHAR_KEY_HANDLE), 5L);

        SourcePage dataPage = SourcePage.create(new Page(varcharBlock("target", "other")));
        predicate.applyFilter(dataPage);

        assertThat(dataPage.getPositionCount())
                .describedAs("delete sequenceNumber=10 should delete data row with sequenceNumber=5")
                .isEqualTo(1);
    }

    @Test
    void testDeleteDoesNotApplyWhenDataSequenceNumberIsGreaterThanDeleteSequenceNumber()
    {
        EqualityDeleteFilterBuilder builder = newBuilder(VARCHAR_KEY_SCHEMA, ImmutableList.of(VARCHAR));
        loadDeleteFile(builder, ImmutableList.of(VARCHAR_KEY_HANDLE), 5L, varcharPage("target"));

        PageFilter predicate = builder.build().createPageFilter(
                ImmutableList.of(VARCHAR_KEY_HANDLE), 10L);

        SourcePage dataPage = SourcePage.create(new Page(varcharBlock("target", "other")));
        predicate.applyFilter(dataPage);

        assertThat(dataPage.getPositionCount())
                .describedAs("delete sequenceNumber=5 should not delete data row with sequenceNumber=10 (row was re-inserted after the delete)")
                .isEqualTo(2);
    }

    @Test
    void testDeleteDoesNotApplyWhenDeleteSequenceNumberEqualsDataSequenceNumber()
    {
        EqualityDeleteFilterBuilder builder = newBuilder(VARCHAR_KEY_SCHEMA, ImmutableList.of(VARCHAR));
        loadDeleteFile(builder, ImmutableList.of(VARCHAR_KEY_HANDLE), 5L, varcharPage("target"));

        PageFilter predicate = builder.build().createPageFilter(
                ImmutableList.of(VARCHAR_KEY_HANDLE), 5L);

        SourcePage dataPage = SourcePage.create(new Page(varcharBlock("target", "other")));
        predicate.applyFilter(dataPage);

        assertThat(dataPage.getPositionCount())
                .describedAs("delete sequenceNumber=5 should not delete data row with sequenceNumber=5 (data and delete were written in the same snapshot)")
                .isEqualTo(2);
    }

    @Test
    void testMaxDeleteSequenceNumberWinsAcrossFiles()
    {
        EqualityDeleteFilterBuilder builder = newBuilder(VARCHAR_KEY_SCHEMA, ImmutableList.of(VARCHAR));
        loadDeleteFile(builder, ImmutableList.of(VARCHAR_KEY_HANDLE), 3L, varcharPage("target"));
        loadDeleteFile(builder, ImmutableList.of(VARCHAR_KEY_HANDLE), 7L, varcharPage("target"));

        PageFilter predicate = builder.build().createPageFilter(
                ImmutableList.of(VARCHAR_KEY_HANDLE), 5L);

        SourcePage dataPage = SourcePage.create(new Page(varcharBlock("target", "other")));
        predicate.applyFilter(dataPage);

        assertThat(dataPage.getPositionCount())
                .describedAs("max delete sequenceNumber=7 should still delete data row with sequenceNumber=5")
                .isEqualTo(1);
    }

    @Test
    void testCombinedPredicateApplyFilterChainsFilters()
    {
        EqualityDeleteFilterBuilder builder = newBuilder(VARCHAR_KEY_SCHEMA, ImmutableList.of(VARCHAR));
        loadDeleteFile(builder, ImmutableList.of(VARCHAR_KEY_HANDLE), DELETE_FILE_SEQUENCE_NUMBER, varcharPage("key-1"));
        PageFilter combined = PageFilter.allOf(ImmutableList.of(
                builder.build().createPageFilter(ImmutableList.of(VARCHAR_KEY_HANDLE, VALUE_HANDLE), SPLIT_DATA_SEQUENCE_NUMBER),
                PageFilter.of((page, position) -> BIGINT.getLong(page.getBlock(1), position) != 30L))).orElseThrow();

        SourcePage page = SourcePage.create(new Page(
                varcharBlock("key-1", "key-2", "key-3"),
                bigintBlock(10L, 20L, 30L)));
        combined.applyFilter(page);

        assertThat(page.getPositionCount()).isEqualTo(1);
        assertThat(BIGINT.getLong(page.getBlock(1), 0)).isEqualTo(20L);
    }

    @Test
    void testTriplePredicateChainApplyFilter()
    {
        EqualityDeleteFilterBuilder builder = newBuilder(VARCHAR_KEY_SCHEMA, ImmutableList.of(VARCHAR));
        loadDeleteFile(builder, ImmutableList.of(VARCHAR_KEY_HANDLE), DELETE_FILE_SEQUENCE_NUMBER, varcharPage("key-1"));
        PageFilter combined = PageFilter.allOf(ImmutableList.of(
                builder.build().createPageFilter(ImmutableList.of(VARCHAR_KEY_HANDLE, VALUE_HANDLE), SPLIT_DATA_SEQUENCE_NUMBER),
                PageFilter.of((page, position) -> BIGINT.getLong(page.getBlock(1), position) != 20L),
                PageFilter.of((page, position) -> BIGINT.getLong(page.getBlock(1), position) != 30L))).orElseThrow();

        SourcePage page = SourcePage.create(new Page(
                varcharBlock("key-1", "key-2", "key-3", "key-4"),
                bigintBlock(10L, 20L, 30L, 40L)));
        combined.applyFilter(page);

        assertThat(page.getPositionCount()).isEqualTo(1);
        assertThat(BIGINT.getLong(page.getBlock(1), 0)).isEqualTo(40L);
    }

    /**
     * Ensures composing PageFilters is valid even with dictionary backed pages.
     * If selectPositions is called multiple times and the page contains a dictionary block,
     * the second call to selectPositions refers to absolute positions but the block has already been filtered.
     */
    @Test
    void testComposedFiltersWithDictionaryBackedPage()
    {
        Block keySource = varcharBlock("key-0", "key-1", "key-2", "key-3", "key-4", "key-5");
        Block rowPosSource = bigintBlock(0L, 1L, 2L, 3L, 4L, 5L);

        int[] ids = {1, 2, 3, 4};
        Block dictionaryKeys = DictionaryBlock.create(ids.length, keySource.getUnderlyingValueBlock(), ids);
        Block dictionaryRowPos = DictionaryBlock.create(ids.length, rowPosSource.getUnderlyingValueBlock(), ids);
        SourcePage page = SourcePage.create(new Page(dictionaryKeys, dictionaryRowPos));

        PageFilter positionDelete = PageFilter.of((p, position) -> BIGINT.getLong(p.getBlock(1), position) != 3L);

        EqualityDeleteFilterBuilder builder = newBuilder(VARCHAR_KEY_SCHEMA, ImmutableList.of(VARCHAR));
        loadDeleteFile(builder, ImmutableList.of(VARCHAR_KEY_HANDLE), DELETE_FILE_SEQUENCE_NUMBER, varcharPage("key-2"));
        PageFilter equalityDelete = builder.build().createPageFilter(ImmutableList.of(VARCHAR_KEY_HANDLE, VALUE_HANDLE), SPLIT_DATA_SEQUENCE_NUMBER);

        PageFilter.allOf(ImmutableList.of(positionDelete, equalityDelete))
                .orElseThrow()
                .applyFilter(page);

        assertThat(page.getPositionCount()).isEqualTo(2);
        assertThat(VARCHAR.getSlice(page.getBlock(0), 0).toStringUtf8()).isEqualTo("key-1");
        assertThat(VARCHAR.getSlice(page.getBlock(0), 1).toStringUtf8()).isEqualTo("key-4");
    }

    /**
     * 10 equality delete files, 1M unique equality delete string keys, 10M equality delete rows
     * Simulates an UPSERT heavy workload that utilizes equality deletes
     */
    @Test
    void testLargeScaleUpsertWorkload()
    {
        int uniqueKeys = 1_000_000;
        int deleteFileCount = 10;

        String[] keys = IntStream.range(0, uniqueKeys)
                .mapToObj(i -> String.format("key-%07d", i))
                .toArray(String[]::new);

        EqualityDeleteFilterBuilder builder = newBuilder(VARCHAR_KEY_SCHEMA, ImmutableList.of(VARCHAR));

        Page deleteFilePage = varcharPage(keys);
        for (int fileIndex = 1; fileIndex <= deleteFileCount; fileIndex++) {
            loadDeleteFile(builder, ImmutableList.of(VARCHAR_KEY_HANDLE), fileIndex, deleteFilePage);
        }
        PageFilter predicate = builder.build().createPageFilter(
                ImmutableList.of(VARCHAR_KEY_HANDLE), 0L);

        SourcePage deletedPage = SourcePage.create(new Page(varcharBlock(keys)));
        predicate.applyFilter(deletedPage);
        assertThat(deletedPage.getPositionCount()).isEqualTo(0);

        String[] keptKeys = IntStream.range(0, 100)
                .mapToObj(i -> "kept-" + i)
                .toArray(String[]::new);
        SourcePage keptPage = SourcePage.create(new Page(varcharBlock(keptKeys)));
        predicate.applyFilter(keptPage);
        assertThat(keptPage.getPositionCount()).isEqualTo(keptKeys.length);
    }

    @Test
    void testConcurrentLoadsAndReads()
            throws Exception
    {
        int uniqueKeys = 1_000;
        int loaderThreadCount = 8;
        int readerThreadCount = 4;

        // Each loader covers a sliding window of keys so adjacent loaders share half their keys
        int stride = uniqueKeys / loaderThreadCount;
        int windowSize = stride * 2;

        String[] allDeletedKeys = IntStream.range(0, uniqueKeys)
                .mapToObj(i -> "del-" + i)
                .toArray(String[]::new);
        String[] keptKeys = IntStream.range(0, 100)
                .mapToObj(i -> "kept-" + i)
                .toArray(String[]::new);

        String[] allKeys = Stream.concat(Arrays.stream(allDeletedKeys), Arrays.stream(keptKeys))
                .toArray(String[]::new);

        EqualityDeleteFilterBuilder builder = newBuilder(VARCHAR_KEY_SCHEMA, ImmutableList.of(VARCHAR));
        PageFilter predicate = builder.build().createPageFilter(
                ImmutableList.of(VARCHAR_KEY_HANDLE), 0L);

        try (ListeningExecutorService executor = MoreExecutors.listeningDecorator(
                Executors.newFixedThreadPool(loaderThreadCount + readerThreadCount))) {
            List<ListenableFuture<Void>> futures = new ArrayList<>();
            CountDownLatch startLatch = new CountDownLatch(1);

            for (int i = 0; i < loaderThreadCount; i++) {
                int start = i * stride;
                int end = Math.min(start + windowSize, uniqueKeys);
                String[] loaderKeys = Arrays.stream(allDeletedKeys, start, end).toArray(String[]::new);
                long sequenceNumber = i + 1L;
                futures.add(executor.submit(() -> {
                    startLatch.await();
                    loadDeleteFile(builder, ImmutableList.of(VARCHAR_KEY_HANDLE), sequenceNumber, varcharPage(loaderKeys));
                    return null;
                }));
            }
            for (int i = 0; i < readerThreadCount; i++) {
                futures.add(executor.submit(() -> {
                    startLatch.await();
                    for (int j = 0; j < 100; j++) {
                        SourcePage page = SourcePage.create(new Page(varcharBlock(allKeys)));
                        predicate.applyFilter(page);
                    }
                    return null;
                }));
            }
            startLatch.countDown();
            Futures.allAsList(futures).get(10, TimeUnit.SECONDS);
        }

        SourcePage deletedPage = SourcePage.create(new Page(varcharBlock(allDeletedKeys)));
        predicate.applyFilter(deletedPage);
        assertThat(deletedPage.getPositionCount()).isEqualTo(0);

        SourcePage keptPage = SourcePage.create(new Page(varcharBlock(keptKeys)));
        predicate.applyFilter(keptPage);
        assertThat(keptPage.getPositionCount()).isEqualTo(keptKeys.length);
    }

    @Test
    void testEstimatedSizeGrowsWithInsertedKeys()
    {
        EqualityDeleteFilterBuilder builder = newBuilder(VARCHAR_KEY_SCHEMA, ImmutableList.of(VARCHAR));

        long emptySize = builder.getEstimatedSizeInBytes();

        String[] keys = IntStream.range(0, 500)
                .mapToObj(i -> "key-" + i)
                .toArray(String[]::new);
        loadDeleteFile(builder, ImmutableList.of(VARCHAR_KEY_HANDLE), 1L, varcharPage(keys));

        long sizeAfter = builder.getEstimatedSizeInBytes();
        assertThat(sizeAfter)
                .describedAs("estimated size should grow after inserting 500 keys")
                .isGreaterThan(emptySize);
    }

    @Test
    void testEstimatedSizeDoesNotGrowForDuplicateKeys()
    {
        EqualityDeleteFilterBuilder builder = newBuilder(VARCHAR_KEY_SCHEMA, ImmutableList.of(VARCHAR));

        DeleteFile fileOne = equalityDeleteFile("fake-delete-path-1", 1L, ImmutableList.of(KEY_FIELD_ID));
        DeleteFile fileTwo = equalityDeleteFile("fake-delete-path-2", 1L, ImmutableList.of(KEY_FIELD_ID));
        Page page = varcharPage("key-a", "key-b");

        builder.readEqualityDeletes(
                fileOne,
                ImmutableList.of(VARCHAR_KEY_HANDLE),
                (_, _, _) -> new FixedPageSource(ImmutableList.of(page)));
        long sizeAfterFirst = builder.getEstimatedSizeInBytes();

        builder.readEqualityDeletes(
                fileTwo,
                ImmutableList.of(VARCHAR_KEY_HANDLE),
                (_, _, _) -> new FixedPageSource(ImmutableList.of(page)));
        long sizeAfterSecond = builder.getEstimatedSizeInBytes();

        assertThat(sizeAfterSecond)
                .describedAs("loading the same deletes twice should not increase size")
                .isEqualTo(sizeAfterFirst);
    }

    @Test
    void testMultipleEqualityDeleteSchemasEachFilterCorrectly()
    {
        int dataPageRowCount = 1_000_000;
        int deleteFilterCount = 10;
        int rowsPerDelete = 10_000;

        IcebergColumnHandle[] columnHandles = new IcebergColumnHandle[deleteFilterCount];
        Schema[] schemas = new Schema[deleteFilterCount];
        for (int i = 0; i < deleteFilterCount; i++) {
            int fieldId = i + 1;
            ColumnIdentity identity = new ColumnIdentity(fieldId, "col" + fieldId, PRIMITIVE, ImmutableList.of());
            columnHandles[i] = IcebergColumnHandle.optional(identity).columnType(BIGINT).build();
            schemas[i] = new Schema(optional(fieldId, "col" + fieldId, Types.LongType.get()));
        }
        List<IcebergColumnHandle> readColumns = ImmutableList.copyOf(columnHandles);

        Block[] blocks = new Block[deleteFilterCount];
        for (int col = 0; col < deleteFilterCount; col++) {
            BlockBuilder blockBuilder = BIGINT.createFixedSizeBlockBuilder(dataPageRowCount);
            for (int row = 0; row < dataPageRowCount; row++) {
                BIGINT.writeLong(blockBuilder, row);
            }
            blocks[col] = blockBuilder.build();
        }
        SourcePage page = SourcePage.create(new Page(blocks));

        // filter i deletes rows [i*rowsPerDelete, (i+1)*rowsPerDelete)
        PageFilter composedPredicate = PageFilter.allOf(IntStream.range(0, deleteFilterCount).mapToObj(i -> {
            long[] deleteValues = IntStream.range(0, rowsPerDelete).mapToLong(row -> (long) i * rowsPerDelete + row).toArray();
            EqualityDeleteFilterBuilder filterBuilder = newBuilder(schemas[i], ImmutableList.of(BIGINT));
            loadDeleteFile(filterBuilder, ImmutableList.of(columnHandles[i]), DELETE_FILE_SEQUENCE_NUMBER, new Page(bigintBlock(deleteValues)));
            return filterBuilder.build().createPageFilter(readColumns, SPLIT_DATA_SEQUENCE_NUMBER);
        }).collect(toImmutableList())).orElseThrow();

        composedPredicate.applyFilter(page);

        int totalDeleted = deleteFilterCount * rowsPerDelete;
        assertThat(page.getPositionCount()).isEqualTo(dataPageRowCount - totalDeleted);
        assertThat(BIGINT.getLong(page.getBlock(0), 0)).isEqualTo(totalDeleted);
    }

    private static EqualityDeleteFilterBuilder newBuilder(Schema schema, List<Type> columnTypes)
    {
        return FlatEqualityDeleteFilter.builder(schema, columnTypes, BLOCKS_HASH_FACTORY);
    }

    private static void loadDeleteFile(
            EqualityDeleteFilterBuilder builder,
            List<IcebergColumnHandle> deleteColumns,
            long sequenceNumber,
            Page... pages)
    {
        String path = "delete-file-" + DELETE_FILE_COUNTER.incrementAndGet();
        DeleteFile file = equalityDeleteFile(
                path,
                sequenceNumber,
                deleteColumns.stream().map(IcebergColumnHandle::getId).collect(toImmutableList()));
        builder.readEqualityDeletes(
                file,
                deleteColumns,
                (_, _, _) -> new FixedPageSource(ImmutableList.copyOf(pages)));
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
                Optional.empty());
    }

    private static Page varcharPage(String... values)
    {
        return new Page(varcharBlock(values));
    }

    private static Page varcharPageWithNulls(String... values)
    {
        return new Page(nullableVarcharBlock(values));
    }

    private static Page bigintPage(long... values)
    {
        return new Page(bigintBlock(values));
    }

    private static Block varcharBlock(String... values)
    {
        BlockBuilder builder = VARCHAR.createBlockBuilder(null, values.length);
        for (String value : values) {
            VARCHAR.writeString(builder, value);
        }
        return builder.build();
    }

    private static Block nullableVarcharBlock(String... values)
    {
        BlockBuilder builder = VARCHAR.createBlockBuilder(null, values.length);
        for (String value : values) {
            if (value == null) {
                builder.appendNull();
            }
            else {
                VARCHAR.writeString(builder, value);
            }
        }
        return builder.build();
    }

    private static Block bigintBlock(long... values)
    {
        BlockBuilder builder = BIGINT.createFixedSizeBlockBuilder(values.length);
        for (long value : values) {
            BIGINT.writeLong(builder, value);
        }
        return builder.build();
    }
}
