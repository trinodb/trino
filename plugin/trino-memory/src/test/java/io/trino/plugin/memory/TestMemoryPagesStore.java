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
package io.trino.plugin.memory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.trino.plugin.memory.MemoryInsertTableHandle.InsertMode;
import io.trino.spi.HostAddress;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.ByteArrayBlock;
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.testing.TestingNodeManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.List;
import java.util.OptionalDouble;
import java.util.OptionalLong;

import static io.trino.block.BlockAssertions.createLongSequenceBlock;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.testing.TestingPageSinkId.TESTING_PAGE_SINK_ID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_METHOD)
@Execution(SAME_THREAD)
public class TestMemoryPagesStore
{
    private static final int POSITIONS_PER_PAGE = 0;

    private MemoryPagesStore pagesStore;
    private MemoryPageSinkProvider pageSinkProvider;

    @BeforeEach
    public void setUp()
    {
        pagesStore = new MemoryPagesStore(new MemoryConfig().setMaxDataPerNode(DataSize.of(1, DataSize.Unit.MEGABYTE)), TestingNodeManager.create());
        pageSinkProvider = new MemoryPageSinkProvider(pagesStore, HostAddress.fromString("localhost:8080"));
    }

    @Test
    public void testCreateEmptyTable()
    {
        createTable(0L, 0L);
        assertThat(pagesStore.getPages(0L, 0, 1, new int[] {0}, List.of(INTEGER), 0, OptionalLong.empty(), OptionalDouble.empty())).isEqualTo(ImmutableList.of());
    }

    @Test
    public void testInsertPage()
    {
        createTable(0L, 0L);
        insertToTable(0L, 0L);
        assertThat(pagesStore.getPages(0L, 0, 1, new int[] {0}, List.of(INTEGER), POSITIONS_PER_PAGE, OptionalLong.empty(), OptionalDouble.empty())).hasSize(1);
    }

    @Test
    public void testInsertPageWithoutCreate()
    {
        insertToTable(0L, 0L);
        assertThat(pagesStore.getPages(0L, 0, 1, new int[] {0}, List.of(INTEGER), POSITIONS_PER_PAGE, OptionalLong.empty(), OptionalDouble.empty())).hasSize(1);
    }

    @Test
    public void testReadFromUnknownTable()
    {
        assertThatThrownBy(() -> {
            pagesStore.getPages(0L, 0, 1, new int[] {0}, List.of(INTEGER), 0, OptionalLong.empty(), OptionalDouble.empty());
        })
                .isInstanceOf(TrinoException.class);
    }

    @Test
    public void testTryToReadFromEmptyTable()
    {
        createTable(0L, 0L);
        assertThat(pagesStore.getPages(0L, 0, 1, new int[] {0}, List.of(INTEGER), 0, OptionalLong.empty(), OptionalDouble.empty())).isEqualTo(ImmutableList.of());
        assertThatThrownBy(() -> pagesStore.getPages(0L, 0, 1, new int[] {0}, List.of(INTEGER), 42, OptionalLong.empty(), OptionalDouble.empty()))
                .isInstanceOf(TrinoException.class)
                .hasMessageMatching("Expected to find.*");
    }

    @Test
    public void testReadBackExtraColumns()
    {
        createTable(0L, 0L);
        int positions = 1024;
        // insert single BIGINT column page
        insertToTable(0L, new Page(createLongSequenceBlock(0, positions)), 0L);
        // Read back as BIGINT, INTEGER, BOOLEAN expecting the INTEGER and BOOLEAN columns to be RLE encoded
        // channel 1 is reserved by rowId column
        List<Page> readBack = pagesStore.getPages(0L, 0, 1, new int[] {0, 2, 3}, List.of(BIGINT, INTEGER, BOOLEAN), positions, OptionalLong.empty(), OptionalDouble.empty());
        assertThat(readBack).hasSize(1);
        Page readPage = readBack.getFirst();
        assertThat(readPage.getChannelCount()).isEqualTo(3);
        assertThat(readPage.getPositionCount()).isEqualTo(positions);
        assertThat(readPage.getBlock(1))
                .isInstanceOf(RunLengthEncodedBlock.class);
        assertThat(readPage.getBlock(1).getUnderlyingValueBlock())
                .isInstanceOf(IntArrayBlock.class);
        assertThat(readPage.getBlock(2))
                .isInstanceOf(RunLengthEncodedBlock.class);
        assertThat(readPage.getBlock(2).getUnderlyingValueBlock())
                .isInstanceOf(ByteArrayBlock.class);
    }

    @Test
    public void testCleanUp()
    {
        createTable(0L, 0L);
        createTable(1L, 0L, 1L);
        createTable(2L, 0L, 1L, 2L);

        assertThat(pagesStore.contains(0L)).isTrue();
        assertThat(pagesStore.contains(1L)).isTrue();
        assertThat(pagesStore.contains(2L)).isTrue();

        insertToTable(1L, 0L, 1L);

        assertThat(pagesStore.contains(0L)).isTrue();
        assertThat(pagesStore.contains(1L)).isTrue();
        assertThat(pagesStore.contains(2L)).isTrue();

        insertToTable(2L, 0L, 2L);

        assertThat(pagesStore.contains(0L)).isTrue();
        assertThat(pagesStore.contains(1L)).isFalse();
        assertThat(pagesStore.contains(2L)).isTrue();
    }

    @Test
    public void testMemoryLimitExceeded()
    {
        createTable(0L, 0L);
        insertToTable(0L, createOneMegaBytePage(), 0L);
        assertThatThrownBy(() -> insertToTable(0L, createOneMegaBytePage(), 0L))
                .isInstanceOf(TrinoException.class)
                .hasMessageMatching("Memory limit.*");
    }

    @Test
    public void testTruncate()
    {
        createTable(0L, 0L);
        insertToTable(0L, createOneMegaBytePage(), 0L);
        truncateTable(0L, 0L);
        insertToTable(0L, createOneMegaBytePage(), 0L);
    }

    private void insertToTable(long tableId, Long... activeTableIds)
    {
        insertToTable(tableId, createPage(), activeTableIds);
    }

    private void insertToTable(long tableId, Page page, Long... activeTableIds)
    {
        ConnectorPageSink pageSink = pageSinkProvider.createPageSink(
                MemoryTransactionHandle.INSTANCE,
                SESSION,
                createMemoryInsertTableHandle(tableId, activeTableIds),
                TESTING_PAGE_SINK_ID);
        pageSink.appendPage(page);
        pageSink.finish();
    }

    private void createTable(long tableId, Long... activeTableIds)
    {
        ConnectorPageSink pageSink = pageSinkProvider.createPageSink(
                MemoryTransactionHandle.INSTANCE,
                SESSION,
                createMemoryOutputTableHandle(tableId, activeTableIds),
                TESTING_PAGE_SINK_ID);
        pageSink.finish();
    }

    private void truncateTable(long tableId, Long... activeTableIds)
    {
        ConnectorPageSink pageSink = pageSinkProvider.createPageSink(
                MemoryTransactionHandle.INSTANCE,
                SESSION,
                createOverwriteMemoryInsertTableHandle(tableId, activeTableIds),
                TESTING_PAGE_SINK_ID);
        pageSink.finish();
    }

    private static ConnectorOutputTableHandle createMemoryOutputTableHandle(long tableId, Long... activeTableIds)
    {
        return new MemoryOutputTableHandle(tableId, ImmutableSet.copyOf(activeTableIds), 1);
    }

    private static ConnectorInsertTableHandle createMemoryInsertTableHandle(long tableId, Long[] activeTableIds)
    {
        return new MemoryInsertTableHandle(tableId, InsertMode.APPEND, ImmutableSet.copyOf(activeTableIds), 1);
    }

    private static ConnectorInsertTableHandle createOverwriteMemoryInsertTableHandle(long tableId, Long[] activeTableIds)
    {
        return new MemoryInsertTableHandle(tableId, InsertMode.OVERWRITE, ImmutableSet.copyOf(activeTableIds), 1);
    }

    private static Page createPage()
    {
        BlockBuilder blockBuilder = BIGINT.createFixedSizeBlockBuilder(POSITIONS_PER_PAGE);
        BIGINT.writeLong(blockBuilder, 42L);
        return new Page(0, blockBuilder.build());
    }

    private static Page createOneMegaBytePage()
    {
        BlockBuilder blockBuilder = BIGINT.createFixedSizeBlockBuilder(POSITIONS_PER_PAGE);
        while (blockBuilder.getRetainedSizeInBytes() < 1024 * 1024) {
            BIGINT.writeLong(blockBuilder, 42L);
        }
        return new Page(0, blockBuilder.build());
    }
}
