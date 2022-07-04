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
import io.trino.spi.Page;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPageSink;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestMemoryPagesStore
{
    private static final int POSITIONS_PER_PAGE = 0;

    private MemoryPagesStore pagesStore;
    private MemoryVersionedPagesStore versionedPagesStore;
    private MemoryPageSinkProvider pageSinkProvider;

    @BeforeMethod
    public void setUp()
    {
        pagesStore = new MemoryPagesStore();
        versionedPagesStore = new MemoryVersionedPagesStore();
        pageSinkProvider = new MemoryPageSinkProvider(pagesStore, versionedPagesStore);
    }

    @Test
    public void testCreateEmptyTable()
    {
        createTable(0L, 0L);
        assertEquals(pagesStore.getPages(0L, ImmutableList.of(0)), ImmutableList.of());
    }

    @Test
    public void testInsertPage()
    {
        createTable(0L, 0L);
        insertToTable(0L, 0L);
        assertEquals(pagesStore.getPages(0L, ImmutableList.of(0)).size(), 1);
    }

    @Test
    public void testInsertPageWithoutCreate()
    {
        insertToTable(0L, 0L);
        assertEquals(pagesStore.getPages(0L, ImmutableList.of(0)).size(), 1);
    }

    @Test
    public void testCleanUp()
    {
        createTable(0L, 0L);
        createTable(1L, 0L, 1L);
        createTable(2L, 0L, 1L, 2L);

        assertTrue(pagesStore.contains(0L));
        assertTrue(pagesStore.contains(1L));
        assertTrue(pagesStore.contains(2L));

        insertToTable(1L, 0L, 1L);

        assertTrue(pagesStore.contains(0L));
        assertTrue(pagesStore.contains(1L));
        assertTrue(pagesStore.contains(2L));

        insertToTable(2L, 0L, 2L);

        assertTrue(pagesStore.contains(0L));
        assertFalse(pagesStore.contains(1L));
        assertTrue(pagesStore.contains(2L));
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
                createMemoryInsertTableHandle(tableId, activeTableIds));
        pageSink.appendPage(page);
        pageSink.finish();
    }

    private void createTable(long tableId, Long... activeTableIds)
    {
        ConnectorPageSink pageSink = pageSinkProvider.createPageSink(
                MemoryTransactionHandle.INSTANCE,
                SESSION,
                createMemoryOutputTableHandle(tableId, activeTableIds));
        pageSink.finish();
    }

    private static ConnectorOutputTableHandle createMemoryOutputTableHandle(long tableId, Long... activeTableIds)
    {
        return new MemoryOutputTableHandle(tableId, ImmutableSet.copyOf(activeTableIds), Optional.empty(), Optional.empty());
    }

    private static ConnectorInsertTableHandle createMemoryInsertTableHandle(long tableId, Long[] activeTableIds)
    {
        return new MemoryInsertTableHandle(tableId, ImmutableSet.copyOf(activeTableIds), Optional.empty(), Optional.empty(), Optional.empty());
    }

    private static Page createPage()
    {
        BlockBuilder blockBuilder = BIGINT.createFixedSizeBlockBuilder(POSITIONS_PER_PAGE);
        BIGINT.writeLong(blockBuilder, 42L);
        return new Page(0, blockBuilder.build());
    }
}
