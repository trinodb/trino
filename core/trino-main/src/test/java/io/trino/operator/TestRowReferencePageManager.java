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
package io.trino.operator;

import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static io.trino.spi.type.BigintType.BIGINT;
import static java.lang.Math.toIntExact;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestRowReferencePageManager
{
    @Test
    public void testEmptyPage()
    {
        RowReferencePageManager pageManager = new RowReferencePageManager();
        Page page = createBigIntSingleBlockPage(0, 0);
        try (RowReferencePageManager.LoadCursor cursor = pageManager.add(page)) {
            assertThat(cursor.advance()).isFalse();
            assertThatThrownBy(cursor::allocateRowId)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("Not yet advanced");
        }
        assertThat(pageManager.getPageBytes()).isEqualTo(0);
    }

    @Test
    public void testSinglePageRowIds()
    {
        RowReferencePageManager pageManager = new RowReferencePageManager();

        RowIdComparisonStrategy strategy = (rowId1, rowId2) -> {
            long value1 = extractValue(pageManager, rowId1);
            long value2 = extractValue(pageManager, rowId2);
            return Long.compare(value1, value2);
        };

        long id0;
        long id1;
        long id3;
        Page page = createBigIntSingleBlockPage(0, 4);
        try (RowReferencePageManager.LoadCursor cursor = pageManager.add(page)) {
            assertThat(cursor.advance()).isTrue();
            id0 = cursor.allocateRowId();
            assertThat(extractValue(pageManager, id0)).isEqualTo(0L);

            assertThat(cursor.advance()).isTrue();
            assertThat(cursor.compareTo(strategy, id0)).isPositive();
            id1 = cursor.allocateRowId();
            assertThat(extractValue(pageManager, id1)).isEqualTo(1L);

            assertThat(cursor.advance()).isTrue();
            assertThat(cursor.compareTo(strategy, id0)).isPositive();
            assertThat(cursor.compareTo(strategy, id1)).isPositive();
            // Skip this row by not allocating an ID

            assertThat(cursor.advance()).isTrue();
            assertThat(cursor.compareTo(strategy, id0)).isPositive();
            assertThat(cursor.compareTo(strategy, id1)).isPositive();
            id3 = cursor.allocateRowId();
            assertThat(extractValue(pageManager, id3)).isEqualTo(3L);

            // Page size accounting happens after closing the cursor
            assertThat(pageManager.getPageBytes()).isEqualTo(0);
        }
        assertThat(pageManager.getPageBytes()).isPositive();

        // Should still be able to extract values for allocated IDs outside of cursor scope
        assertThat(extractValue(pageManager, id0)).isEqualTo(0L);
        assertThat(extractValue(pageManager, id1)).isEqualTo(1L);
        assertThat(extractValue(pageManager, id3)).isEqualTo(3L);
    }

    @Test
    public void testMultiplePageRowIds()
    {
        RowReferencePageManager pageManager = new RowReferencePageManager();

        RowIdComparisonStrategy strategy = (rowId1, rowId2) -> {
            long value1 = extractValue(pageManager, rowId1);
            long value2 = extractValue(pageManager, rowId2);
            return Long.compare(value1, value2);
        };

        long id0;
        Page page = createBigIntSingleBlockPage(0, 1);
        try (RowReferencePageManager.LoadCursor cursor = pageManager.add(page)) {
            assertThat(cursor.advance()).isTrue();
            id0 = cursor.allocateRowId();
            assertThat(extractValue(pageManager, id0)).isEqualTo(0L);

            assertThat(cursor.advance()).isFalse();

            // Page size accounting happens after closing the cursor
            assertThat(pageManager.getPageBytes()).isEqualTo(0);
        }
        long pageBytes1 = pageManager.getPageBytes();
        assertThat(pageBytes1).isPositive();

        // Should still be able to extract values for allocated IDs outside of cursor scope
        assertThat(extractValue(pageManager, id0)).isEqualTo(0L);

        long id1;
        page = createBigIntSingleBlockPage(1, 2);
        try (RowReferencePageManager.LoadCursor cursor = pageManager.add(page)) {
            assertThat(cursor.advance()).isTrue();
            assertThat(cursor.compareTo(strategy, id0)).isPositive();
            id1 = cursor.allocateRowId();
            assertThat(extractValue(pageManager, id1)).isEqualTo(1L);
            assertThat(cursor.advance()).isFalse();

            // Page size accounting happens after closing the cursor
            assertThat(pageManager.getPageBytes()).isEqualTo(pageBytes1);
        }
        // Another page added, so should be larger
        assertThat(pageManager.getPageBytes()).isGreaterThan(pageBytes1);

        // Should still be able to extract values for allocated IDs outside of cursor scopes
        assertThat(extractValue(pageManager, id0)).isEqualTo(0L);
        assertThat(extractValue(pageManager, id1)).isEqualTo(1L);
    }

    @Test
    public void testSkipCompaction()
    {
        RowReferencePageManager pageManager = new RowReferencePageManager();

        long id0;
        Page page = createBigIntSingleBlockPage(0, 100);
        try (RowReferencePageManager.LoadCursor cursor = pageManager.add(page)) {
            assertThat(cursor.advance()).isTrue();
            id0 = cursor.allocateRowId();
            assertThat(extractValue(pageManager, id0)).isEqualTo(0L);

            // No compaction candidates until after the cursor is closed
            assertThat(pageManager.getCompactionCandidateCount()).isEqualTo(0);

            // Ignore the remaining positions, which means they should remain unreferenced
        }

        // Should still be able to extract values for allocated IDs outside of cursor scope
        assertThat(extractValue(pageManager, id0)).isEqualTo(0L);

        // Page should have some size before compaction
        long pageBytesBeforeCompaction = pageManager.getPageBytes();
        assertThat(pageBytesBeforeCompaction).isPositive();

        // With a 1% fill, this page will certainly require compaction
        assertThat(pageManager.getCompactionCandidateCount()).isEqualTo(1);
        pageManager.compactIfNeeded();
        assertThat(pageManager.getCompactionCandidateCount()).isEqualTo(0);

        // Page size should shrink after compaction
        assertThat(pageManager.getPageBytes()).isLessThan(pageBytesBeforeCompaction);

        // Should still be able to extract same value after compaction
        assertThat(extractValue(pageManager, id0)).isEqualTo(0L);
    }

    @Test
    public void testDereferenceCompaction()
    {
        RowReferencePageManager pageManager = new RowReferencePageManager();

        long id0;
        List<Long> rowIdsToDereference = new ArrayList<>();
        Page page = createBigIntSingleBlockPage(0, 100);
        try (RowReferencePageManager.LoadCursor cursor = pageManager.add(page)) {
            assertThat(cursor.advance()).isTrue();
            id0 = cursor.allocateRowId();
            assertThat(extractValue(pageManager, id0)).isEqualTo(0L);

            // Collect the remaining rowIds
            while (cursor.advance()) {
                rowIdsToDereference.add(cursor.allocateRowId());
            }
        }

        // No compaction candidates since all rows should be referenced
        assertThat(pageManager.getCompactionCandidateCount()).isEqualTo(0);

        // Dereference 99% of row IDs
        for (long rowId : rowIdsToDereference) {
            pageManager.dereference(rowId);
        }

        // Page should have some size before compaction
        long pageBytesBeforeCompaction = pageManager.getPageBytes();
        assertThat(pageBytesBeforeCompaction).isPositive();

        // With a 1% fill, this page will certainly require compaction
        assertThat(pageManager.getCompactionCandidateCount()).isEqualTo(1);
        pageManager.compactIfNeeded();
        assertThat(pageManager.getCompactionCandidateCount()).isEqualTo(0);

        // Page size should shrink after compaction
        assertThat(pageManager.getPageBytes()).isLessThan(pageBytesBeforeCompaction);

        // Should still be able to extract same value after compaction
        assertThat(extractValue(pageManager, id0)).isEqualTo(0L);
    }

    @Test
    public void testSkipFullPage()
    {
        RowReferencePageManager pageManager = new RowReferencePageManager();

        Page page = createBigIntSingleBlockPage(0, 100);
        try (RowReferencePageManager.LoadCursor cursor = pageManager.add(page)) {
            // Close the cursor without any row ID allocations
        }

        // No compaction candidates since page is no longer needed
        assertThat(pageManager.getCompactionCandidateCount()).isEqualTo(0);

        // Should not have any page bytes since page was skipped
        assertThat(pageManager.getPageBytes()).isEqualTo(0);
    }

    @Test
    public void testDereferenceFullPage()
    {
        RowReferencePageManager pageManager = new RowReferencePageManager();

        List<Long> rowIdsToDereference = new ArrayList<>();
        Page page = createBigIntSingleBlockPage(0, 100);
        try (RowReferencePageManager.LoadCursor cursor = pageManager.add(page)) {
            while (cursor.advance()) {
                rowIdsToDereference.add(cursor.allocateRowId());
            }
        }

        // Dereference all row IDs
        for (long rowId : rowIdsToDereference) {
            pageManager.dereference(rowId);
        }

        // No compaction candidates since page is no longer needed
        assertThat(pageManager.getCompactionCandidateCount()).isEqualTo(0);

        // Should not have any page bytes since page was fully dereferenced
        assertThat(pageManager.getPageBytes()).isEqualTo(0);
    }

    @Test
    public void testInlineDereferenceFullPage()
    {
        RowReferencePageManager pageManager = new RowReferencePageManager();

        Page page = createBigIntSingleBlockPage(0, 100);
        try (RowReferencePageManager.LoadCursor cursor = pageManager.add(page)) {
            while (cursor.advance()) {
                pageManager.dereference(cursor.allocateRowId());
            }
        }

        // No compaction candidates since page is no longer needed
        assertThat(pageManager.getCompactionCandidateCount()).isEqualTo(0);

        // Should not have any page bytes since page was fully dereferenced
        assertThat(pageManager.getPageBytes()).isEqualTo(0);
    }

    @Test
    public void testRowIdRecycling()
    {
        RowReferencePageManager pageManager = new RowReferencePageManager();

        Page page = createBigIntSingleBlockPage(0, 3);
        try (RowReferencePageManager.LoadCursor cursor = pageManager.add(page)) {
            assertThat(cursor.advance()).isTrue();
            long id0 = cursor.allocateRowId();
            assertThat(extractValue(pageManager, id0)).isEqualTo(0L);

            assertThat(cursor.advance()).isTrue();
            long id1 = cursor.allocateRowId();
            assertThat(extractValue(pageManager, id1)).isEqualTo(1L);

            pageManager.dereference(id0);

            // Since id0 was dereferenced, the system can recycle that id for reuse
            assertThat(cursor.advance()).isTrue();
            long id2 = cursor.allocateRowId();
            assertThat(extractValue(pageManager, id2)).isEqualTo(2L);
            assertThat(id0).isEqualTo(id2);
        }
    }

    private static long extractValue(RowReferencePageManager pageManager, long rowId)
    {
        Page page = pageManager.getPage(rowId);
        int position = pageManager.getPosition(rowId);
        return BIGINT.getLong(page.getBlock(0), position);
    }

    private static Page createBigIntSingleBlockPage(long startInclusive, long endExclusive)
    {
        BlockBuilder blockBuilder = BIGINT.createBlockBuilder(null, toIntExact(endExclusive - startInclusive));
        for (long i = startInclusive; i < endExclusive; i++) {
            BIGINT.writeLong(blockBuilder, i);
        }
        Block block = blockBuilder.build();
        return new Page(block);
    }
}
