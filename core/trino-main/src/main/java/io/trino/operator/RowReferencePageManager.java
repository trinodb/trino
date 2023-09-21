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

import com.google.common.annotations.VisibleForTesting;
import io.airlift.slice.SizeOf;
import io.trino.array.IntBigArray;
import io.trino.spi.Page;
import io.trino.util.LongBigArrayFIFOQueue;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import jakarta.annotation.Nullable;

import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.airlift.slice.SizeOf.instanceSize;

/**
 * Page buffering manager that enables access to individual rows via stable row IDs. This allows computation to be
 * built against these row IDs, while still enabling bulk memory optimizations such as compaction and lazy loading
 * behind the scenes. Callers are responsible for explicitly de-referencing any rows that are no longer needed.
 */
public final class RowReferencePageManager
{
    private static final long INSTANCE_SIZE = instanceSize(RowReferencePageManager.class);
    private static final long PAGE_ACCOUNTING_INSTANCE_SIZE = instanceSize(PageAccounting.class);
    private static final int RESERVED_ROW_ID_FOR_CURSOR = -1;

    private final IdRegistry<PageAccounting> pages = new IdRegistry<>();
    private final RowIdBuffer rowIdBuffer = new RowIdBuffer();
    private final IntHashSet compactionCandidates = new IntHashSet();

    @Nullable
    private LoadCursor currentCursor;
    private long pageBytes;

    public LoadCursor add(Page page)
    {
        return add(page, 0);
    }

    public LoadCursor add(Page page, int startingPosition)
    {
        checkState(currentCursor == null, "Cursor still active");
        checkArgument(startingPosition >= 0 && startingPosition <= page.getPositionCount(), "invalid startingPosition: %s", startingPosition);

        PageAccounting pageAccounting = pages.allocateId(id -> new PageAccounting(id, page));

        pageAccounting.lockPage();
        currentCursor = new LoadCursor(pageAccounting, startingPosition, () -> {
            // Initiate additional actions on close
            checkState(currentCursor != null);
            pageAccounting.unlockPage();
            pageAccounting.loadPageLoadIfNeeded();
            // Account for page size after lazy loading (which can change the page size)
            pageBytes += pageAccounting.sizeOf();
            currentCursor = null;

            checkPageMaintenance(pageAccounting);
        });

        return currentCursor;
    }

    public void dereference(long rowId)
    {
        PageAccounting pageAccounting = pages.get(rowIdBuffer.getPageId(rowId));
        pageAccounting.dereference(rowId);
        checkPageMaintenance(pageAccounting);
    }

    private void checkPageMaintenance(PageAccounting pageAccounting)
    {
        int pageId = pageAccounting.getPageId();
        if (pageAccounting.isPruneEligible()) {
            compactionCandidates.remove(pageId);
            pages.deallocate(pageId);
            pageBytes -= pageAccounting.sizeOf();
        }
        else if (pageAccounting.isCompactionEligible()) {
            compactionCandidates.add(pageId);
        }
    }

    public Page getPage(long rowId)
    {
        if (isCursorRowId(rowId)) {
            checkState(currentCursor != null, "No active cursor");
            return currentCursor.getPage();
        }
        int pageId = rowIdBuffer.getPageId(rowId);
        return pages.get(pageId).getPage();
    }

    public int getPosition(long rowId)
    {
        if (isCursorRowId(rowId)) {
            checkState(currentCursor != null, "No active cursor");
            // rowId for cursors only reference the single current position
            return currentCursor.getCurrentPosition();
        }
        return rowIdBuffer.getPosition(rowId);
    }

    private static boolean isCursorRowId(long rowId)
    {
        return rowId == RESERVED_ROW_ID_FOR_CURSOR;
    }

    public void compactIfNeeded()
    {
        IntIterator iterator = compactionCandidates.iterator();
        while (iterator.hasNext()) {
            int pageId = iterator.nextInt();

            PageAccounting pageAccounting = pages.get(pageId);
            pageBytes -= pageAccounting.sizeOf();
            pageAccounting.compact();
            pageBytes += pageAccounting.sizeOf();
        }
        compactionCandidates.clear();
    }

    @VisibleForTesting
    int getCompactionCandidateCount()
    {
        return compactionCandidates.size();
    }

    @VisibleForTesting
    long getPageBytes()
    {
        return pageBytes;
    }

    public long sizeOf()
    {
        return INSTANCE_SIZE
                + pageBytes
                + pages.sizeOf()
                + rowIdBuffer.sizeOf()
                + compactionCandidates.sizeOf();
    }

    /**
     * Cursor that allows callers to advance through the registered page and dictate whether a specific position should
     * be preserved with a stable row ID. Row ID generation can be expensive in tight loops, so this allows callers to
     * quickly skip positions that won't be needed.
     */
    public static final class LoadCursor
            implements RowReference, AutoCloseable
    {
        private final PageAccounting pageAccounting;
        private final Runnable closeCallback;

        private int currentPosition;

        private LoadCursor(PageAccounting pageAccounting, int startingPosition, Runnable closeCallback)
        {
            this.pageAccounting = pageAccounting;
            this.currentPosition = startingPosition - 1;
            this.closeCallback = closeCallback;
        }

        private Page getPage()
        {
            return pageAccounting.getPage();
        }

        private int getCurrentPosition()
        {
            checkState(currentPosition >= 0, "Not yet advanced");
            return currentPosition;
        }

        public boolean advance()
        {
            if (currentPosition >= pageAccounting.getPage().getPositionCount() - 1) {
                return false;
            }
            currentPosition++;
            return true;
        }

        @Override
        public int compareTo(RowIdComparisonStrategy strategy, long rowId)
        {
            checkState(currentPosition >= 0, "Not yet advanced");
            return strategy.compare(RESERVED_ROW_ID_FOR_CURSOR, rowId);
        }

        @Override
        public boolean equals(RowIdHashStrategy strategy, long rowId)
        {
            checkState(currentPosition >= 0, "Not yet advanced");
            return strategy.equals(RESERVED_ROW_ID_FOR_CURSOR, rowId);
        }

        @Override
        public long hash(RowIdHashStrategy strategy)
        {
            checkState(currentPosition >= 0, "Not yet advanced");
            return strategy.hashCode(RESERVED_ROW_ID_FOR_CURSOR);
        }

        @Override
        public long allocateRowId()
        {
            checkState(currentPosition >= 0, "Not yet advanced");
            return pageAccounting.referencePosition(currentPosition);
        }

        @Override
        public void close()
        {
            closeCallback.run();
        }
    }

    private final class PageAccounting
    {
        private static final int COMPACTION_MIN_FILL_MULTIPLIER = 2;

        private final int pageId;
        private Page page;
        private boolean isPageLoaded;
        private long[] rowIds;
        // Start off locked to give the caller time to declare which rows to reference
        private boolean lockedPage = true;
        private int activePositions;

        public PageAccounting(int pageId, Page page)
        {
            this.pageId = pageId;
            this.page = page;
            rowIds = new long[page.getPositionCount()];
            Arrays.fill(rowIds, RowIdBuffer.UNKNOWN_ID);
        }

        /**
         * Record the position as referenced and return a corresponding stable row ID
         */
        public long referencePosition(int position)
        {
            long rowId = rowIds[position];
            if (rowId == RowIdBuffer.UNKNOWN_ID) {
                rowId = rowIdBuffer.allocateRowId(pageId, position);
                rowIds[position] = rowId;
                activePositions++;
            }
            return rowId;
        }

        /**
         * Locks the current page so that it can't be compacted (thus allowing for stable position-based access).
         */
        public void lockPage()
        {
            lockedPage = true;
        }

        /**
         * Unlocks the current page so that it becomes eligible for compaction.
         */
        public void unlockPage()
        {
            lockedPage = false;
        }

        public int getPageId()
        {
            return pageId;
        }

        public Page getPage()
        {
            return page;
        }

        /**
         * Dereferences the row ID from this page.
         */
        public void dereference(long rowId)
        {
            int position = rowIdBuffer.getPosition(rowId);
            checkArgument(rowId == rowIds[position], "rowId does not match this page");
            rowIds[position] = RowIdBuffer.UNKNOWN_ID;
            activePositions--;
            rowIdBuffer.deallocate(rowId);
        }

        public boolean isPruneEligible()
        {
            // Pruning is only allowed if the page is unlocked
            return !lockedPage && activePositions == 0;
        }

        public boolean isCompactionEligible()
        {
            // Compaction is only allowed if the page is unlocked
            return !lockedPage && activePositions * COMPACTION_MIN_FILL_MULTIPLIER < page.getPositionCount();
        }

        public void loadPageLoadIfNeeded()
        {
            if (!isPageLoaded && activePositions > 0) {
                page = page.getLoadedPage();
                isPageLoaded = true;
            }
        }

        public void compact()
        {
            checkState(!lockedPage, "Should not attempt compaction when page is locked");

            if (activePositions == page.getPositionCount()) {
                return;
            }

            loadPageLoadIfNeeded();

            int newIndex = 0;
            int[] positionsToKeep = new int[activePositions];
            long[] newRowIds = new long[activePositions];
            for (int i = 0; i < page.getPositionCount() && newIndex < positionsToKeep.length; i++) {
                long rowId = rowIds[i];
                positionsToKeep[newIndex] = i;
                newRowIds[newIndex] = rowId;
                newIndex += rowId == RowIdBuffer.UNKNOWN_ID ? 0 : 1;
            }
            verify(newIndex == activePositions);
            for (int i = 0; i < newRowIds.length; i++) {
                rowIdBuffer.setPosition(newRowIds[i], i);
            }

            // Compact page
            page = page.copyPositions(positionsToKeep, 0, positionsToKeep.length);
            rowIds = newRowIds;
        }

        public long sizeOf()
        {
            // Getting the size of a page forces a lazy page to be loaded, so only provide the size after an explicit decision to load
            long loadedPageSize = isPageLoaded ? page.getSizeInBytes() : 0;
            return PAGE_ACCOUNTING_INSTANCE_SIZE + loadedPageSize + SizeOf.sizeOf(rowIds);
        }
    }

    /**
     * Buffer abstracting a mapping between row IDs and their associated page IDs and positions.
     */
    private static class RowIdBuffer
    {
        public static final long UNKNOWN_ID = -1;
        private static final long INSTANCE_SIZE = instanceSize(RowIdBuffer.class);

        /*
         *  Memory layout:
         *  [INT] pageId1, [INT] position1,
         *  [INT] pageId2, [INT] position2,
         *  ...
         */
        private final IntBigArray buffer = new IntBigArray();

        private final LongBigArrayFIFOQueue emptySlots = new LongBigArrayFIFOQueue();

        private long capacity;

        /**
         * Provides a new row ID referencing the provided page position.
         *
         * @return ID referencing the provided page position
         */
        public long allocateRowId(int pageId, int position)
        {
            long newRowId;
            if (!emptySlots.isEmpty()) {
                newRowId = emptySlots.dequeueLong();
            }
            else {
                newRowId = capacity;
                capacity++;
                buffer.ensureCapacity(capacity * 2);
            }

            setPageId(newRowId, pageId);
            setPosition(newRowId, position);

            return newRowId;
        }

        public void deallocate(long rowId)
        {
            emptySlots.enqueue(rowId);
        }

        public int getPageId(long rowId)
        {
            return buffer.get(rowId * 2);
        }

        public void setPageId(long rowId, int pageId)
        {
            buffer.set(rowId * 2, pageId);
        }

        public int getPosition(long rowId)
        {
            return buffer.get(rowId * 2 + 1);
        }

        public void setPosition(long rowId, int position)
        {
            buffer.set(rowId * 2 + 1, position);
        }

        public long sizeOf()
        {
            return INSTANCE_SIZE + buffer.sizeOf() + emptySlots.sizeOf();
        }
    }

    private static class IntHashSet
            extends IntOpenHashSet
    {
        private static final long INSTANCE_SIZE = instanceSize(IntHashSet.class);

        public long sizeOf()
        {
            return INSTANCE_SIZE + SizeOf.sizeOf(key);
        }
    }
}
