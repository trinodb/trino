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
import io.trino.array.LongBigArray;
import io.trino.spi.Page;
import io.trino.util.HeapTraversal;
import io.trino.util.LongBigArrayFIFOQueue;
import jakarta.annotation.Nullable;

import java.util.function.LongConsumer;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.airlift.slice.SizeOf.instanceSize;
import static java.lang.Long.compareUnsigned;
import static java.lang.Math.abs;
import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;

/**
 * Memory Layout:
 * <pre>{@code
 *          +--------------------+   +--------------+
 *          |GroupIdToHeapBuffer |   |HeapNodeBuffer|
 *          +--------------------+   +--------------+
 * Group1+->+RootNodeIndex1+-------->+RowID1        |
 *          |RootNodeIndex2      |   |LeftChild1+-----+
 *          |...                 |   |RightChild1   | |
 *          +--------------------+   |RowID2    <-----+
 *          |HeapSize1           |   |LeftChild2    |
 *          |HeapSize2           |   |RightChild2   |
 *          |...                 |   |...           |
 *          +--------------------+   +--------------+
 * }</pre>
 */
public class GroupedTopNRowNumberAccumulator
{
    private static final long INSTANCE_SIZE = instanceSize(GroupedTopNRowNumberAccumulator.class);
    private static final long UNKNOWN_INDEX = -1;

    private final GroupIdToHeapBuffer groupIdToHeapBuffer = new GroupIdToHeapBuffer();
    private final HeapNodeBuffer heapNodeBuffer = new HeapNodeBuffer();
    private final HeapTraversal heapTraversal = new HeapTraversal();

    private final RowIdComparisonStrategy strategy;
    private final int topN;
    private final LongConsumer rowIdEvictionListener;

    public GroupedTopNRowNumberAccumulator(RowIdComparisonStrategy strategy, int topN, LongConsumer rowIdEvictionListener)
    {
        this.strategy = requireNonNull(strategy, "strategy is null");
        checkArgument(topN > 0, "topN must be greater than zero");
        this.topN = topN;
        this.rowIdEvictionListener = requireNonNull(rowIdEvictionListener, "rowIdEvictionListener is null");
    }

    public long sizeOf()
    {
        return INSTANCE_SIZE + groupIdToHeapBuffer.sizeOf() + heapNodeBuffer.sizeOf() + heapTraversal.sizeOf();
    }

    public int findFirstPositionToAdd(Page newPage, int groupCount, int[] groupIds, PageWithPositionComparator comparator, RowReferencePageManager pageManager, @Nullable long[] prefixes)
    {
        int currentTotalGroups = groupIdToHeapBuffer.getTotalGroups();
        groupIdToHeapBuffer.allocateGroupIfNeeded(groupCount);

        for (int position = 0; position < newPage.getPositionCount(); position++) {
            int groupId = groupIds[position];
            if (groupId >= currentTotalGroups || calculateRootRowNumber(groupId) < topN) {
                return position;
            }
            long heapRootNodeIndex = groupIdToHeapBuffer.getHeapRootNodeIndex(groupId);
            if (heapRootNodeIndex == UNKNOWN_INDEX) {
                return position;
            }
            if (prefixes != null) {
                int prefixComparison = compareUnsigned(prefixes[position], heapNodeBuffer.getPrefix(heapRootNodeIndex));
                if (prefixComparison != 0) {
                    if (prefixComparison < 0) {
                        return position;
                    }
                    continue;
                }
            }
            long rowId = heapNodeBuffer.getRowId(heapRootNodeIndex);
            Page rightPage = pageManager.getPage(rowId);
            int rightPosition = pageManager.getPosition(rowId);
            if (comparator.compareTo(newPage, position, rightPage, rightPosition) < 0) {
                return position;
            }
        }
        return -1;
    }

    /**
     * Add the specified row to this accumulator.
     * <p>
     * This may trigger row eviction callbacks if other rows have to be evicted to make space.
     *
     * @return true if this row was incorporated, false otherwise
     */
    public boolean add(int groupId, RowReference rowReference, long prefix)
    {
        groupIdToHeapBuffer.allocateGroupIfNeeded(groupId);

        long heapRootNodeIndex = groupIdToHeapBuffer.getHeapRootNodeIndex(groupId);
        if (heapRootNodeIndex == UNKNOWN_INDEX || calculateRootRowNumber(groupId) < topN) {
            heapInsert(groupId, rowReference.allocateRowId(), prefix);
            return true;
        }
        int comparison = compareUnsigned(prefix, heapNodeBuffer.getPrefix(heapRootNodeIndex));
        if (comparison == 0) {
            comparison = rowReference.compareTo(strategy, heapNodeBuffer.getRowId(heapRootNodeIndex));
        }
        if (comparison < 0) {
            heapPopAndInsert(groupId, rowReference.allocateRowId(), prefix, rowIdEvictionListener);
            return true;
        }
        return false;
    }

    /**
     * Drain the contents of groupId from this accumulator to the provided output row ID buffer.
     * <p>
     * Rows will be presented in increasing rank order. Draining will not trigger any row eviction callbacks.
     * After this method completion, the Accumulator will contain zero rows for the specified groupId.
     *
     * @return number of rows deposited to the output buffer
     */
    public long drainTo(int groupId, LongBigArray rowIdOutput)
    {
        long heapSize = groupIdToHeapBuffer.getHeapSize(groupId);
        rowIdOutput.ensureCapacity(heapSize);
        // Heap is inverted to output order, so insert back to front
        for (long i = heapSize - 1; i >= 0; i--) {
            rowIdOutput.set(i, peekRootRowId(groupId));
            // No eviction listener needed because this is an explicit caller directive to extract data
            heapPop(groupId, null);
        }
        return heapSize;
    }

    private long calculateRootRowNumber(int groupId)
    {
        return groupIdToHeapBuffer.getHeapSize(groupId);
    }

    private long peekRootRowId(int groupId)
    {
        long heapRootNodeIndex = groupIdToHeapBuffer.getHeapRootNodeIndex(groupId);
        checkArgument(heapRootNodeIndex != UNKNOWN_INDEX, "No root to peek");
        return heapNodeBuffer.getRowId(heapRootNodeIndex);
    }

    private int compare(long leftPrefix, long leftRowId, long rightPrefix, long rightRowId)
    {
        int comparison = compareUnsigned(leftPrefix, rightPrefix);
        if (comparison != 0) {
            return comparison;
        }
        return strategy.compare(leftRowId, rightRowId);
    }

    private long getChildIndex(long heapNodeIndex, HeapTraversal.Child child)
    {
        return child == HeapTraversal.Child.LEFT
                ? heapNodeBuffer.getLeftChildHeapIndex(heapNodeIndex)
                : heapNodeBuffer.getRightChildHeapIndex(heapNodeIndex);
    }

    private void setChildIndex(long heapNodeIndex, HeapTraversal.Child child, long newChildIndex)
    {
        if (child == HeapTraversal.Child.LEFT) {
            heapNodeBuffer.setLeftChildHeapIndex(heapNodeIndex, newChildIndex);
        }
        else {
            heapNodeBuffer.setRightChildHeapIndex(heapNodeIndex, newChildIndex);
        }
    }

    /**
     * Pop the root node off the group ID's max heap.
     *
     * @param contextEvictionListener optional callback for the root node that gets popped off
     */
    private void heapPop(int groupId, @Nullable LongConsumer contextEvictionListener)
    {
        long heapRootNodeIndex = groupIdToHeapBuffer.getHeapRootNodeIndex(groupId);
        checkArgument(heapRootNodeIndex != UNKNOWN_INDEX, "Group ID has an empty heap");

        long lastNodeIndex = heapDetachLastInsertionLeaf(groupId);
        long lastRowId = heapNodeBuffer.getRowId(lastNodeIndex);
        long lastPrefix = heapNodeBuffer.getPrefix(lastNodeIndex);
        heapNodeBuffer.deallocate(lastNodeIndex);

        if (lastNodeIndex == heapRootNodeIndex) {
            // The root is the last node remaining
            if (contextEvictionListener != null) {
                contextEvictionListener.accept(lastRowId);
            }
        }
        else {
            // Pop the root and insert lastRowId back into the heap to ensure a balanced tree
            heapPopAndInsert(groupId, lastRowId, lastPrefix, contextEvictionListener);
        }
    }

    /**
     * Detaches (but does not deallocate) the leaf in the bottom right-most position in the heap.
     * <p>
     * Given the fixed insertion order, the bottom right-most leaf will correspond to the last leaf node inserted into
     * the balanced heap.
     *
     * @return leaf node index that was detached from the heap
     */
    private long heapDetachLastInsertionLeaf(int groupId)
    {
        long heapRootNodeIndex = groupIdToHeapBuffer.getHeapRootNodeIndex(groupId);
        long heapSize = groupIdToHeapBuffer.getHeapSize(groupId);

        long previousNodeIndex = UNKNOWN_INDEX;
        HeapTraversal.Child childPosition = null;
        long currentNodeIndex = heapRootNodeIndex;

        heapTraversal.resetWithPathTo(heapSize);
        while (!heapTraversal.isTarget()) {
            previousNodeIndex = currentNodeIndex;
            childPosition = heapTraversal.nextChild();
            currentNodeIndex = getChildIndex(currentNodeIndex, childPosition);
            verify(currentNodeIndex != UNKNOWN_INDEX, "Target node must exist");
        }

        // Detach the last insertion leaf node, but do not deallocate yet
        if (previousNodeIndex == UNKNOWN_INDEX) {
            // Last insertion leaf was the root node
            groupIdToHeapBuffer.setHeapRootNodeIndex(groupId, UNKNOWN_INDEX);
            groupIdToHeapBuffer.setHeapSize(groupId, 0);
        }
        else {
            setChildIndex(previousNodeIndex, childPosition, UNKNOWN_INDEX);
            groupIdToHeapBuffer.addHeapSize(groupId, -1);
        }

        return currentNodeIndex;
    }

    /**
     * Inserts a new row into the heap for the specified group ID.
     * <p>
     * The technique involves traversing the heap from the root to a new bottom left-priority leaf position,
     * potentially swapping heap nodes along the way to find the proper insertion position for the new row.
     * Insertions always fill the left child before the right, and fill up an entire heap level before moving to the
     * next level.
     */
    private void heapInsert(int groupId, long newRowId, long newPrefix)
    {
        long heapRootNodeIndex = groupIdToHeapBuffer.getHeapRootNodeIndex(groupId);
        if (heapRootNodeIndex == UNKNOWN_INDEX) {
            // Heap is currently empty, so this will be the first node
            heapRootNodeIndex = heapNodeBuffer.allocateNewNode(newRowId, newPrefix);

            groupIdToHeapBuffer.setHeapRootNodeIndex(groupId, heapRootNodeIndex);
            groupIdToHeapBuffer.setHeapSize(groupId, 1);
            return;
        }

        long previousHeapNodeIndex = UNKNOWN_INDEX;
        HeapTraversal.Child childPosition = null;
        long currentHeapNodeIndex = heapRootNodeIndex;

        heapTraversal.resetWithPathTo(groupIdToHeapBuffer.getHeapSize(groupId) + 1);
        while (!heapTraversal.isTarget()) {
            long currentRowId = heapNodeBuffer.getRowId(currentHeapNodeIndex);
            long currentPrefix = heapNodeBuffer.getPrefix(currentHeapNodeIndex);
            if (compare(newPrefix, newRowId, currentPrefix, currentRowId) > 0) {
                // Swap the row values
                heapNodeBuffer.setRowId(currentHeapNodeIndex, newRowId);
                heapNodeBuffer.setPrefix(currentHeapNodeIndex, newPrefix);

                newRowId = currentRowId;
                newPrefix = currentPrefix;
            }

            previousHeapNodeIndex = currentHeapNodeIndex;
            childPosition = heapTraversal.nextChild();
            currentHeapNodeIndex = getChildIndex(currentHeapNodeIndex, childPosition);
        }

        verify(previousHeapNodeIndex != UNKNOWN_INDEX && childPosition != null, "heap must have at least one node before starting traversal");
        verify(currentHeapNodeIndex == UNKNOWN_INDEX, "New child shouldn't exist yet");

        long newHeapNodeIndex = heapNodeBuffer.allocateNewNode(newRowId, newPrefix);

        //  Link the new child to the parent
        setChildIndex(previousHeapNodeIndex, childPosition, newHeapNodeIndex);

        groupIdToHeapBuffer.incrementHeapSize(groupId);
    }

    /**
     * Pop the root node off the group ID's max heap and insert the newRowId.
     * <p>
     * These two operations are more efficient if performed together. The technique involves swapping the new row into
     * the root position, and applying a heap down bubbling operation to heap-ify.
     *
     * @param contextEvictionListener optional callback for the root node that gets popped off
     */
    private void heapPopAndInsert(int groupId, long newRowId, long newPrefix, @Nullable LongConsumer contextEvictionListener)
    {
        long heapRootNodeIndex = groupIdToHeapBuffer.getHeapRootNodeIndex(groupId);
        checkState(heapRootNodeIndex != UNKNOWN_INDEX, "popAndInsert() requires at least a root node");

        // Clear contents of the root node to create a vacancy for another row
        long poppedRowId = heapNodeBuffer.getRowId(heapRootNodeIndex);

        long currentNodeIndex = heapRootNodeIndex;
        while (true) {
            long maxChildNodeIndex = heapNodeBuffer.getLeftChildHeapIndex(currentNodeIndex);
            if (maxChildNodeIndex == UNKNOWN_INDEX) {
                // Left is always inserted before right, so a missing left child means there can't be a right child,
                // which means this must already be a leaf position.
                break;
            }
            long maxChildRowId = heapNodeBuffer.getRowId(maxChildNodeIndex);
            long maxChildPrefix = heapNodeBuffer.getPrefix(maxChildNodeIndex);

            long rightChildNodeIndex = heapNodeBuffer.getRightChildHeapIndex(currentNodeIndex);
            if (rightChildNodeIndex != UNKNOWN_INDEX) {
                long rightRowId = heapNodeBuffer.getRowId(rightChildNodeIndex);
                long rightPrefix = heapNodeBuffer.getPrefix(rightChildNodeIndex);
                if (compare(rightPrefix, rightRowId, maxChildPrefix, maxChildRowId) > 0) {
                    maxChildNodeIndex = rightChildNodeIndex;
                    maxChildRowId = rightRowId;
                    maxChildPrefix = rightPrefix;
                }
            }

            if (compare(newPrefix, newRowId, maxChildPrefix, maxChildRowId) >= 0) {
                // New row is greater than or equal to both children, so the heap invariant is satisfied by inserting the
                // new row at this position
                break;
            }

            // Swap the max child row value into the current node
            heapNodeBuffer.setRowId(currentNodeIndex, maxChildRowId);
            heapNodeBuffer.setPrefix(currentNodeIndex, maxChildPrefix);

            // Max child now has an unfilled vacancy, so continue processing with that as the current node
            currentNodeIndex = maxChildNodeIndex;
        }

        heapNodeBuffer.setRowId(currentNodeIndex, newRowId);
        heapNodeBuffer.setPrefix(currentNodeIndex, newPrefix);

        if (contextEvictionListener != null) {
            contextEvictionListener.accept(poppedRowId);
        }
    }

    /**
     * Sanity check the invariants of the underlying data structure.
     */
    @VisibleForTesting
    void verifyIntegrity()
    {
        long totalHeapNodes = 0;
        for (int groupId = 0; groupId < groupIdToHeapBuffer.getTotalGroups(); groupId++) {
            long heapSize = groupIdToHeapBuffer.getHeapSize(groupId);
            long rootNodeIndex = groupIdToHeapBuffer.getHeapRootNodeIndex(groupId);
            verify(rootNodeIndex == UNKNOWN_INDEX || calculateRootRowNumber(groupId) <= topN, "Max heap has more values than needed");
            IntegrityStats integrityStats = verifyHeapIntegrity(rootNodeIndex);
            verify(integrityStats.getNodeCount() == heapSize, "Recorded heap size does not match actual heap size");
            totalHeapNodes += integrityStats.getNodeCount();
        }
        verify(totalHeapNodes == heapNodeBuffer.getActiveNodeCount(), "Failed to deallocate some unused nodes");
    }

    private IntegrityStats verifyHeapIntegrity(long heapNodeIndex)
    {
        if (heapNodeIndex == UNKNOWN_INDEX) {
            return new IntegrityStats(0, 0);
        }
        long rowId = heapNodeBuffer.getRowId(heapNodeIndex);
        long leftChildHeapIndex = heapNodeBuffer.getLeftChildHeapIndex(heapNodeIndex);
        long rightChildHeapIndex = heapNodeBuffer.getRightChildHeapIndex(heapNodeIndex);

        long prefix = heapNodeBuffer.getPrefix(heapNodeIndex);
        if (leftChildHeapIndex != UNKNOWN_INDEX) {
            verify(compare(prefix, rowId, heapNodeBuffer.getPrefix(leftChildHeapIndex), heapNodeBuffer.getRowId(leftChildHeapIndex)) >= 0, "Max heap invariant violated");
        }
        if (rightChildHeapIndex != UNKNOWN_INDEX) {
            verify(leftChildHeapIndex != UNKNOWN_INDEX, "Left should always be inserted before right");
            verify(compare(prefix, rowId, heapNodeBuffer.getPrefix(rightChildHeapIndex), heapNodeBuffer.getRowId(rightChildHeapIndex)) >= 0, "Max heap invariant violated");
        }

        IntegrityStats leftIntegrityStats = verifyHeapIntegrity(leftChildHeapIndex);
        IntegrityStats rightIntegrityStats = verifyHeapIntegrity(rightChildHeapIndex);

        verify(abs(leftIntegrityStats.getMaxDepth() - rightIntegrityStats.getMaxDepth()) <= 1, "Heap not balanced");

        return new IntegrityStats(
                max(leftIntegrityStats.getMaxDepth(), rightIntegrityStats.getMaxDepth()) + 1,
                leftIntegrityStats.getNodeCount() + rightIntegrityStats.getNodeCount() + 1);
    }

    private static class IntegrityStats
    {
        private final long maxDepth;
        private final long nodeCount;

        public IntegrityStats(long maxDepth, long nodeCount)
        {
            this.maxDepth = maxDepth;
            this.nodeCount = nodeCount;
        }

        public long getMaxDepth()
        {
            return maxDepth;
        }

        public long getNodeCount()
        {
            return nodeCount;
        }
    }

    /**
     * Buffer abstracting a mapping from group ID to a heap. The group ID provides the index for all operations.
     */
    private static class GroupIdToHeapBuffer
    {
        private static final long INSTANCE_SIZE = instanceSize(GroupIdToHeapBuffer.class);

        /*
         *  Memory layout:
         *  [LONG] heapNodeIndex1,
         *  [LONG] heapNodeIndex2,
         *  ...
         */
        // Since we have a single element per group, this array is effectively indexed on group ID
        private final LongBigArray heapIndexBuffer = new LongBigArray(UNKNOWN_INDEX);

        /*
         *  Memory layout:
         *  [LONG] heapSize1,
         *  [LONG] heapSize2,
         *  ...
         */
        // Since we have a single element per group, this array is effectively indexed on group ID
        private final LongBigArray sizeBuffer = new LongBigArray(0);

        private int totalGroups;

        public void allocateGroupIfNeeded(int groupId)
        {
            if (totalGroups > groupId) {
                return;
            }
            // Group IDs generated by GroupByHash are always generated consecutively starting from 0, so observing a
            // group ID N means groups [0, N] inclusive must exist.
            totalGroups = groupId + 1;
            heapIndexBuffer.ensureCapacity(totalGroups);
            sizeBuffer.ensureCapacity(totalGroups);
        }

        public int getTotalGroups()
        {
            return totalGroups;
        }

        public long getHeapRootNodeIndex(int groupId)
        {
            return heapIndexBuffer.get(groupId);
        }

        public void setHeapRootNodeIndex(int groupId, long heapNodeIndex)
        {
            heapIndexBuffer.set(groupId, heapNodeIndex);
        }

        public long getHeapSize(int groupId)
        {
            return sizeBuffer.get(groupId);
        }

        public void setHeapSize(int groupId, long count)
        {
            sizeBuffer.set(groupId, count);
        }

        public void addHeapSize(int groupId, long delta)
        {
            sizeBuffer.add(groupId, delta);
        }

        public void incrementHeapSize(int groupId)
        {
            sizeBuffer.increment(groupId);
        }

        public long sizeOf()
        {
            return INSTANCE_SIZE + heapIndexBuffer.sizeOf() + sizeBuffer.sizeOf();
        }
    }

    /**
     * Buffer abstracting storage of nodes in the heap. Nodes are referenced by their node index for operations.
     */
    private static class HeapNodeBuffer
    {
        private static final long INSTANCE_SIZE = instanceSize(HeapNodeBuffer.class);
        private static final int POSITIONS_PER_ENTRY = 4;
        private static final int PREFIX_OFFSET = 1;
        private static final int LEFT_CHILD_HEAP_INDEX_OFFSET = 2;
        private static final int RIGHT_CHILD_HEAP_INDEX_OFFSET = 3;

        /*
         *  Memory layout:
         *  [LONG] rowId1, [LONG] prefix1, [LONG] leftChildNodeIndex1, [LONG] rightChildNodeIndex1,
         *  [LONG] rowId2, [LONG] prefix2, [LONG] leftChildNodeIndex2, [LONG] rightChildNodeIndex2,
         *  ...
         */
        private final LongBigArray buffer = new LongBigArray();

        private final LongBigArrayFIFOQueue emptySlots = new LongBigArrayFIFOQueue();

        private long capacity;

        /**
         * Allocates storage for a new heap node.
         *
         * @return index referencing the node
         */
        public long allocateNewNode(long rowId, long prefix)
        {
            long newHeapIndex;
            if (!emptySlots.isEmpty()) {
                newHeapIndex = emptySlots.dequeueLong();
            }
            else {
                newHeapIndex = capacity;
                capacity++;
                buffer.ensureCapacity(capacity * POSITIONS_PER_ENTRY);
            }

            setRowId(newHeapIndex, rowId);
            setPrefix(newHeapIndex, prefix);
            setLeftChildHeapIndex(newHeapIndex, UNKNOWN_INDEX);
            setRightChildHeapIndex(newHeapIndex, UNKNOWN_INDEX);

            return newHeapIndex;
        }

        public void deallocate(long index)
        {
            emptySlots.enqueue(index);
        }

        public long getActiveNodeCount()
        {
            return capacity - emptySlots.longSize();
        }

        public long getRowId(long index)
        {
            return buffer.get(index * POSITIONS_PER_ENTRY);
        }

        public void setRowId(long index, long rowId)
        {
            buffer.set(index * POSITIONS_PER_ENTRY, rowId);
        }

        public long getPrefix(long index)
        {
            return buffer.get(index * POSITIONS_PER_ENTRY + PREFIX_OFFSET);
        }

        public void setPrefix(long index, long prefix)
        {
            buffer.set(index * POSITIONS_PER_ENTRY + PREFIX_OFFSET, prefix);
        }

        public long getLeftChildHeapIndex(long index)
        {
            return buffer.get(index * POSITIONS_PER_ENTRY + LEFT_CHILD_HEAP_INDEX_OFFSET);
        }

        public void setLeftChildHeapIndex(long index, long childHeapIndex)
        {
            buffer.set(index * POSITIONS_PER_ENTRY + LEFT_CHILD_HEAP_INDEX_OFFSET, childHeapIndex);
        }

        public long getRightChildHeapIndex(long index)
        {
            return buffer.get(index * POSITIONS_PER_ENTRY + RIGHT_CHILD_HEAP_INDEX_OFFSET);
        }

        public void setRightChildHeapIndex(long index, long childHeapIndex)
        {
            buffer.set(index * POSITIONS_PER_ENTRY + RIGHT_CHILD_HEAP_INDEX_OFFSET, childHeapIndex);
        }

        public long sizeOf()
        {
            return INSTANCE_SIZE + buffer.sizeOf() + emptySlots.sizeOf();
        }
    }
}
