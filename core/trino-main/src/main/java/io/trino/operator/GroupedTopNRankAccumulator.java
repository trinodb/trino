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

import javax.annotation.Nullable;

import java.util.function.LongConsumer;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.airlift.slice.SizeOf.instanceSize;
import static java.lang.Math.abs;
import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;

/**
 * Memory Layout:
 * <pre>
 *                    +----------------------+
 *                    |PeerGroupLookup       |
 *                    +----------------------+
 *                    |Map:                  |
 * (Group1, RowID4)+->+(Group,RowID):HeapNode+-+
 *                    +----------------------+ |
 *                                             |
 *                    +--------------------+   |  +---------------+    +---------------+
 *                    |GroupIdToHeapBuffer |   |  |HeapNodeBuffer |    |PeerGroupBuffer|
 *                    +--------------------+   |  +---------------+    +---------------+
 *           Group1+->+RootNodeIndex1+----------->+PeerGroupIndex1+--->+RowID1         |
 *                    |RootNodeIndex2      |   |  |PeerGroupCount1|    |NextPeerIndex1+--+
 *                    |...                 |   |  |LeftChild1   +----+ |RowID2         | |
 *                    +--------------------+   |  |RightChild1    |  | |NextPeerIndex2 | |
 *                    |ValueCount1         |   +->+PeerGroupIndex2+<-+ |RowID3   <-------+
 *                    |HeapSize1           |      |PeerGroupCount2|    |NextPeerIndex3 |
 *                    |ValueCount2         |      |LeftChild2     |    |...            |
 *                    |HeapSize2           |      |RightChild2    |    +---------------+
 *                    |...                 |      |...            |
 *                    +--------------------+      +---------------+
 * </pre>
 */
public class GroupedTopNRankAccumulator
{
    private static final long INSTANCE_SIZE = instanceSize(GroupedTopNRankAccumulator.class);
    private static final long UNKNOWN_INDEX = -1;
    private static final long NULL_GROUP_ID = -1;

    private final GroupIdToHeapBuffer groupIdToHeapBuffer = new GroupIdToHeapBuffer();
    private final HeapNodeBuffer heapNodeBuffer = new HeapNodeBuffer();
    private final PeerGroupBuffer peerGroupBuffer = new PeerGroupBuffer();
    private final HeapTraversal heapTraversal = new HeapTraversal();

    // Map from (Group ID, Row Value) to Heap Node Index where the value is stored
    private final TopNPeerGroupLookup peerGroupLookup;

    private final RowIdComparisonHashStrategy strategy;
    private final int topN;
    private final LongConsumer rowIdEvictionListener;

    public GroupedTopNRankAccumulator(RowIdComparisonHashStrategy strategy, int topN, LongConsumer rowIdEvictionListener)
    {
        this.strategy = requireNonNull(strategy, "strategy is null");
        this.peerGroupLookup = new TopNPeerGroupLookup(10_000, strategy, NULL_GROUP_ID, UNKNOWN_INDEX);
        checkArgument(topN > 0, "topN must be greater than zero");
        this.topN = topN;
        this.rowIdEvictionListener = requireNonNull(rowIdEvictionListener, "rowIdEvictionListener is null");
    }

    public long sizeOf()
    {
        return INSTANCE_SIZE
                + groupIdToHeapBuffer.sizeOf()
                + heapNodeBuffer.sizeOf()
                + peerGroupBuffer.sizeOf()
                + heapTraversal.sizeOf()
                + peerGroupLookup.sizeOf();
    }

    public int findFirstPositionToAdd(Page newPage, int groupCount, int[] groupIds, PageWithPositionComparator comparator, RowReferencePageManager pageManager)
    {
        int currentGroups = groupIdToHeapBuffer.getTotalGroups();
        groupIdToHeapBuffer.allocateGroupIfNeeded(groupCount);

        for (int position = 0; position < newPage.getPositionCount(); position++) {
            int groupId = groupIds[position];
            if (groupId >= currentGroups || groupIdToHeapBuffer.getHeapValueCount(groupId) < topN) {
                return position;
            }
            long heapRootNodeIndex = groupIdToHeapBuffer.getHeapRootNodeIndex(groupId);
            if (heapRootNodeIndex == UNKNOWN_INDEX) {
                return position;
            }
            long rightPageRowId = peekRootRowIdByHeapNodeIndex(heapRootNodeIndex);
            Page rightPage = pageManager.getPage(rightPageRowId);
            int rightPosition = pageManager.getPosition(rightPageRowId);
            // If the current position is equal to or less than the current heap root index, then we may need to insert it
            if (comparator.compareTo(newPage, position, rightPage, rightPosition) <= 0) {
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
    public boolean add(int groupId, RowReference rowReference)
    {
        // Insert to any existing peer groups first (heap nodes contain distinct values)
        long peerHeapNodeIndex = peerGroupLookup.get(groupId, rowReference);
        if (peerHeapNodeIndex != UNKNOWN_INDEX) {
            directPeerGroupInsert(groupId, peerHeapNodeIndex, rowReference.allocateRowId());
            if (calculateRootRank(groupId, groupIdToHeapBuffer.getHeapRootNodeIndex(groupId)) > topN) {
                heapPop(groupId, rowIdEvictionListener);
            }
            // Return true because heapPop is guaranteed not to evict the newly inserted row (by definition of rank)
            return true;
        }

        groupIdToHeapBuffer.allocateGroupIfNeeded(groupId);
        if (groupIdToHeapBuffer.getHeapValueCount(groupId) < topN) {
            // Always safe to insert if total number of values is still less than topN
            long newPeerGroupIndex = peerGroupBuffer.allocateNewNode(rowReference.allocateRowId(), UNKNOWN_INDEX);
            heapInsert(groupId, newPeerGroupIndex, 1);
            return true;
        }
        long heapRootNodeIndex = groupIdToHeapBuffer.getHeapRootNodeIndex(groupId);
        if (rowReference.compareTo(strategy, peekRootRowIdByHeapNodeIndex(heapRootNodeIndex)) < 0) {
            // Given that total number of values >= topN, we can only consider values that are less than the root (otherwise topN would be violated)
            long newPeerGroupIndex = peerGroupBuffer.allocateNewNode(rowReference.allocateRowId(), UNKNOWN_INDEX);
            // Rank will increase by +1 after insertion, so only need to pop if root rank is already == topN.
            if (calculateRootRank(groupId, heapRootNodeIndex) < topN) {
                heapInsert(groupId, newPeerGroupIndex, 1);
            }
            else {
                heapPopAndInsert(groupId, newPeerGroupIndex, 1, rowIdEvictionListener);
            }
            return true;
        }
        // Row cannot be accepted because the total number of values >= topN, and the row is greater than the root (meaning it's rank would be at least topN+1).
        return false;
    }

    /**
     * Drain the contents of this accumulator to the provided output row ID and ranking buffer.
     * <p>
     * Rows will be presented in increasing rank order. Draining will not trigger any row eviction callbacks.
     * After this method completion, the Accumulator will contain zero rows for the specified groupId.
     *
     * @return number of rows deposited to the output buffers
     */
    public long drainTo(int groupId, LongBigArray rowIdOutput, LongBigArray rankingOutput)
    {
        long valueCount = groupIdToHeapBuffer.getHeapValueCount(groupId);
        rowIdOutput.ensureCapacity(valueCount);
        rankingOutput.ensureCapacity(valueCount);

        // Heap is inverted to output order, so insert back to front
        long insertionIndex = valueCount - 1;
        while (insertionIndex >= 0) {
            long heapRootNodeIndex = groupIdToHeapBuffer.getHeapRootNodeIndex(groupId);
            verify(heapRootNodeIndex != UNKNOWN_INDEX);

            long peerGroupIndex = heapNodeBuffer.getPeerGroupIndex(heapRootNodeIndex);
            verify(peerGroupIndex != UNKNOWN_INDEX, "Peer group should have at least one value");

            long rank = calculateRootRank(groupId, heapRootNodeIndex);
            do {
                rowIdOutput.set(insertionIndex, peerGroupBuffer.getRowId(peerGroupIndex));
                rankingOutput.set(insertionIndex, rank);
                insertionIndex--;
                peerGroupIndex = peerGroupBuffer.getNextPeerIndex(peerGroupIndex);
            }
            while (peerGroupIndex != UNKNOWN_INDEX);

            heapPop(groupId, null);
        }
        return valueCount;
    }

    /**
     * Drain the contents of this accumulator to the provided output row ID.
     * <p>
     * Rows will be presented in increasing rank order. Draining will not trigger any row eviction callbacks.
     * After this method completion, the Accumulator will contain zero rows for the specified groupId.
     *
     * @return number of rows deposited to the output buffer
     */
    public long drainTo(int groupId, LongBigArray rowIdOutput)
    {
        long valueCount = groupIdToHeapBuffer.getHeapValueCount(groupId);
        rowIdOutput.ensureCapacity(valueCount);

        // Heap is inverted to output order, so insert back to front
        long insertionIndex = valueCount - 1;
        while (insertionIndex >= 0) {
            long heapRootNodeIndex = groupIdToHeapBuffer.getHeapRootNodeIndex(groupId);
            verify(heapRootNodeIndex != UNKNOWN_INDEX);

            long peerGroupIndex = heapNodeBuffer.getPeerGroupIndex(heapRootNodeIndex);
            verify(peerGroupIndex != UNKNOWN_INDEX, "Peer group should have at least one value");

            do {
                rowIdOutput.set(insertionIndex, peerGroupBuffer.getRowId(peerGroupIndex));
                insertionIndex--;
                peerGroupIndex = peerGroupBuffer.getNextPeerIndex(peerGroupIndex);
            }
            while (peerGroupIndex != UNKNOWN_INDEX);

            heapPop(groupId, null);
        }
        return valueCount;
    }

    private long calculateRootRank(int groupId, long heapRootIndex)
    {
        long heapValueCount = groupIdToHeapBuffer.getHeapValueCount(groupId);
        checkArgument(heapRootIndex != UNKNOWN_INDEX, "Group does not have a root");
        long rootPeerGroupCount = heapNodeBuffer.getPeerGroupCount(heapRootIndex);
        return heapValueCount - rootPeerGroupCount + 1;
    }

    private void directPeerGroupInsert(int groupId, long heapNodeIndex, long rowId)
    {
        long existingPeerGroupIndex = heapNodeBuffer.getPeerGroupIndex(heapNodeIndex);
        long newPeerGroupIndex = peerGroupBuffer.allocateNewNode(rowId, existingPeerGroupIndex);
        heapNodeBuffer.setPeerGroupIndex(heapNodeIndex, newPeerGroupIndex);
        heapNodeBuffer.incrementPeerGroupCount(heapNodeIndex);
        groupIdToHeapBuffer.incrementHeapValueCount(groupId);
    }

    private long peekRootRowIdByHeapNodeIndex(long heapRootNodeIndex)
    {
        checkArgument(heapRootNodeIndex != UNKNOWN_INDEX, "Group has nothing to peek");
        return peerGroupBuffer.getRowId(heapNodeBuffer.getPeerGroupIndex(heapRootNodeIndex));
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

        long lastHeapNodeIndex = heapDetachLastInsertionLeaf(groupId);
        long lastPeerGroupIndex = heapNodeBuffer.getPeerGroupIndex(lastHeapNodeIndex);
        long lastPeerGroupCount = heapNodeBuffer.getPeerGroupCount(lastHeapNodeIndex);

        if (lastHeapNodeIndex == heapRootNodeIndex) {
            // The root is the last node remaining
            dropHeapNodePeerGroup(groupId, lastHeapNodeIndex, contextEvictionListener);
        }
        else {
            // Pop the root and insert the last peer group back into the heap to ensure a balanced tree
            heapPopAndInsert(groupId, lastPeerGroupIndex, lastPeerGroupCount, contextEvictionListener);
        }

        // peerGroupLookup entry will be updated by definition of inserting the last peer group into a new node
        heapNodeBuffer.deallocate(lastHeapNodeIndex);
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
            groupIdToHeapBuffer.setHeapValueCount(groupId, 0);
            groupIdToHeapBuffer.setHeapSize(groupId, 0);
        }
        else {
            setChildIndex(previousNodeIndex, childPosition, UNKNOWN_INDEX);
            groupIdToHeapBuffer.addHeapValueCount(groupId, -heapNodeBuffer.getPeerGroupCount(currentNodeIndex));
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
    private void heapInsert(int groupId, long newPeerGroupIndex, long newPeerGroupCount)
    {
        long newCanonicalRowId = peerGroupBuffer.getRowId(newPeerGroupIndex);

        long heapRootNodeIndex = groupIdToHeapBuffer.getHeapRootNodeIndex(groupId);
        if (heapRootNodeIndex == UNKNOWN_INDEX) {
            // Heap is currently empty, so this will be the first node
            heapRootNodeIndex = heapNodeBuffer.allocateNewNode(newPeerGroupIndex, newPeerGroupCount);
            verify(peerGroupLookup.put(groupId, newCanonicalRowId, heapRootNodeIndex) == UNKNOWN_INDEX);
            groupIdToHeapBuffer.setHeapRootNodeIndex(groupId, heapRootNodeIndex);
            groupIdToHeapBuffer.setHeapValueCount(groupId, newPeerGroupCount);
            groupIdToHeapBuffer.setHeapSize(groupId, 1);
            return;
        }

        long previousHeapNodeIndex = UNKNOWN_INDEX;
        HeapTraversal.Child childPosition = null;
        long currentHeapNodeIndex = heapRootNodeIndex;
        boolean swapped = false;

        groupIdToHeapBuffer.addHeapValueCount(groupId, newPeerGroupCount);
        groupIdToHeapBuffer.incrementHeapSize(groupId);
        heapTraversal.resetWithPathTo(groupIdToHeapBuffer.getHeapSize(groupId));
        while (!heapTraversal.isTarget()) {
            long peerGroupIndex = heapNodeBuffer.getPeerGroupIndex(currentHeapNodeIndex);
            long currentCanonicalRowId = peerGroupBuffer.getRowId(peerGroupIndex);
            // We can short-circuit the check if a parent has already been swapped because the new row to insert must
            // be greater than all of it's children.
            if (swapped || strategy.compare(newCanonicalRowId, currentCanonicalRowId) > 0) {
                long peerGroupCount = heapNodeBuffer.getPeerGroupCount(currentHeapNodeIndex);

                // Swap the peer groups
                heapNodeBuffer.setPeerGroupIndex(currentHeapNodeIndex, newPeerGroupIndex);
                heapNodeBuffer.setPeerGroupCount(currentHeapNodeIndex, newPeerGroupCount);
                peerGroupLookup.put(groupId, newCanonicalRowId, currentHeapNodeIndex);

                newPeerGroupIndex = peerGroupIndex;
                newPeerGroupCount = peerGroupCount;
                newCanonicalRowId = currentCanonicalRowId;
                swapped = true;
            }

            previousHeapNodeIndex = currentHeapNodeIndex;
            childPosition = heapTraversal.nextChild();
            currentHeapNodeIndex = getChildIndex(currentHeapNodeIndex, childPosition);
        }

        verify(previousHeapNodeIndex != UNKNOWN_INDEX && childPosition != null, "heap must have at least one node before starting traversal");
        verify(currentHeapNodeIndex == UNKNOWN_INDEX, "New child shouldn't exist yet");

        long newHeapNodeIndex = heapNodeBuffer.allocateNewNode(newPeerGroupIndex, newPeerGroupCount);
        peerGroupLookup.put(groupId, newCanonicalRowId, newHeapNodeIndex);

        //  Link the new child to the parent
        setChildIndex(previousHeapNodeIndex, childPosition, newHeapNodeIndex);
    }

    /**
     * Pop the root off the group ID's max heap and insert the new peer group.
     * <p>
     * These two operations are more efficient if performed together. The technique involves swapping the new row into
     * the root position, and applying a heap down bubbling operation to heap-ify.
     *
     * @param contextEvictionListener optional callback for the root node that gets popped off
     */
    private void heapPopAndInsert(int groupId, long newPeerGroupIndex, long newPeerGroupCount, @Nullable LongConsumer contextEvictionListener)
    {
        long heapRootNodeIndex = groupIdToHeapBuffer.getHeapRootNodeIndex(groupId);
        checkState(heapRootNodeIndex != UNKNOWN_INDEX, "popAndInsert() requires at least a root node");

        // Clear contents of the root node to create a vacancy for the new peer group
        groupIdToHeapBuffer.addHeapValueCount(groupId, newPeerGroupCount - heapNodeBuffer.getPeerGroupCount(heapRootNodeIndex));
        dropHeapNodePeerGroup(groupId, heapRootNodeIndex, contextEvictionListener);

        long newCanonicalRowId = peerGroupBuffer.getRowId(newPeerGroupIndex);

        long currentNodeIndex = heapRootNodeIndex;
        while (true) {
            long maxChildNodeIndex = heapNodeBuffer.getLeftChildHeapIndex(currentNodeIndex);
            if (maxChildNodeIndex == UNKNOWN_INDEX) {
                // Left is always inserted before right, so a missing left child means there can't be a right child,
                // which means this must already be a leaf position.
                break;
            }
            long maxChildPeerGroupIndex = heapNodeBuffer.getPeerGroupIndex(maxChildNodeIndex);
            long maxChildCanonicalRowId = peerGroupBuffer.getRowId(maxChildPeerGroupIndex);

            long rightChildNodeIndex = heapNodeBuffer.getRightChildHeapIndex(currentNodeIndex);
            if (rightChildNodeIndex != UNKNOWN_INDEX) {
                long rightChildPeerGroupIndex = heapNodeBuffer.getPeerGroupIndex(rightChildNodeIndex);
                long rightChildCanonicalRowId = peerGroupBuffer.getRowId(rightChildPeerGroupIndex);
                if (strategy.compare(rightChildCanonicalRowId, maxChildCanonicalRowId) > 0) {
                    maxChildNodeIndex = rightChildNodeIndex;
                    maxChildPeerGroupIndex = rightChildPeerGroupIndex;
                    maxChildCanonicalRowId = rightChildCanonicalRowId;
                }
            }

            if (strategy.compare(newCanonicalRowId, maxChildCanonicalRowId) >= 0) {
                // New row is greater than or equal to both children, so the heap invariant is satisfied by inserting the
                // new row at this position
                break;
            }

            // Swap the max child row value into the current node
            heapNodeBuffer.setPeerGroupIndex(currentNodeIndex, maxChildPeerGroupIndex);
            heapNodeBuffer.setPeerGroupCount(currentNodeIndex, heapNodeBuffer.getPeerGroupCount(maxChildNodeIndex));
            peerGroupLookup.put(groupId, maxChildCanonicalRowId, currentNodeIndex);

            // Max child now has an unfilled vacancy, so continue processing with that as the current node
            currentNodeIndex = maxChildNodeIndex;
        }

        heapNodeBuffer.setPeerGroupIndex(currentNodeIndex, newPeerGroupIndex);
        heapNodeBuffer.setPeerGroupCount(currentNodeIndex, newPeerGroupCount);
        peerGroupLookup.put(groupId, newCanonicalRowId, currentNodeIndex);
    }

    /**
     * Deallocates all peer group associations for this heap node, leaving a structural husk with no contents. Assumes
     * that any required group level metric changes are handled externally.
     */
    private void dropHeapNodePeerGroup(int groupId, long heapNodeIndex, @Nullable LongConsumer contextEvictionListener)
    {
        long peerGroupIndex = heapNodeBuffer.getPeerGroupIndex(heapNodeIndex);
        checkState(peerGroupIndex != UNKNOWN_INDEX, "Heap node must have at least one peer group");

        long rowId = peerGroupBuffer.getRowId(peerGroupIndex);
        long nextPeerIndex = peerGroupBuffer.getNextPeerIndex(peerGroupIndex);
        peerGroupBuffer.deallocate(peerGroupIndex);
        verify(peerGroupLookup.remove(groupId, rowId) == heapNodeIndex);

        if (contextEvictionListener != null) {
            contextEvictionListener.accept(rowId);
        }

        peerGroupIndex = nextPeerIndex;

        while (peerGroupIndex != UNKNOWN_INDEX) {
            rowId = peerGroupBuffer.getRowId(peerGroupIndex);
            nextPeerIndex = peerGroupBuffer.getNextPeerIndex(peerGroupIndex);
            peerGroupBuffer.deallocate(peerGroupIndex);

            if (contextEvictionListener != null) {
                contextEvictionListener.accept(rowId);
            }

            peerGroupIndex = nextPeerIndex;
        }
    }

    /**
     * Sanity check the invariants of the underlying data structure.
     */
    @VisibleForTesting
    void verifyIntegrity()
    {
        long totalHeapNodes = 0;
        long totalValueCount = 0;
        for (int groupId = 0; groupId < groupIdToHeapBuffer.getTotalGroups(); groupId++) {
            long heapSize = groupIdToHeapBuffer.getHeapSize(groupId);
            long heapValueCount = groupIdToHeapBuffer.getHeapValueCount(groupId);
            long rootNodeIndex = groupIdToHeapBuffer.getHeapRootNodeIndex(groupId);
            verify(rootNodeIndex == UNKNOWN_INDEX || calculateRootRank(groupId, rootNodeIndex) <= topN, "Max heap has more values than needed");
            IntegrityStats integrityStats = verifyHeapIntegrity(groupId, rootNodeIndex);
            verify(integrityStats.getPeerGroupCount() == heapSize, "Recorded heap size does not match actual heap size");
            totalHeapNodes += integrityStats.getPeerGroupCount();
            verify(integrityStats.getValueCount() == heapValueCount, "Recorded value count does not match actual value count");
            totalValueCount += integrityStats.getValueCount();
        }
        verify(totalHeapNodes == heapNodeBuffer.getActiveNodeCount(), "Failed to deallocate some unused nodes");
        verify(totalHeapNodes == peerGroupLookup.size(), "Peer group lookup does not have the right number of entries");
        verify(totalValueCount == peerGroupBuffer.getActiveNodeCount(), "Failed to deallocate some unused nodes");
    }

    private IntegrityStats verifyHeapIntegrity(int groupId, long heapNodeIndex)
    {
        if (heapNodeIndex == UNKNOWN_INDEX) {
            return new IntegrityStats(0, 0, 0);
        }
        long peerGroupIndex = heapNodeBuffer.getPeerGroupIndex(heapNodeIndex);
        long peerGroupCount = heapNodeBuffer.getPeerGroupCount(heapNodeIndex);
        long leftChildHeapIndex = heapNodeBuffer.getLeftChildHeapIndex(heapNodeIndex);
        long rightChildHeapIndex = heapNodeBuffer.getRightChildHeapIndex(heapNodeIndex);

        long actualPeerGroupCount = 0;
        long previousRowId;
        long rowId = -1; // Arbitrary initial value that will be overwritten
        do {
            previousRowId = rowId;
            rowId = peerGroupBuffer.getRowId(peerGroupIndex);
            actualPeerGroupCount++;
            if (actualPeerGroupCount >= 2) {
                verify(strategy.equals(rowId, previousRowId), "Row value does not belong in peer group");
            }
            verify(peerGroupLookup.get(groupId, rowId) == heapNodeIndex, "Mismatch between peer group and lookup mapping");

            peerGroupIndex = peerGroupBuffer.getNextPeerIndex(peerGroupIndex);
        }
        while (peerGroupIndex != UNKNOWN_INDEX);
        verify(actualPeerGroupCount == peerGroupCount, "Recorded peer group count does not match actual");

        if (leftChildHeapIndex != UNKNOWN_INDEX) {
            verify(strategy.compare(rowId, peerGroupBuffer.getRowId(heapNodeBuffer.getPeerGroupIndex(leftChildHeapIndex))) > 0, "Max heap invariant violated");
        }
        if (rightChildHeapIndex != UNKNOWN_INDEX) {
            verify(leftChildHeapIndex != UNKNOWN_INDEX, "Left should always be inserted before right");
            verify(strategy.compare(rowId, peerGroupBuffer.getRowId(heapNodeBuffer.getPeerGroupIndex(rightChildHeapIndex))) > 0, "Max heap invariant violated");
        }

        IntegrityStats leftIntegrityStats = verifyHeapIntegrity(groupId, leftChildHeapIndex);
        IntegrityStats rightIntegrityStats = verifyHeapIntegrity(groupId, rightChildHeapIndex);

        verify(abs(leftIntegrityStats.getMaxDepth() - rightIntegrityStats.getMaxDepth()) <= 1, "Heap not balanced");

        return new IntegrityStats(
                max(leftIntegrityStats.getMaxDepth(), rightIntegrityStats.getMaxDepth()) + 1,
                leftIntegrityStats.getPeerGroupCount() + rightIntegrityStats.getPeerGroupCount() + 1,
                leftIntegrityStats.getValueCount() + rightIntegrityStats.getValueCount() + peerGroupCount);
    }

    private static class IntegrityStats
    {
        private final long maxDepth;
        private final long peerGroupCount;
        private final long valueCount;

        public IntegrityStats(long maxDepth, long peerGroupCount, long valueCount)
        {
            this.maxDepth = maxDepth;
            this.peerGroupCount = peerGroupCount;
            this.valueCount = valueCount;
        }

        public long getMaxDepth()
        {
            return maxDepth;
        }

        public long getPeerGroupCount()
        {
            return peerGroupCount;
        }

        public long getValueCount()
        {
            return valueCount;
        }
    }

    /**
     * Buffer abstracting a mapping from group ID to a heap. The group ID provides the index for all operations.
     */
    private static final class GroupIdToHeapBuffer
    {
        private static final long INSTANCE_SIZE = instanceSize(GroupIdToHeapBuffer.class);
        private static final int METRICS_POSITIONS_PER_ENTRY = 2;
        private static final int METRICS_HEAP_SIZE_OFFSET = 1;

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
         *  [LONG] valueCount1, [LONG] heapSize1,
         *  [LONG] valueCount2, [LONG] heapSize2,
         *  ...
         */
        private final LongBigArray metricsBuffer = new LongBigArray(0);

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
            metricsBuffer.ensureCapacity((long) totalGroups * METRICS_POSITIONS_PER_ENTRY);
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

        public long getHeapValueCount(int groupId)
        {
            return metricsBuffer.get((long) groupId * METRICS_POSITIONS_PER_ENTRY);
        }

        public void setHeapValueCount(int groupId, long count)
        {
            metricsBuffer.set((long) groupId * METRICS_POSITIONS_PER_ENTRY, count);
        }

        public void addHeapValueCount(int groupId, long delta)
        {
            metricsBuffer.add((long) groupId * METRICS_POSITIONS_PER_ENTRY, delta);
        }

        public void incrementHeapValueCount(int groupId)
        {
            metricsBuffer.increment((long) groupId * METRICS_POSITIONS_PER_ENTRY);
        }

        public long getHeapSize(int groupId)
        {
            return metricsBuffer.get((long) groupId * METRICS_POSITIONS_PER_ENTRY + METRICS_HEAP_SIZE_OFFSET);
        }

        public void setHeapSize(int groupId, long size)
        {
            metricsBuffer.set((long) groupId * METRICS_POSITIONS_PER_ENTRY + METRICS_HEAP_SIZE_OFFSET, size);
        }

        public void addHeapSize(int groupId, long delta)
        {
            metricsBuffer.add((long) groupId * METRICS_POSITIONS_PER_ENTRY + METRICS_HEAP_SIZE_OFFSET, delta);
        }

        public void incrementHeapSize(int groupId)
        {
            metricsBuffer.increment((long) groupId * METRICS_POSITIONS_PER_ENTRY + METRICS_HEAP_SIZE_OFFSET);
        }

        public long sizeOf()
        {
            return INSTANCE_SIZE + heapIndexBuffer.sizeOf() + metricsBuffer.sizeOf();
        }
    }

    /**
     * Buffer abstracting storage of nodes in the heap. Nodes are referenced by their node index for operations.
     */
    private static final class HeapNodeBuffer
    {
        private static final long INSTANCE_SIZE = instanceSize(HeapNodeBuffer.class);
        private static final int POSITIONS_PER_ENTRY = 4;
        private static final int PEER_GROUP_COUNT_OFFSET = 1;
        private static final int LEFT_CHILD_HEAP_INDEX_OFFSET = 2;
        private static final int RIGHT_CHILD_HEAP_INDEX_OFFSET = 3;

        /*
         *  Memory layout:
         *  [LONG] peerGroupIndex1, [LONG] peerGroupCount1, [LONG] leftChildNodeIndex1, [LONG] rightChildNodeIndex1,
         *  [LONG] peerGroupIndex2, [LONG] peerGroupCount2, [LONG] leftChildNodeIndex2, [LONG] rightChildNodeIndex2,
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
        public long allocateNewNode(long peerGroupIndex, long peerGroupCount)
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

            setPeerGroupIndex(newHeapIndex, peerGroupIndex);
            setPeerGroupCount(newHeapIndex, peerGroupCount);
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

        public long getPeerGroupIndex(long index)
        {
            return buffer.get(index * POSITIONS_PER_ENTRY);
        }

        public void setPeerGroupIndex(long index, long peerGroupIndex)
        {
            buffer.set(index * POSITIONS_PER_ENTRY, peerGroupIndex);
        }

        public long getPeerGroupCount(long index)
        {
            return buffer.get(index * POSITIONS_PER_ENTRY + PEER_GROUP_COUNT_OFFSET);
        }

        public void setPeerGroupCount(long index, long peerGroupCount)
        {
            buffer.set(index * POSITIONS_PER_ENTRY + PEER_GROUP_COUNT_OFFSET, peerGroupCount);
        }

        public void incrementPeerGroupCount(long index)
        {
            buffer.increment(index * POSITIONS_PER_ENTRY + PEER_GROUP_COUNT_OFFSET);
        }

        public void addPeerGroupCount(long index, long delta)
        {
            buffer.add(index * POSITIONS_PER_ENTRY + PEER_GROUP_COUNT_OFFSET, delta);
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

    /**
     * Buffer abstracting storage of peer groups as linked chains of matching values. Peer groups are referenced by
     * their node index for operations.
     */
    private static final class PeerGroupBuffer
    {
        private static final long INSTANCE_SIZE = instanceSize(PeerGroupBuffer.class);
        private static final int POSITIONS_PER_ENTRY = 2;
        private static final int NEXT_PEER_INDEX_OFFSET = 1;

        /*
         *  Memory layout:
         *  [LONG] rowId1, [LONG] nextPeerIndex1,
         *  [LONG] rowId2, [LONG] nextPeerIndex2,
         *  ...
         */
        private final LongBigArray buffer = new LongBigArray();

        private final LongBigArrayFIFOQueue emptySlots = new LongBigArrayFIFOQueue();

        private long capacity;

        /**
         * Allocates storage for a new peer group node.
         *
         * @return index referencing the node
         */
        public long allocateNewNode(long rowId, long nextPeerIndex)
        {
            long newPeerIndex;
            if (!emptySlots.isEmpty()) {
                newPeerIndex = emptySlots.dequeueLong();
            }
            else {
                newPeerIndex = capacity;
                capacity++;
                buffer.ensureCapacity(capacity * POSITIONS_PER_ENTRY);
            }

            setRowId(newPeerIndex, rowId);
            setNextPeerIndex(newPeerIndex, nextPeerIndex);

            return newPeerIndex;
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

        public long getNextPeerIndex(long index)
        {
            return buffer.get(index * POSITIONS_PER_ENTRY + NEXT_PEER_INDEX_OFFSET);
        }

        public void setNextPeerIndex(long index, long nextPeerIndex)
        {
            buffer.set(index * POSITIONS_PER_ENTRY + NEXT_PEER_INDEX_OFFSET, nextPeerIndex);
        }

        public long sizeOf()
        {
            return INSTANCE_SIZE + buffer.sizeOf() + emptySlots.sizeOf();
        }
    }
}
