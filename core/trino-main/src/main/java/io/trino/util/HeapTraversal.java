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
package io.trino.util;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.instanceSize;

/**
 * If heap nodes are numbered top down, and left to right as follows:
 * <pre>
 *             Node1
 *          /        \
 *       Node2       Node3
 *      /    \       /
 *   Node4 Node5  Node6 ...
 * </pre>
 * We can compute the path from the root to each node by following the binary sequence of 0's and 1s of the index as
 * follows:
 * 1. Starting from the most significant 1 bit, shift to the next most significant bit. If this bit is 0, we need to move to
 * the root's left child, otherwise move to the right child.
 * 2. From the context of this new node, observe the next most significant bit. If this bit is 0, we need to move to this
 * node's left child, otherwise move to the right child.
 * 3. Repeat until all significant bits have been consumed.
 * <pre>
 * Examples:
 * - Node4 => 100 => 1 [ROOT] - 0 [GO LEFT] - 0 [GO LEFT]
 * - Node6 => 110 => 1 [ROOT] - 1 [GO RIGHT] - 0 [GO LEFT]
 * </pre>
 */
public class HeapTraversal
{
    public enum Child
    {
        LEFT,
        RIGHT
    }

    private static final long INSTANCE_SIZE = instanceSize(HeapTraversal.class);
    private static final long TOP_BIT_MASK = 1L << (Long.SIZE - 1);

    private long shifted;
    private int treeDepthToNode;

    public void resetWithPathTo(long targetNodeIndex)
    {
        checkArgument(targetNodeIndex >= 1, "Target node index must be greater than or equal to one");
        int leadingZeros = Long.numberOfLeadingZeros(targetNodeIndex);
        // Shift off the leading zeros PLUS the most significant one bit (which is not needed for this calculation)
        shifted = targetNodeIndex << (leadingZeros + 1);
        treeDepthToNode = Long.SIZE - (leadingZeros + 1);
    }

    public boolean isTarget()
    {
        return treeDepthToNode == 0;
    }

    public Child nextChild()
    {
        checkState(!isTarget(), "Already at target");
        Child childToFollow = (shifted & TOP_BIT_MASK) == 0 ? Child.LEFT : Child.RIGHT;
        shifted <<= 1;
        treeDepthToNode--;
        return childToFollow;
    }

    public long sizeOf()
    {
        return INSTANCE_SIZE;
    }
}
