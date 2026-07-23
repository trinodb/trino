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
package io.trino.spi.block;

import java.lang.invoke.MethodHandle;

import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;

/// A counting hash index over the distinct elements of a single multiset value.
///
/// Elements are keyed by the element type's `IDENTICAL` operator (null is not distinct from
/// null), so the index answers the per-value bag operations — membership, multiplicity, and
/// duplicate detection (MEMBER OF, SUBMULTISET, IS A SET) — in amortized O(1) per probe rather than
/// the O(n) scan the bare element block forces. It is built lazily (only when an operation needs it)
/// and cached on the owning [SqlMultiset] view, so construction of a multiset stays index-free and
/// the build cost amortizes across probes against the same value (see the caching discussion on
/// [SqlMultiset]).
///
/// Layout: open-addressing table over the element range `[offset, offset + count)` of an
/// element block. `buckets[b]` holds the within-range index of the first (representative)
/// element of a distinct value, or `-1`; `multiplicities[b]` is the number of occurrences
/// of that value. Nulls cannot be keyed (the element hash/identical operators fail on null),
/// so they are counted separately in `nullCount`.
///
/// Instances are deeply immutable — all fields final, the arrays fully populated in the
/// constructor, no `this` escape — which [SqlMultiset] relies on to publish the index across
/// threads with a benign race. Keep it that way.
final class MultisetHashTables
{
    private static final int INSTANCE_SIZE = instanceSize(MultisetHashTables.class);

    // inverse of the hash fill ratio must be integer
    static final int HASH_MULTIPLIER = 2;

    private final Block elementBlock;
    private final int offset;
    private final MethodHandle elementHashCode;
    private final MethodHandle elementIdentical;

    private final int[] buckets;
    private final int[] multiplicities;
    private final int nullCount;
    private final boolean hasIndeterminate;

    /// Builds the index over `elementBlock[offset, offset + count)`.
    ///
    /// @param elementHashCode element `HASH_CODE` operator, convention `(FAIL_ON_NULL, BLOCK_POSITION_NOT_NULL)`
    /// @param elementIdentical element `IDENTICAL` operator, convention `(FAIL_ON_NULL, BLOCK_POSITION, BLOCK_POSITION)`
    /// @param elementIndeterminate element `INDETERMINATE` operator, convention `(FAIL_ON_NULL, BLOCK_POSITION_NOT_NULL)`
    MultisetHashTables(Block elementBlock, int offset, int count, MethodHandle elementHashCode, MethodHandle elementIdentical, MethodHandle elementIndeterminate)
    {
        this.elementBlock = elementBlock;
        this.offset = offset;
        this.elementHashCode = elementHashCode;
        this.elementIdentical = elementIdentical;

        int size = count * HASH_MULTIPLIER;
        int[] buckets = new int[size];
        int[] multiplicities = new int[size];
        for (int i = 0; i < size; i++) {
            buckets[i] = -1;
        }

        int nullCount = 0;
        boolean hasIndeterminate = false;
        for (int i = 0; i < count; i++) {
            int position = offset + i;
            if (elementBlock.isNull(position)) {
                nullCount++;
                continue;
            }
            if (!hasIndeterminate && indeterminate(elementIndeterminate, elementBlock, position)) {
                hasIndeterminate = true;
            }
            int bucket = bucket(elementBlock, position, size);
            while (true) {
                if (buckets[bucket] == -1) {
                    buckets[bucket] = i;
                    multiplicities[bucket] = 1;
                    break;
                }
                if (identical(elementBlock, offset + buckets[bucket], elementBlock, position)) {
                    multiplicities[bucket]++;
                    break;
                }
                bucket++;
                if (bucket == size) {
                    bucket = 0;
                }
            }
        }
        this.buckets = buckets;
        this.multiplicities = multiplicities;
        this.nullCount = nullCount;
        this.hasIndeterminate = hasIndeterminate;
    }

    boolean hasNull()
    {
        return nullCount > 0;
    }

    boolean hasIndeterminate()
    {
        return hasIndeterminate;
    }

    private static boolean indeterminate(MethodHandle elementIndeterminate, Block block, int position)
    {
        try {
            return (boolean) elementIndeterminate.invokeExact(block, position);
        }
        catch (Throwable throwable) {
            throw handle(throwable);
        }
    }

    long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + sizeOf(buckets) + sizeOf(multiplicities);
    }

    /// The number of occurrences of the value at `probeBlock[probePosition]` in this multiset,
    /// pairing with the element `IDENTICAL` operator (null not distinct from null).
    int multiplicity(Block probeBlock, int probePosition)
    {
        if (probeBlock.isNull(probePosition)) {
            return nullCount;
        }
        int bucket = findBucket(probeBlock, probePosition);
        return bucket == -1 ? 0 : multiplicities[bucket];
    }

    /// The within-range index of the representative element of the probe's distinct value, or -1
    /// when the value does not occur. Nulls are counted, not keyed, so a null probe has no
    /// representative.
    int representative(Block probeBlock, int probePosition)
    {
        if (probeBlock.isNull(probePosition)) {
            return -1;
        }
        int bucket = findBucket(probeBlock, probePosition);
        return bucket == -1 ? -1 : buckets[bucket];
    }

    /// The bucket holding the probe's distinct value, or -1 when the value does not occur. The
    /// probe must not be null.
    private int findBucket(Block probeBlock, int probePosition)
    {
        int size = buckets.length;
        if (size == 0) {
            return -1;
        }
        int bucket = bucket(probeBlock, probePosition, size);
        while (true) {
            int representative = buckets[bucket];
            if (representative == -1) {
                return -1;
            }
            if (identical(elementBlock, offset + representative, probeBlock, probePosition)) {
                return bucket;
            }
            bucket++;
            if (bucket == size) {
                bucket = 0;
            }
        }
    }

    /// Whether the multiset is a set: no value occurs more than once (null counted as one value, so
    /// two nulls are duplicates). Equivalent to `IS A SET`.
    boolean isSet()
    {
        if (nullCount > 1) {
            return false;
        }
        for (int bucket = 0; bucket < buckets.length; bucket++) {
            if (buckets[bucket] != -1 && multiplicities[bucket] > 1) {
                return false;
            }
        }
        return true;
    }

    private int bucket(Block block, int position, int size)
    {
        long hashCode;
        try {
            hashCode = (long) elementHashCode.invokeExact(block, position);
        }
        catch (Throwable throwable) {
            throw handle(throwable);
        }
        return MapHashTables.computePosition(hashCode, size);
    }

    private boolean identical(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        try {
            return (boolean) elementIdentical.invokeExact(leftBlock, leftPosition, rightBlock, rightPosition);
        }
        catch (Throwable throwable) {
            throw handle(throwable);
        }
    }

    private static RuntimeException handle(Throwable throwable)
    {
        if (throwable instanceof Error error) {
            throw error;
        }
        if (throwable instanceof RuntimeException runtimeException) {
            return runtimeException;
        }
        return new RuntimeException(throwable);
    }
}
