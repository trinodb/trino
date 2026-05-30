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

import io.trino.spi.type.MultisetType;

import java.lang.invoke.MethodHandle;
import java.util.function.ObjLongConsumer;

import static io.airlift.slice.SizeOf.instanceSize;
import static java.lang.String.format;
import static java.util.Objects.checkFromIndexSize;
import static java.util.Objects.requireNonNull;

/// The native value of a [MultisetType] (`getJavaType()`). It is a view over a contiguous
/// element range `[offset, offset + size)` of an element block, paired with a **lazily-built**
/// [MultisetHashTables] index of distinct values → multiplicity (IDENTICAL-keyed, null counted
/// separately). The index serves the per-value bag operations — membership, multiplicity, and
/// duplicate detection (MEMBER OF, SUBMULTISET, IS A SET) — in amortized O(1) per probe; it is
/// built only when an operation first needs it, so just constructing or iterating a multiset stays
/// index-free.
///
/// The index lives on this view, not on the underlying block: [MultisetType#getObject] creates a
/// fresh view per evaluation, so the build cost amortizes across probes against the *same value* —
/// a constant-folded `MULTISET[...]` literal shared by all rows, or an operation that probes one
/// multiset many times (SUBMULTISET, IS A SET). A block-level cache in the `MapBlock` style would
/// extend the payoff to per-row probes of a multiset column and is the natural follow-up.
///
/// Mirrors [SqlMap] for maps: the element storage is a plain element block (a multiset column is
/// an `ArrayBlock`), and this view carries the index, which is consulted via the element
/// `HASH_CODE` and `IDENTICAL` operators held on the [MultisetType].
public class SqlMultiset
{
    private static final int INSTANCE_SIZE = instanceSize(SqlMultiset.class);

    private final MultisetType multisetType;
    private final Block rawElementBlock;
    private final int offset;
    private final int size;
    // element HASH_CODE (FAIL_ON_NULL, BLOCK_POSITION_NOT_NULL) and IDENTICAL (FAIL_ON_NULL,
    // BLOCK_POSITION, BLOCK_POSITION) operators, held on the type and threaded in to build the index
    private final MethodHandle elementHashCode;
    private final MethodHandle elementIdentical;
    private final MethodHandle elementIndeterminate;

    // Benign race: MultisetHashTables is deeply immutable (all fields final, arrays fully populated
    // in the constructor, no `this` escape), so a racing reader sees either null (and rebuilds an
    // identical index) or a fully-frozen object. The field is volatile so a build is published to
    // the other threads sharing this value (for example through a constant-folded IR expression) —
    // without it another thread might rebuild on every probe. Keep the immutability invariant when
    // editing MultisetHashTables, or this publication breaks.
    private volatile MultisetHashTables index;

    public SqlMultiset(MultisetType multisetType, Block rawElementBlock, int offset, int size, MethodHandle elementHashCode, MethodHandle elementIdentical, MethodHandle elementIndeterminate)
    {
        this.multisetType = requireNonNull(multisetType, "multisetType is null");
        this.rawElementBlock = requireNonNull(rawElementBlock, "rawElementBlock is null");
        checkFromIndexSize(offset, size, rawElementBlock.getPositionCount());
        this.offset = offset;
        this.size = size;
        this.elementHashCode = requireNonNull(elementHashCode, "elementHashCode is null");
        this.elementIdentical = requireNonNull(elementIdentical, "elementIdentical is null");
        this.elementIndeterminate = requireNonNull(elementIndeterminate, "elementIndeterminate is null");
    }

    public MultisetType getMultisetType()
    {
        return multisetType;
    }

    /// The number of elements, counting duplicates (the cardinality of the multiset).
    public int getSize()
    {
        return size;
    }

    public int getRawOffset()
    {
        return offset;
    }

    public Block getRawElementBlock()
    {
        return rawElementBlock;
    }

    /// The elements of this multiset as a block of exactly [#getSize] positions — the
    /// `[offset, offset + size)` region of the raw element block. Use this instead of
    /// [#getRawElementBlock] unless offset arithmetic is wanted, so a view with a non-zero offset
    /// cannot be misread.
    public Block getElementBlock()
    {
        return rawElementBlock.getRegion(offset, size);
    }

    public boolean isElementNull(int position)
    {
        return rawElementBlock.isNull(offset + position);
    }

    public int getUnderlyingElementPosition(int position)
    {
        return rawElementBlock.getUnderlyingValuePosition(offset + position);
    }

    public ValueBlock getUnderlyingElementBlock()
    {
        return rawElementBlock.getUnderlyingValueBlock();
    }

    public long getRetainedSizeInBytes()
    {
        long retainedSize = INSTANCE_SIZE + rawElementBlock.getRetainedSizeInBytes();
        MultisetHashTables index = this.index;
        if (index != null) {
            retainedSize += index.getRetainedSizeInBytes();
        }
        return retainedSize;
    }

    public void retainedBytesForEachPart(ObjLongConsumer<Object> consumer)
    {
        consumer.accept(this, INSTANCE_SIZE);
        consumer.accept(rawElementBlock, rawElementBlock.getRetainedSizeInBytes());
        MultisetHashTables index = this.index;
        if (index != null) {
            consumer.accept(index, index.getRetainedSizeInBytes());
        }
    }

    /// The number of occurrences of `probeBlock[probePosition]` in this multiset (IDENTICAL).
    /// O(1) amortized.
    public int multiplicity(Block probeBlock, int probePosition)
    {
        return index().multiplicity(probeBlock, probePosition);
    }

    /// The position (within this multiset, usable with [#getUnderlyingElementPosition]) of the
    /// representative element of the probe's `IDENTICAL` class, or -1 when no element is identical
    /// to the probe. Nulls are counted, not keyed, so a null probe has no representative. O(1)
    /// amortized.
    public int representativePosition(Block probeBlock, int probePosition)
    {
        return index().representative(probeBlock, probePosition);
    }

    /// Whether this multiset is a set (no value occurs more than once, nulls included). O(1) amortized.
    public boolean isSet()
    {
        return index().isSet();
    }

    /// Whether the multiset contains a null element. O(1) amortized.
    public boolean hasNullElement()
    {
        return index().hasNull();
    }

    /// Whether any element is indeterminate (a nested null, so an element `=` comparison can be
    /// unknown). When false, every element `=` comparison against a determinate probe is definite,
    /// so the index can answer value-equality questions exactly. O(1) amortized.
    public boolean hasIndeterminateElement()
    {
        return index().hasIndeterminate();
    }

    private MultisetHashTables index()
    {
        MultisetHashTables index = this.index;
        if (index == null) {
            index = new MultisetHashTables(rawElementBlock, offset, size, elementHashCode, elementIdentical, elementIndeterminate);
            this.index = index;
        }
        return index;
    }

    @Override
    public String toString()
    {
        return format("SqlMultiset{size=%d}", size);
    }
}
