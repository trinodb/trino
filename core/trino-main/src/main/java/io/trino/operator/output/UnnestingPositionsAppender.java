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
package io.trino.operator.output;

import io.trino.spi.block.Block;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.block.ValueBlock;
import io.trino.type.BlockTypeOperators.BlockPositionIsDistinctFrom;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntArrays;
import jakarta.annotation.Nullable;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.operator.output.PositionsAppenderUtil.calculateBlockResetSize;
import static io.trino.operator.output.PositionsAppenderUtil.calculateNewArraySize;
import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;

/**
 * Dispatches the {@link #append} and {@link #appendRle} methods to the {@link #delegate} depending on the input {@link Block} class.
 */
public class UnnestingPositionsAppender
{
    private static final int INSTANCE_SIZE = instanceSize(UnnestingPositionsAppender.class);

    // The initial state will transition to either the DICTIONARY or RLE state, and from there to the DIRECT state if necessary.
    private enum State
    {
        UNINITIALIZED, DICTIONARY, RLE, DIRECT
    }

    private final PositionsAppender delegate;
    @Nullable
    private final BlockPositionIsDistinctFrom isDistinctFromOperator;

    private State state = State.UNINITIALIZED;

    private ValueBlock dictionary;
    private DictionaryIdsBuilder dictionaryIdsBuilder;

    @Nullable
    private ValueBlock rleValue;
    private int rlePositionCount;

    public UnnestingPositionsAppender(PositionsAppender delegate, Optional<BlockPositionIsDistinctFrom> isDistinctFromOperator)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.dictionaryIdsBuilder = new DictionaryIdsBuilder(1024);
        this.isDistinctFromOperator = isDistinctFromOperator.orElse(null);
    }

    public void append(IntArrayList positions, Block source)
    {
        if (positions.isEmpty()) {
            return;
        }

        if (source instanceof RunLengthEncodedBlock rleBlock) {
            appendRle(rleBlock.getValue(), positions.size());
        }
        else if (source instanceof DictionaryBlock dictionaryBlock) {
            ValueBlock dictionary = dictionaryBlock.getDictionary();
            if (state == State.UNINITIALIZED) {
                state = State.DICTIONARY;
                this.dictionary = dictionary;
                dictionaryIdsBuilder.appendPositions(positions, dictionaryBlock);
            }
            else if (state == State.DICTIONARY && this.dictionary == dictionary) {
                dictionaryIdsBuilder.appendPositions(positions, dictionaryBlock);
            }
            else {
                transitionToDirect();

                int[] positionArray = new int[positions.size()];
                for (int i = 0; i < positions.size(); i++) {
                    positionArray[i] = dictionaryBlock.getId(positions.getInt(i));
                }
                delegate.append(IntArrayList.wrap(positionArray), dictionary);
            }
        }
        else if (source instanceof ValueBlock valueBlock) {
            transitionToDirect();
            delegate.append(positions, valueBlock);
        }
        else {
            throw new IllegalArgumentException("Unsupported block type: " + source.getClass().getSimpleName());
        }
    }

    public void appendRle(ValueBlock value, int positionCount)
    {
        if (positionCount == 0) {
            return;
        }

        if (state == State.DICTIONARY) {
            transitionToDirect();
        }
        if (isDistinctFromOperator == null) {
            transitionToDirect();
        }

        if (state == State.UNINITIALIZED) {
            state = State.RLE;
            rleValue = value;
            rlePositionCount = positionCount;
            return;
        }
        if (state == State.RLE) {
            if (!isDistinctFromOperator.isDistinctFrom(rleValue, 0, value, 0)) {
                // the values match. we can just add positions.
                rlePositionCount += positionCount;
                return;
            }
            transitionToDirect();
        }

        verify(state == State.DIRECT);
        delegate.appendRle(value, positionCount);
    }

    public void append(int position, Block source)
    {
        if (state != State.DIRECT) {
            transitionToDirect();
        }

        if (source instanceof RunLengthEncodedBlock runLengthEncodedBlock) {
            delegate.append(0, runLengthEncodedBlock.getValue());
        }
        else if (source instanceof DictionaryBlock dictionaryBlock) {
            delegate.append(dictionaryBlock.getId(position), dictionaryBlock.getDictionary());
        }
        else if (source instanceof ValueBlock valueBlock) {
            delegate.append(position, valueBlock);
        }
        else {
            throw new IllegalArgumentException("Unsupported block type: " + source.getClass().getSimpleName());
        }
    }

    private void transitionToDirect()
    {
        if (state == State.DICTIONARY) {
            int[] dictionaryIds = dictionaryIdsBuilder.getDictionaryIds();
            delegate.append(IntArrayList.wrap(dictionaryIds, dictionaryIdsBuilder.size()), dictionary);
            dictionary = null;
            dictionaryIdsBuilder = dictionaryIdsBuilder.newBuilderLike();
        }
        else if (state == State.RLE) {
            delegate.appendRle(rleValue, rlePositionCount);
            rleValue = null;
            rlePositionCount = 0;
        }
        state = State.DIRECT;
    }

    public Block build()
    {
        Block result = switch (state) {
            case DICTIONARY -> DictionaryBlock.create(dictionaryIdsBuilder.size(), dictionary, dictionaryIdsBuilder.getDictionaryIds());
            case RLE -> RunLengthEncodedBlock.create(rleValue, rlePositionCount);
            case UNINITIALIZED, DIRECT -> delegate.build();
        };

        reset();

        return result;
    }

    public void reset()
    {
        state = State.UNINITIALIZED;
        dictionary = null;
        dictionaryIdsBuilder = dictionaryIdsBuilder.newBuilderLike();
        rleValue = null;
        rlePositionCount = 0;
        delegate.reset();
    }

    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE +
                delegate.getRetainedSizeInBytes() +
                dictionaryIdsBuilder.getRetainedSizeInBytes() +
                (rleValue != null ? rleValue.getRetainedSizeInBytes() : 0);
    }

    public long getSizeInBytes()
    {
        return delegate.getSizeInBytes() +
                // dictionary size is not included due to the expense of the calculation
                (rleValue != null ? rleValue.getSizeInBytes() : 0);
    }

    private static class DictionaryIdsBuilder
    {
        private static final int INSTANCE_SIZE = instanceSize(DictionaryIdsBuilder.class);

        private final int initialEntryCount;
        private int[] dictionaryIds;
        private int size;

        public DictionaryIdsBuilder(int initialEntryCount)
        {
            this.initialEntryCount = initialEntryCount;
            this.dictionaryIds = new int[0];
        }

        public int[] getDictionaryIds()
        {
            return dictionaryIds;
        }

        public int size()
        {
            return size;
        }

        public long getRetainedSizeInBytes()
        {
            return INSTANCE_SIZE + sizeOf(dictionaryIds);
        }

        public void appendPositions(IntArrayList positions, DictionaryBlock block)
        {
            checkArgument(!positions.isEmpty(), "positions is empty");
            ensureCapacity(size + positions.size());

            for (int i = 0; i < positions.size(); i++) {
                dictionaryIds[size + i] = block.getId(positions.getInt(i));
            }
            size += positions.size();
        }

        public DictionaryIdsBuilder newBuilderLike()
        {
            if (size == 0) {
                return this;
            }
            return new DictionaryIdsBuilder(max(calculateBlockResetSize(size), initialEntryCount));
        }

        private void ensureCapacity(int capacity)
        {
            if (dictionaryIds.length >= capacity) {
                return;
            }

            int newSize;
            if (dictionaryIds.length > 0) {
                newSize = calculateNewArraySize(dictionaryIds.length);
            }
            else {
                newSize = initialEntryCount;
            }
            newSize = max(newSize, capacity);

            dictionaryIds = IntArrays.ensureCapacity(dictionaryIds, newSize, size);
        }
    }
}
