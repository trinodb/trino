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
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntArrays;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.trino.operator.output.PositionsAppenderUtil.calculateBlockResetSize;
import static io.trino.operator.output.PositionsAppenderUtil.calculateNewArraySize;
import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;

/**
 * Dispatches the {@link #append} and {@link #appendRle} methods to the {@link #delegate} depending on the input {@link Block} class.
 */
public class UnnestingPositionsAppender
        implements PositionsAppender
{
    private static final int INSTANCE_SIZE = instanceSize(UnnestingPositionsAppender.class);

    private final PositionsAppender delegate;
    private DictionaryBlockBuilder dictionaryBlockBuilder;

    public UnnestingPositionsAppender(PositionsAppender delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.dictionaryBlockBuilder = new DictionaryBlockBuilder();
    }

    @Override
    public void append(IntArrayList positions, Block source)
    {
        if (positions.isEmpty()) {
            return;
        }
        if (source instanceof RunLengthEncodedBlock) {
            dictionaryBlockBuilder.flushDictionary(delegate);
            delegate.appendRle(((RunLengthEncodedBlock) source).getValue(), positions.size());
        }
        else if (source instanceof DictionaryBlock) {
            appendDictionary(positions, (DictionaryBlock) source);
        }
        else {
            dictionaryBlockBuilder.flushDictionary(delegate);
            delegate.append(positions, source);
        }
    }

    @Override
    public void appendRle(Block block, int rlePositionCount)
    {
        if (rlePositionCount == 0) {
            return;
        }
        dictionaryBlockBuilder.flushDictionary(delegate);
        delegate.appendRle(block, rlePositionCount);
    }

    @Override
    public void append(int position, Block source)
    {
        dictionaryBlockBuilder.flushDictionary(delegate);

        if (source instanceof RunLengthEncodedBlock runLengthEncodedBlock) {
            delegate.append(0, runLengthEncodedBlock.getValue());
        }
        else if (source instanceof DictionaryBlock dictionaryBlock) {
            delegate.append(dictionaryBlock.getId(position), dictionaryBlock.getDictionary());
        }
        else {
            delegate.append(position, source);
        }
    }

    @Override
    public Block build()
    {
        Block result;
        if (dictionaryBlockBuilder.isEmpty()) {
            result = delegate.build();
        }
        else {
            result = dictionaryBlockBuilder.build();
        }
        dictionaryBlockBuilder = dictionaryBlockBuilder.newBuilderLike();
        return result;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + delegate.getRetainedSizeInBytes() + dictionaryBlockBuilder.getRetainedSizeInBytes();
    }

    @Override
    public long getSizeInBytes()
    {
        return delegate.getSizeInBytes();
    }

    private void appendDictionary(IntArrayList positions, DictionaryBlock source)
    {
        Block dictionary = source.getDictionary();
        if (dictionary instanceof RunLengthEncodedBlock rleDictionary) {
            appendRle(rleDictionary.getValue(), positions.size());
            return;
        }

        IntArrayList dictionaryPositions = getDictionaryPositions(positions, source);
        if (dictionaryBlockBuilder.canAppend(dictionary)) {
            dictionaryBlockBuilder.append(dictionaryPositions, dictionary);
        }
        else {
            dictionaryBlockBuilder.flushDictionary(delegate);
            delegate.append(dictionaryPositions, dictionary);
        }
    }

    private IntArrayList getDictionaryPositions(IntArrayList positions, DictionaryBlock block)
    {
        int[] positionArray = new int[positions.size()];
        for (int i = 0; i < positions.size(); i++) {
            positionArray[i] = block.getId(positions.getInt(i));
        }
        return IntArrayList.wrap(positionArray);
    }

    private static class DictionaryBlockBuilder
    {
        private static final int INSTANCE_SIZE = instanceSize(DictionaryBlockBuilder.class);
        private final int initialEntryCount;
        private Block dictionary;
        private int[] dictionaryIds;
        private int positionCount;
        private boolean closed;

        public DictionaryBlockBuilder()
        {
            this(1024);
        }

        public DictionaryBlockBuilder(int initialEntryCount)
        {
            this.initialEntryCount = initialEntryCount;
            this.dictionaryIds = new int[0];
        }

        public boolean isEmpty()
        {
            return positionCount == 0;
        }

        public Block build()
        {
            return DictionaryBlock.create(positionCount, dictionary, dictionaryIds);
        }

        public long getRetainedSizeInBytes()
        {
            return INSTANCE_SIZE
                    + (long) dictionaryIds.length * Integer.BYTES
                    + (dictionary != null ? dictionary.getRetainedSizeInBytes() : 0);
        }

        public boolean canAppend(Block dictionary)
        {
            return !closed && (dictionary == this.dictionary || this.dictionary == null);
        }

        public void append(IntArrayList mappedPositions, Block dictionary)
        {
            checkArgument(canAppend(dictionary));
            this.dictionary = dictionary;
            ensureCapacity(positionCount + mappedPositions.size());
            System.arraycopy(mappedPositions.elements(), 0, dictionaryIds, positionCount, mappedPositions.size());
            positionCount += mappedPositions.size();
        }

        public void flushDictionary(PositionsAppender delegate)
        {
            if (closed) {
                return;
            }
            if (positionCount > 0) {
                requireNonNull(dictionary, () -> "dictionary is null but we have pending dictionaryIds " + positionCount);
                delegate.append(IntArrayList.wrap(dictionaryIds, positionCount), dictionary);
            }

            closed = true;
            dictionaryIds = new int[0];
            positionCount = 0;
            dictionary = null;
        }

        public DictionaryBlockBuilder newBuilderLike()
        {
            return new DictionaryBlockBuilder(max(calculateBlockResetSize(positionCount), initialEntryCount));
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
            newSize = Math.max(newSize, capacity);

            dictionaryIds = IntArrays.ensureCapacity(dictionaryIds, newSize, positionCount);
        }
    }
}
