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

import static io.airlift.slice.SizeOf.instanceSize;
import static java.util.Objects.requireNonNull;

/**
 * Dispatches the {@link #append} and {@link #appendRle} methods to the {@link #delegate} depending on the input {@link Block} class.
 */
public class UnnestingPositionsAppender
        implements PositionsAppender
{
    private static final int INSTANCE_SIZE = instanceSize(UnnestingPositionsAppender.class);

    private final PositionsAppender delegate;

    public UnnestingPositionsAppender(PositionsAppender delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public void append(IntArrayList positions, Block source)
    {
        if (positions.isEmpty()) {
            return;
        }
        if (source instanceof RunLengthEncodedBlock) {
            delegate.appendRle(((RunLengthEncodedBlock) source).getValue(), positions.size());
        }
        else if (source instanceof DictionaryBlock) {
            appendDictionary(positions, (DictionaryBlock) source);
        }
        else {
            delegate.append(positions, source);
        }
    }

    @Override
    public void appendRle(Block block, int rlePositionCount)
    {
        if (rlePositionCount == 0) {
            return;
        }
        delegate.appendRle(block, rlePositionCount);
    }

    @Override
    public void append(int position, Block source)
    {
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
        return delegate.build();
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + delegate.getRetainedSizeInBytes();
    }

    @Override
    public long getSizeInBytes()
    {
        return delegate.getSizeInBytes();
    }

    private void appendDictionary(IntArrayList positions, DictionaryBlock source)
    {
        delegate.append(mapPositions(positions, source), source.getDictionary());
    }

    private IntArrayList mapPositions(IntArrayList positions, DictionaryBlock block)
    {
        int[] positionArray = new int[positions.size()];
        for (int i = 0; i < positions.size(); i++) {
            positionArray[i] = block.getId(positions.getInt(i));
        }
        return IntArrayList.wrap(positionArray);
    }
}
