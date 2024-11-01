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
package io.trino.spi.connector;

import io.trino.spi.Page;
import io.trino.spi.block.Block;

import java.util.Objects;
import java.util.function.ObjLongConsumer;

final class PositionCountSourcePage
        implements SourcePage
{
    private int positionCount;

    PositionCountSourcePage(int positionCount)
    {
        if (positionCount < 0) {
            throw new IllegalArgumentException("positionCount is negative");
        }
        this.positionCount = positionCount;
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
    }

    @Override
    public long getSizeInBytes()
    {
        return 0;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return 0;
    }

    @Override
    public void retainedBytesForEachPart(ObjLongConsumer<Object> consumer) {}

    @Override
    public int getChannelCount()
    {
        return 0;
    }

    @Override
    public Block getBlock(int channel)
    {
        throw new IllegalArgumentException("Page has no channels");
    }

    @Override
    public Page getPage()
    {
        return new Page(positionCount);
    }

    @Override
    public void selectPositions(int[] positions, int offset, int size)
    {
        if (size > positionCount) {
            throw new IllegalArgumentException("Page has no channels");
        }

        for (int i = 0; i < size; i++) {
            Objects.checkIndex(offset + i, positionCount);
        }
        positionCount = size;
    }
}
