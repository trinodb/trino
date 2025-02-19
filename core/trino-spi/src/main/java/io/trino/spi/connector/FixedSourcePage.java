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

import java.util.function.ObjLongConsumer;

import static java.util.Objects.requireNonNull;

final class FixedSourcePage
        implements SourcePage
{
    private Page page;

    FixedSourcePage(Page page)
    {
        requireNonNull(page, "page is null");
        this.page = page;
    }

    @Override
    public int getPositionCount()
    {
        return page.getPositionCount();
    }

    @Override
    public long getSizeInBytes()
    {
        return page.getSizeInBytes();
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return page.getRetainedSizeInBytes();
    }

    @Override
    public void retainedBytesForEachPart(ObjLongConsumer<Object> consumer)
    {
        for (int i = 0; i < page.getChannelCount(); i++) {
            page.getBlock(i).retainedBytesForEachPart(consumer);
        }
    }

    @Override
    public int getChannelCount()
    {
        return page.getChannelCount();
    }

    @Override
    public Block getBlock(int channel)
    {
        return page.getBlock(channel);
    }

    @Override
    public Page getPage()
    {
        return page;
    }

    @Override
    public Page getColumns(int[] channels)
    {
        return page.getColumns(channels);
    }

    @Override
    public void selectPositions(int[] positions, int offset, int size)
    {
        page = page.getPositions(positions, offset, size);
    }
}
