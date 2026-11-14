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
import io.trino.spi.predicate.Domain;

import java.util.Optional;
import java.util.function.ObjLongConsumer;

/**
 * A page of data from a connector.
 * <p>
 * A page has a fixed number of positions and a fixed set of channels.
 * <p>
 * This interface is not thread safe.
 */
public interface SourcePage
{
    /**
     * Creates a new SourcePage from the specified block.
     */
    static SourcePage create(int positionCount)
    {
        return new PositionCountSourcePage(positionCount);
    }

    /**
     * Creates a new SourcePage from the specified block.
     */
    static SourcePage create(Block block)
    {
        return new FixedSourcePage(new Page(block.getPositionCount(), block));
    }

    /**
     * Creates a new SourcePage from the specified page.
     */
    static SourcePage create(Page page)
    {
        return new FixedSourcePage(page);
    }

    /**
     * Gets the number of positions in the page.
     */
    int getPositionCount();

    /**
     * Gets the current loaded size of the page in bytes.
     */
    long getSizeInBytes();

    /**
     * Gets the current retained size of the page in bytes.
     */
    long getRetainedSizeInBytes();

    /**
     * Calls retainedBytesForEachPart on all loaded blocks;
     */
    void retainedBytesForEachPart(ObjLongConsumer<Object> consumer);

    /**
     * Gets the number of channels in the page.
     */
    int getChannelCount();

    /**
     * Gets the block for the specified channel.
     */
    Block getBlock(int channel);

    /**
     * Gets all data.
     */
    Page getPage();

    /**
     * Attempts to apply the specified position selection when doing so can avoid loading or
     * decoding data that has not been accessed yet. Returns whether the selection was applied.
     * If this method returns false, the logical contents and position count of the page must remain unchanged.
     * <p>
     * The positions array is owned by the caller and may only be accessed for the duration of
     * this call. An implementation must copy any positions it retains.
     */
    default boolean trySelectPositions(int[] positions, int offset, int size)
    {
        return false;
    }

    /**
     * Returns a domain that contains the value of the channel at every position of this page, when the source
     * knows one without reading the data, for example from the statistics of the row group the page comes from.
     * <p>
     * The engine may skip a filter on the channel when the bounds prove that every position passes, and may drop
     * the whole page when they prove that none does. The domain must therefore hold for every position, and it
     * must allow null unless the channel is known to hold no null. Bounds that are wider than the data are
     * always valid. A source should return the same instance for consecutive pages sharing the same bounds,
     * as consumers cache their decision by identity.
     */
    default Optional<Domain> getValueBounds(int channel)
    {
        return Optional.empty();
    }

    /**
     * Gets a projection of the page containing only the specified channels.
     */
    default Page getColumns(int[] channels)
    {
        Block[] blocks = new Block[channels.length];
        for (int i = 0; i < channels.length; i++) {
            blocks[i] = getBlock(channels[i]);
        }
        return new Page(getPositionCount(), blocks);
    }

    /**
     * Modify this page to mask data internally. After this method is called
     * this page and all returned blocks and pages will have the specified size.
     * <p>
     * This method should be preferred to {@link Block#getPositions(int[], int, int)}
     * and {@link Page#getPositions(int[], int, int)} where possible, as this allows
     * the underlying reader to filter positions on subsequent reads.
     */
    void selectPositions(int[] positions, int offset, int size);
}
