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

import static io.trino.spi.block.BlockUtil.calculateBlockResetSize;

public interface BlockBuilder
        extends Block
{
    /**
     * Create a new block from the current materialized block by keeping the same elements
     * only with respect to {@code visiblePositions}.
     */
    @Override
    default Block getPositions(int[] visiblePositions, int offset, int length)
    {
        return build().getPositions(visiblePositions, offset, length);
    }

    /**
     * Appends a null value to the block.
     */
    BlockBuilder appendNull();

    /**
     * Builds the block. This method can be called multiple times.
     */
    Block build();

    /**
     * Creates a new block builder of the same type based on the current usage statistics of this block builder.
     */
    BlockBuilder newBlockBuilderLike(int expectedEntries, BlockBuilderStatus blockBuilderStatus);

    default BlockBuilder newBlockBuilderLike(BlockBuilderStatus blockBuilderStatus)
    {
        return newBlockBuilderLike(calculateBlockResetSize(getPositionCount()), blockBuilderStatus);
    }

    /**
     * This method is not expected to be implemented for {@code BlockBuilder} implementations, the method
     * {@link BlockBuilder#appendNull} should be used instead.
     */
    @Override
    default Block copyWithAppendedNull()
    {
        throw new UnsupportedOperationException("BlockBuilder implementation does not support newBlockWithAppendedNull");
    }
}
