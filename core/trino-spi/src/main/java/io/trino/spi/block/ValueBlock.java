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

import io.trino.spi.Experimental;

import java.util.Optional;

public non-sealed interface ValueBlock
        extends Block
{
    @Override
    ValueBlock copyPositions(int[] positions, int offset, int length);

    @Override
    ValueBlock getRegion(int positionOffset, int length);

    @Override
    ValueBlock copyRegion(int position, int length);

    @Override
    ValueBlock copyWithAppendedNull();

    @Override
    default ValueBlock getUnderlyingValueBlock()
    {
        return this;
    }

    @Override
    default int getUnderlyingValuePosition(int position)
    {
        return position;
    }

    /**
     * Returns a ByteArrayBlock specifying whether the current positions in the Block contain a NULL.
     * Returns Optional.empty() when there are no NULLs in the Block.
     * The returned ByteArrayBlock must not contain NULL values.
     */
    @Experimental(eta = "2025-01-01")
    Optional<ByteArrayBlock> getNulls();
}
