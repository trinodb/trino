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

    /// Returns a validity bitmap for the current positions in the Block.
    ///
    /// `Optional.empty()` means all positions may be treated as valid without consulting a bitmap. Set bits mean valid,
    /// non-null positions, and unset bits mean null positions.
    ///
    /// Validity uses set bits for present values instead of set bits for null values so the bitmap matches the Java
    /// Vector API mask convention, where a set lane is selected for processing. The returned bitmap uses the raw
    /// [Bitmap] encoding and can be a view over block storage with a non-zero raw bit offset.
    Optional<Bitmap> getValidityBitmap();
}
