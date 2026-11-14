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
package io.trino.operator.scalar;

import io.trino.spi.block.ArrayBlock;
import io.trino.spi.block.Block;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.MapBlock;
import io.trino.spi.function.SelectedPositions;

import java.util.Optional;

import static io.trino.spi.block.Bitmap.allocateWords;
import static io.trino.spi.block.Bitmap.copyBits;
import static io.trino.spi.block.Bitmap.hasUnsetBit;
import static io.trino.spi.block.Bitmap.isSet;
import static io.trino.spi.block.Bitmap.set;

final class CardinalityBlockProjection
{
    private CardinalityBlockProjection() {}

    public static Block project(SelectedPositions selectedPositions, Block column)
    {
        // Read the arrays or maps under a dictionary or a run, so only the selected values are resolved
        Block values = column.getUnderlyingValueBlock();
        int[] offsets;
        long[] valueIsValid;
        int offsetBase;
        if (values instanceof ArrayBlock arrayBlock) {
            offsets = arrayBlock.getRawOffsets();
            valueIsValid = arrayBlock.getRawValueIsValid();
            offsetBase = arrayBlock.getOffsetBase();
        }
        else if (values instanceof MapBlock mapBlock) {
            offsets = mapBlock.getRawOffsets();
            valueIsValid = mapBlock.getRawValueIsValid();
            offsetBase = mapBlock.getOffsetBase();
        }
        else {
            throw new IllegalArgumentException("Unsupported block: " + values.getClass().getSimpleName());
        }

        int positionCount = selectedPositions.size();
        long[] cardinalities = new long[positionCount];
        long[] cardinalityIsValid = valueIsValid == null ? null : allocateWords(positionCount, false);
        if (values == column && !selectedPositions.isList()) {
            int start = offsetBase + selectedPositions.offset();
            for (int index = 0; index < positionCount; index++) {
                cardinalities[index] = offsets[start + index + 1] - offsets[start + index];
            }
            if (cardinalityIsValid != null) {
                copyBits(valueIsValid, start, cardinalityIsValid, 0, positionCount);
            }
        }
        else {
            for (int index = 0; index < positionCount; index++) {
                int position = offsetBase + column.getUnderlyingValuePosition(selectedPositions.position(index));
                cardinalities[index] = offsets[position + 1] - offsets[position];
                if (cardinalityIsValid != null && isSet(valueIsValid, 0, position)) {
                    set(cardinalityIsValid, 0, index);
                }
            }
        }

        if (cardinalityIsValid != null && !hasUnsetBit(cardinalityIsValid, 0, positionCount)) {
            cardinalityIsValid = null;
        }
        return new LongArrayBlock(positionCount, Optional.ofNullable(cardinalityIsValid), cardinalities);
    }
}
