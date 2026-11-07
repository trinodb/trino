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
import io.trino.spi.block.ColumnarMap;
import io.trino.spi.block.MapBlock;

import java.util.Optional;

import static io.trino.spi.block.Bitmap.allocateWords;
import static io.trino.spi.block.Bitmap.set;

final class MapBlockProjection
{
    private MapBlockProjection() {}

    public static Block project(Block mapColumn, ColumnarMap map, Block elements)
    {
        int positionCount = map.getPositionCount();
        // The arrays of a map block that starts at its first entry have the offsets and nulls of the maps
        if (mapColumn instanceof MapBlock mapBlock && mapBlock.getOffsetBase() == 0 && mapBlock.getRawOffsets()[0] == 0) {
            return ArrayBlock.fromElementBlock(positionCount, Optional.ofNullable(mapBlock.getRawValueIsValid()), mapBlock.getRawOffsets(), elements);
        }

        // Null maps have no entries, so the offsets of the entries are the offsets of the arrays
        int[] offsets = new int[positionCount + 1];
        for (int position = 0; position <= positionCount; position++) {
            offsets[position] = map.getOffset(position);
        }
        long[] valueIsValid = null;
        if (map.mayHaveNull()) {
            valueIsValid = allocateWords(positionCount, false);
            for (int position = 0; position < positionCount; position++) {
                if (!map.isNull(position)) {
                    set(valueIsValid, 0, position);
                }
            }
        }
        return ArrayBlock.fromElementBlock(positionCount, Optional.ofNullable(valueIsValid), offsets, elements);
    }
}
