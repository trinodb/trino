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

import java.util.Optional;

import static io.trino.spi.block.Bitmap.allocateWords;
import static io.trino.spi.block.Bitmap.set;

final class MapBlockProjection
{
    private MapBlockProjection() {}

    public static Block project(ColumnarMap map, Block elements)
    {
        int positionCount = map.getPositionCount();
        int[] offsets = new int[positionCount + 1];
        long[] valueIsValid = map.mayHaveNull() ? allocateWords(positionCount, false) : null;
        for (int position = 0; position < positionCount; position++) {
            if (!map.isNull(position)) {
                if (valueIsValid != null) {
                    set(valueIsValid, 0, position);
                }
                offsets[position + 1] = offsets[position] + map.getEntryCount(position);
            }
            else {
                offsets[position + 1] = offsets[position];
            }
        }
        return ArrayBlock.fromElementBlock(positionCount, Optional.ofNullable(valueIsValid), offsets, elements);
    }
}
