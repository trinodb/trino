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
import io.trino.spi.block.BlockBuilder;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import static io.airlift.slice.SizeOf.SIZE_OF_LONG;

public class Int128PositionsAppender
        implements PositionsAppender
{
    @Override
    public void appendTo(IntArrayList positions, Block block, BlockBuilder blockBuilder)
    {
        int[] positionArray = positions.elements();
        if (block.mayHaveNull()) {
            for (int i = 0; i < positions.size(); i++) {
                int position = positionArray[i];
                if (block.isNull(position)) {
                    blockBuilder.appendNull();
                }
                else {
                    blockBuilder.writeLong(block.getLong(position, 0));
                    blockBuilder.writeLong(block.getLong(position, SIZE_OF_LONG));
                    blockBuilder.closeEntry();
                }
            }
        }
        else {
            for (int i = 0; i < positions.size(); i++) {
                int position = positionArray[i];
                blockBuilder.writeLong(block.getLong(position, 0));
                blockBuilder.writeLong(block.getLong(position, SIZE_OF_LONG));
                blockBuilder.closeEntry();
            }
        }
    }
}
