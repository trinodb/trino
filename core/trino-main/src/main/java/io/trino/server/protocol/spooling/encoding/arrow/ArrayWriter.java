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
package io.trino.server.protocol.spooling.encoding.arrow;

import io.trino.spi.block.ArrayBlock;
import io.trino.spi.block.Block;
import io.trino.spi.type.ArrayType;
import org.apache.arrow.vector.complex.ListVector;

import static io.trino.server.protocol.spooling.encoding.arrow.VectorWriters.writerForVector;
import static java.util.Objects.requireNonNull;

public final class ArrayWriter
        implements ArrowWriter
{
    private final ListVector vector;
    private final ArrayType type;

    public ArrayWriter(ListVector vector, ArrayType type)
    {
        this.vector = requireNonNull(vector, "vector is null");
        this.type = requireNonNull(type, "type is null");
    }

    @Override
    public void write(Block block)
    {
        vector.setInitialCapacity(block.getPositionCount());
        vector.allocateNew();

        if (block instanceof ArrayBlock arrayBlock) {
            Block dataBlock = arrayBlock.getElementsBlock();
            ArrowWriter elementWriter = writerForVector(vector.getDataVector(), type.getElementType());
            for (int position = 0; position < block.getPositionCount(); position++) {
                if (block.isNull(position)) {
                    vector.setNull(position);
                    continue;
                }
                Block elementBlock = arrayBlock.getArray(position);
                vector.startNewValue(position);
                vector.endValue(position, elementBlock.getPositionCount());
            }
            elementWriter.write(dataBlock);
            vector.setValueCount(block.getPositionCount());
        }
        else {
            throw new UnsupportedOperationException("ArrayBlock is expected but got " + block.getClass().getSimpleName());
        }
    }
}
