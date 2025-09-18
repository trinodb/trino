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

import io.trino.spi.block.Block;
import io.trino.spi.block.ColumnarMap;
import io.trino.spi.type.MapType;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;

import static io.trino.server.protocol.spooling.encoding.arrow.VectorWriters.writerForVector;
import static java.util.Objects.requireNonNull;

public final class MapWriter
        implements ArrowWriter
{
    private final MapVector vector;
    private final ArrowWriter keyWriter;
    private final ArrowWriter valueWriter;

    public MapWriter(MapVector vector, MapType type)
    {
        this.vector = requireNonNull(vector, "vector is null");
        if (vector.getDataVector() instanceof StructVector structVector) {
            this.keyWriter = writerForVector(structVector.getChild("key"), type.getKeyType());
            this.valueWriter = writerForVector(structVector.getChild("value"), type.getValueType());
        }
        else {
            throw new UnsupportedOperationException("Unsupported data vector: " + vector.getDataVector().getClass());
        }
    }

    @Override
    public void write(Block block)
    {
        ColumnarMap mapBlock = ColumnarMap.toColumnarMap(block);
        Block keyBlock = mapBlock.getKeysBlock();
        Block valueBlock = mapBlock.getValuesBlock();
        vector.setInitialTotalCapacity(mapBlock.getPositionCount(), mapBlock.getValuesBlock().getPositionCount());
        vector.allocateNew();
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (block.isNull(position)) {
                vector.setNull(position);
                continue;
            }

            vector.startNewValue(position);
            int entries = mapBlock.getEntryCount(position);
            vector.endValue(position, entries);
        }
        keyWriter.write(keyBlock);
        valueWriter.write(valueBlock);
        vector.setValueCount(block.getPositionCount());
    }
}
