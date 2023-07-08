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

import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.trino.spi.type.MapType;

import java.util.Optional;

import static io.trino.spi.block.MapHashTables.HASH_MULTIPLIER;
import static java.lang.String.format;

public class SqlMapBlockEncoding
        implements BlockEncoding
{
    public static final String NAME = "MAP_ELEMENT";

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public void writeBlock(BlockEncodingSerde blockEncodingSerde, SliceOutput sliceOutput, Block block)
    {
        SqlMap sqlMap = (SqlMap) block;
        blockEncodingSerde.writeType(sliceOutput, sqlMap.getMapType());

        int offset = sqlMap.getRawOffset();
        int size = sqlMap.getSize();
        blockEncodingSerde.writeBlock(sliceOutput, sqlMap.getRawKeyBlock().getRegion(offset, size));
        blockEncodingSerde.writeBlock(sliceOutput, sqlMap.getRawValueBlock().getRegion(offset, size));

        Optional<int[]> hashTable = sqlMap.tryGetHashTable();
        if (hashTable.isPresent()) {
            int hashTableLength = size * HASH_MULTIPLIER;
            sliceOutput.appendInt(hashTableLength);  // hashtable length
            sliceOutput.writeInts(hashTable.get(), offset * HASH_MULTIPLIER, hashTableLength);
        }
        else {
            // if the hashTable is null, we write the length -1
            sliceOutput.appendInt(-1);
        }
    }

    @Override
    public Block readBlock(BlockEncodingSerde blockEncodingSerde, SliceInput sliceInput)
    {
        MapType mapType = (MapType) blockEncodingSerde.readType(sliceInput);

        Block keyBlock = blockEncodingSerde.readBlock(sliceInput);
        Block valueBlock = blockEncodingSerde.readBlock(sliceInput);

        int hashTableLength = sliceInput.readInt();
        int[] hashTable = null;
        if (hashTableLength >= 0) {
            hashTable = new int[hashTableLength];
            sliceInput.readInts(hashTable);
        }

        if (keyBlock.getPositionCount() != valueBlock.getPositionCount()) {
            throw new IllegalArgumentException(format("Deserialized SqlMap violates invariants: key %d, value %d",
                    keyBlock.getPositionCount(),
                    valueBlock.getPositionCount()));
        }

        if (hashTable != null && keyBlock.getPositionCount() * HASH_MULTIPLIER != hashTable.length) {
            throw new IllegalArgumentException(format("Deserialized SqlMap violates invariants: expected hashtable size %d, actual hashtable size %d",
                    keyBlock.getPositionCount() * HASH_MULTIPLIER,
                    hashTable.length));
        }

        return new SqlMap(
                mapType,
                keyBlock,
                valueBlock,
                new SqlMap.HashTableSupplier(hashTable),
                0,
                keyBlock.getPositionCount());
    }
}
