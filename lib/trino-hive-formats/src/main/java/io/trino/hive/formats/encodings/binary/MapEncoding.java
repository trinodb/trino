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
package io.trino.hive.formats.encodings.binary;

import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.trino.hive.formats.ReadWriteUtils;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.type.Type;

import static java.lang.Math.toIntExact;

public class MapEncoding
        extends BlockEncoding
{
    private final BinaryColumnEncoding keyReader;
    private final BinaryColumnEncoding valueReader;

    public MapEncoding(Type type, BinaryColumnEncoding keyReader, BinaryColumnEncoding valueReader)
    {
        super(type);
        this.keyReader = keyReader;
        this.valueReader = valueReader;
    }

    @Override
    public void encodeValue(Block block, int position, SliceOutput output)
    {
        Block map = block.getObject(position, Block.class);

        // write entry count
        ReadWriteUtils.writeVInt(output, map.getPositionCount() / 2);

        // write null bits
        int nullByte = 0b0101_0101;
        int bits = 0;
        for (int elementIndex = 0; elementIndex < map.getPositionCount(); elementIndex += 2) {
            if (map.isNull(elementIndex)) {
                throw new TrinoException(StandardErrorCode.GENERIC_INTERNAL_ERROR, "Map must never contain null keys");
            }

            if (bits == 8) {
                output.writeByte(nullByte);
                nullByte = 0b0101_0101;
                bits = 0;
            }

            if (!map.isNull(elementIndex + 1)) {
                nullByte |= (1 << bits + 1);
            }
            bits += 2;
        }
        output.writeByte(nullByte);

        // write values
        for (int elementIndex = 0; elementIndex < map.getPositionCount(); elementIndex += 2) {
            if (map.isNull(elementIndex)) {
                // skip null keys
                continue;
            }

            keyReader.encodeValueInto(map, elementIndex, output);
            if (!map.isNull(elementIndex + 1)) {
                valueReader.encodeValueInto(map, elementIndex + 1, output);
            }
        }
    }

    @Override
    public void decodeValueInto(BlockBuilder builder, Slice slice, int offset, int length)
    {
        ((MapBlockBuilder) builder).buildEntry((keyBuilder, valueBuilder) -> decodeValueInto(keyBuilder, valueBuilder, slice, offset));
    }

    private void decodeValueInto(BlockBuilder keyBuilder, BlockBuilder valueBuilder, Slice slice, int offset)
    {
        // entries in list
        int entries = toIntExact(ReadWriteUtils.readVInt(slice, offset));
        offset += ReadWriteUtils.decodeVIntSize(slice.getByte(offset));

        // null bytes
        int nullByteCur = offset;
        int nullByteEnd = offset + (entries * 2 + 7) / 8;

        // read elements starting after null bytes
        int elementOffset = nullByteEnd;
        for (int i = 0; i < entries; i++) {
            // read key
            boolean nullKey;
            if ((slice.getByte(nullByteCur) & (1 << ((i * 2) % 8))) != 0) {
                int keyOffset = keyReader.getValueOffset(slice, elementOffset);
                int keyLength = keyReader.getValueLength(slice, elementOffset);

                keyReader.decodeValueInto(keyBuilder, slice, elementOffset + keyOffset, keyLength);
                nullKey = false;

                elementOffset = elementOffset + keyOffset + keyLength;
            }
            else {
                nullKey = true;
            }

            // ignore entries with a null key

            // read value
            if ((slice.getByte(nullByteCur) & (1 << ((i * 2 + 1) % 8))) != 0) {
                int valueOffset = valueReader.getValueOffset(slice, elementOffset);
                int valueLength = valueReader.getValueLength(slice, elementOffset);

                // ignore entries with a null key
                if (!nullKey) {
                    valueReader.decodeValueInto(valueBuilder, slice, elementOffset + valueOffset, valueLength);
                }

                elementOffset = elementOffset + valueOffset + valueLength;
            }
            else {
                // ignore entries with a null key
                if (!nullKey) {
                    valueBuilder.appendNull();
                }
            }

            // move onto the next null byte
            if (3 == (i % 4)) {
                nullByteCur++;
            }
        }
    }
}
