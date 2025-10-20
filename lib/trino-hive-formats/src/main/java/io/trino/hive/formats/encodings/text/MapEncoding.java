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
package io.trino.hive.formats.encodings.text;

import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.trino.hive.formats.DistinctMapKeys;
import io.trino.hive.formats.FileCorruptionException;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.block.SqlMap;
import io.trino.spi.type.MapType;

public class MapEncoding
        extends BlockEncoding
{
    private final TextColumnEncoding keyEncoding;
    private final TextColumnEncoding valueEncoding;
    private final MapType mapType;
    private final byte elementSeparator;
    private final byte keyValueSeparator;

    private final DistinctMapKeys distinctMapKeys;
    private BlockBuilder keyBlockBuilder;

    public MapEncoding(
            MapType mapType,
            Slice nullSequence,
            byte elementSeparator,
            byte keyValueSeparator,
            Byte escapeByte,
            TextColumnEncoding keyEncoding,
            TextColumnEncoding valueEncoding)
    {
        super(mapType, nullSequence, escapeByte);
        this.mapType = mapType;
        this.elementSeparator = elementSeparator;
        this.keyValueSeparator = keyValueSeparator;
        this.keyEncoding = keyEncoding;
        this.valueEncoding = valueEncoding;
        this.distinctMapKeys = new DistinctMapKeys(mapType, false);
        this.keyBlockBuilder = mapType.getKeyType().createBlockBuilder(null, 128);
    }

    @Override
    public void encodeValueInto(Block block, int position, SliceOutput output)
            throws FileCorruptionException
    {
        SqlMap sqlMap = mapType.getObject(block, position);
        int rawOffset = sqlMap.getRawOffset();
        Block rawKeyBlock = sqlMap.getRawKeyBlock();
        Block rawValueBlock = sqlMap.getRawValueBlock();

        boolean first = true;
        for (int elementIndex = 0; elementIndex < sqlMap.getSize(); elementIndex++) {
            if (rawKeyBlock.isNull(rawOffset + elementIndex)) {
                throw new TrinoException(StandardErrorCode.GENERIC_INTERNAL_ERROR, "Map must never contain null keys");
            }

            if (!first) {
                output.writeByte(elementSeparator);
            }
            first = false;
            keyEncoding.encodeValueInto(rawKeyBlock, rawOffset + elementIndex, output);
            output.writeByte(keyValueSeparator);
            if (rawValueBlock.isNull(rawOffset + elementIndex)) {
                output.writeBytes(nullSequence);
            }
            else {
                valueEncoding.encodeValueInto(rawValueBlock, rawOffset + elementIndex, output);
            }
        }
    }

    @Override
    public void decodeValueInto(BlockBuilder builder, Slice slice, int offset, int length)
            throws FileCorruptionException
    {
        // read all the keys
        KeyOnlyEntryDecoder keyDecoder = new KeyOnlyEntryDecoder();
        processEntries(slice, offset, length, keyDecoder);
        Block keyBlock = keyDecoder.getKeyBlock();

        // determine which keys are distinct
        boolean[] distinctKeys = distinctMapKeys.selectDistinctKeys(keyBlock);

        // add the distinct entries to the map
        ((MapBlockBuilder) builder).buildEntry((keyBuilder, valueBuilder) -> {
            DistinctEntryDecoder entryDecoder = new DistinctEntryDecoder(distinctKeys, keyBlock, keyBuilder, valueBuilder);
            processEntries(slice, offset, length, entryDecoder);
        });
    }

    private void processEntries(Slice slice, int offset, int length, EntryDecoder entryDecoder)
            throws FileCorruptionException
    {
        int end = offset + length;

        if (length > 0) {
            int elementOffset = offset;
            int keyValueSeparatorPosition = -1;
            while (offset < end) {
                byte currentByte = slice.getByte(offset);
                if (currentByte == elementSeparator) {
                    entryDecoder.decodeEntry(slice, elementOffset, offset, keyValueSeparatorPosition);
                    elementOffset = offset + 1;
                    keyValueSeparatorPosition = -1;
                }
                else if (currentByte == keyValueSeparator && keyValueSeparatorPosition == -1) {
                    keyValueSeparatorPosition = offset;
                }
                else if (isEscapeByte(currentByte) && offset + 1 < length) {
                    // ignore the char after escape_char
                    offset++;
                }
                offset++;
            }
            entryDecoder.decodeEntry(slice, elementOffset, offset, keyValueSeparatorPosition);
        }
    }

    private interface EntryDecoder
    {
        default void decodeEntry(Slice slice, int entryOffset, int entryEnd, int keyValueSeparatorPosition)
                throws FileCorruptionException
        {
            boolean hasValue = keyValueSeparatorPosition >= 0;
            int keyLength;
            int valueOffset;
            int valueLength;
            if (hasValue) {
                keyLength = keyValueSeparatorPosition - entryOffset;
                valueOffset = keyValueSeparatorPosition + 1;
                valueLength = entryEnd - valueOffset;
            }
            else {
                keyLength = entryEnd - entryOffset;
                // there is no value
                valueOffset = entryOffset;
                valueLength = 0;
            }
            decodeKeyValue(0, slice, entryOffset, keyLength, hasValue, valueOffset, valueLength);
        }

        void decodeKeyValue(int depth, Slice slice, int keyOffset, int keyLength, boolean hasValue, int valueOffset, int valueLength)
                throws FileCorruptionException;
    }

    private class KeyOnlyEntryDecoder
            implements EntryDecoder
    {
        public Block getKeyBlock()
        {
            Block keyBlock = keyBlockBuilder.build();
            keyBlockBuilder = mapType.getKeyType().createBlockBuilder(null, keyBlock.getPositionCount());
            return keyBlock;
        }

        @Override
        public void decodeKeyValue(int depth, Slice slice, int keyOffset, int keyLength, boolean hasValue, int valueOffset, int valueLength)
                throws FileCorruptionException
        {
            if (isNullSequence(slice, keyOffset, keyLength)) {
                keyBlockBuilder.appendNull();
            }
            else {
                keyEncoding.decodeValueInto(keyBlockBuilder, slice, keyOffset, keyLength);
            }
        }
    }

    private class DistinctEntryDecoder
            implements EntryDecoder
    {
        private final boolean[] distinctKeys;
        private final Block keyBlock;
        private final BlockBuilder keyBuilder;
        private final BlockBuilder valueBuilder;
        private int entryPosition;

        public DistinctEntryDecoder(boolean[] distinctKeys, Block keyBlock, BlockBuilder keyBuilder, BlockBuilder valueBuilder)
        {
            this.distinctKeys = distinctKeys;
            this.keyBlock = keyBlock;
            this.keyBuilder = keyBuilder;
            this.valueBuilder = valueBuilder;
        }

        @Override
        public void decodeKeyValue(int depth, Slice slice, int keyOffset, int keyLength, boolean hasValue, int valueOffset, int valueLength)
                throws FileCorruptionException
        {
            if (distinctKeys[entryPosition]) {
                keyBuilder.append(keyBlock.getUnderlyingValueBlock(), keyBlock.getUnderlyingValuePosition(entryPosition));

                if (hasValue && !isNullSequence(slice, valueOffset, valueLength)) {
                    valueEncoding.decodeValueInto(valueBuilder, slice, valueOffset, valueLength);
                }
                else {
                    valueBuilder.appendNull();
                }
            }
            entryPosition++;
        }
    }
}
