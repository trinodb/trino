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
package io.trino.json;

import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.trino.spi.block.Bitmap;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockEncoding;
import io.trino.spi.block.BlockEncodingSerde;

/// Wire encoding for [JsonBlock]: position count, optional validity bitmap, end offsets,
/// payload length, and one contiguous payload buffer. Offsets are relative to the serialized
/// region. Typed encodings and raw JSON text are preserved without parsing or rendering.
public final class JsonBlockEncoding
        implements BlockEncoding
{
    public static final String NAME = "JSON";

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public Class<? extends Block> getBlockClass()
    {
        return JsonBlock.class;
    }

    @Override
    public void writeBlock(BlockEncodingSerde blockEncodingSerde, SliceOutput sliceOutput, Block block)
    {
        JsonBlock jsonBlock = (JsonBlock) block;
        int positionCount = jsonBlock.getPositionCount();
        sliceOutput.appendInt(positionCount);

        int rawOffset = jsonBlock.getArrayOffset();
        long[] validity = jsonBlock.getRawValueIsValid();
        sliceOutput.writeBoolean(validity != null);
        if (validity != null) {
            for (int position = 0; position < positionCount; position += Long.SIZE) {
                sliceOutput.writeLong(Bitmap.getBits(validity, rawOffset, position, Math.min(Long.SIZE, positionCount - position)));
            }
        }

        // The values are already contiguous bytes: write the offsets, then the buffer.
        int[] offsets = jsonBlock.getRawOffsets();
        int start = offsets[rawOffset];
        for (int i = 0; i < positionCount; i++) {
            sliceOutput.writeInt(offsets[rawOffset + i + 1] - start);
        }
        int end = offsets[rawOffset + positionCount];
        sliceOutput.writeInt(end - start);
        sliceOutput.writeBytes(jsonBlock.getRawSlice(), start, end - start);
    }

    @Override
    public JsonBlock readBlock(BlockEncodingSerde blockEncodingSerde, SliceInput sliceInput)
    {
        int positionCount = sliceInput.readInt();
        long[] valueIsValid = null;
        if (sliceInput.readBoolean()) {
            valueIsValid = Bitmap.allocateWords(positionCount, false);
            sliceInput.readLongs(valueIsValid);
        }
        int[] offsets = new int[positionCount + 1];
        for (int i = 0; i < positionCount; i++) {
            offsets[i + 1] = sliceInput.readInt();
        }
        int length = sliceInput.readInt();
        Slice slice = sliceInput.readSlice(length);
        return new JsonBlock(0, positionCount, valueIsValid, offsets, slice);
    }
}
