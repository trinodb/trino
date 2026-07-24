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
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockEncoding;
import io.trino.spi.block.BlockEncodingSerde;

/// Wire encoding for [JsonBlock]. Per-row layout on the wire is
/// `int32 byte-length | byte[byte-length]` of the canonical encoding. On read each
/// row's bytes wrap as an [io.trino.json.EncodedJson] referencing a fresh slice — the
/// downstream consumer sees byte-form `Json` regardless of what the writer originally
/// stored.
public final class JsonBlockEncoding
        implements BlockEncoding
{
    public static final String NAME = "JSON_REF";

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

        boolean[] isNull = jsonBlock.getRawValueIsNull();
        int rawOffset = jsonBlock.getArrayOffset();

        // Per-position null mask: 1 byte per row. Simpler than bit-packing and the
        // serialize-side overhead is negligible vs. the JSON payload bytes themselves.
        sliceOutput.writeBoolean(isNull != null);
        if (isNull != null) {
            for (int i = 0; i < positionCount; i++) {
                sliceOutput.writeByte(isNull[rawOffset + i] ? 1 : 0);
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
        boolean hasNulls = sliceInput.readBoolean();
        boolean[] valueIsNull = null;
        if (hasNulls) {
            valueIsNull = new boolean[positionCount];
            for (int i = 0; i < positionCount; i++) {
                valueIsNull[i] = sliceInput.readByte() != 0;
            }
        }
        int[] offsets = new int[positionCount + 1];
        for (int i = 0; i < positionCount; i++) {
            offsets[i + 1] = sliceInput.readInt();
        }
        int length = sliceInput.readInt();
        Slice slice = sliceInput.readSlice(length);
        return new JsonBlock(0, positionCount, valueIsNull, offsets, slice);
    }
}
