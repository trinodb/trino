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
import io.airlift.slice.Slices;
import io.trino.hive.formats.encodings.ColumnData;
import io.trino.hive.formats.encodings.EncodeOutput;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;
import org.gaul.modernizer_maven_annotations.SuppressModernizer;

import java.util.Base64;

import static org.apache.commons.codec.binary.Base64.decodeBase64;
import static org.apache.commons.codec.binary.Base64.isBase64;

public class BinaryEncoding
        implements TextColumnEncoding
{
    private static final Base64.Encoder base64Encoder = Base64.getEncoder();

    private final Type type;
    private final Slice nullSequence;

    public BinaryEncoding(Type type, Slice nullSequence)
    {
        this.type = type;
        this.nullSequence = nullSequence;
    }

    @Override
    public void encodeColumn(Block block, SliceOutput output, EncodeOutput encodeOutput)
    {
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (block.isNull(position)) {
                output.writeBytes(nullSequence);
            }
            else {
                Slice slice = type.getSlice(block, position);
                byte[] data = slice.getBytes();
                slice = Slices.wrappedBuffer(base64Encoder.encode(data));
                output.writeBytes(slice);
            }
            encodeOutput.closeEntry();
        }
    }

    @Override
    public void encodeValueInto(Block block, int position, SliceOutput output)
    {
        Slice slice = type.getSlice(block, position);
        byte[] data = slice.getBytes();
        slice = Slices.wrappedBuffer(base64Encoder.encode(data));
        output.writeBytes(slice);
    }

    @Override
    @SuppressModernizer
    public Block decodeColumn(ColumnData columnData)
    {
        int size = columnData.rowCount();
        BlockBuilder builder = type.createBlockBuilder(null, size);

        Slice slice = columnData.getSlice();
        for (int i = 0; i < size; i++) {
            int offset = columnData.getOffset(i);
            int length = columnData.getLength(i);
            if (nullSequence.equals(0, nullSequence.length(), slice, offset, length)) {
                builder.appendNull();
            }
            else {
                decodeValue(builder, slice, offset, length);
            }
        }
        return builder.build();
    }

    @Override
    public void decodeValueInto(BlockBuilder builder, Slice slice, int offset, int length)
    {
        decodeValue(builder, slice, offset, length);
    }

    // This code must use the Apache commons base64 decoder which supports both standard and URL safe alphabets
    @SuppressModernizer
    private void decodeValue(BlockBuilder builder, Slice slice, int offset, int length)
    {
        byte[] data = slice.getBytes(offset, length);
        if (isBase64(data)) {
            type.writeSlice(builder, Slices.wrappedBuffer(decodeBase64(data)));
        }
        else {
            type.writeSlice(builder, slice, offset, length);
        }
    }
}
