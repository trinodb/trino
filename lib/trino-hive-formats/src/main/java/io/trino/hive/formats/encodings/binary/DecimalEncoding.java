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
import io.airlift.slice.Slices;
import io.trino.hive.formats.encodings.ColumnData;
import io.trino.hive.formats.encodings.EncodeOutput;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.Int128Math;
import io.trino.spi.type.Type;

import java.math.BigInteger;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.hive.formats.ReadWriteUtils.decodeVIntSize;
import static io.trino.hive.formats.ReadWriteUtils.readVInt;
import static io.trino.hive.formats.ReadWriteUtils.writeVInt;
import static io.trino.spi.type.Decimals.rescale;
import static java.lang.Math.toIntExact;

public class DecimalEncoding
        implements BinaryColumnEncoding
{
    private static final int BITS_IN_BYTE = 8;
    private static final int BYTES_IN_LONG_DECIMAL = 16;

    private final DecimalType type;
    private final byte[] resultBytes = new byte[BYTES_IN_LONG_DECIMAL];
    private final Slice resultSlice = Slices.wrappedBuffer(resultBytes);

    public DecimalEncoding(Type type)
    {
        this.type = (DecimalType) type;
    }

    @Override
    public void encodeColumn(Block block, SliceOutput output, EncodeOutput encodeOutput)
    {
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (!block.isNull(position)) {
                encodeValueInto(block, position, output);
            }
            encodeOutput.closeEntry();
        }
    }

    @Override
    public void encodeValueInto(Block block, int position, SliceOutput output)
    {
        if (type.isShort()) {
            writeLong(output, type.getLong(block, position));
        }
        else {
            writeSlice(output, block, position);
        }
    }

    @Override
    public Block decodeColumn(ColumnData columnData)
    {
        int size = columnData.rowCount();
        BlockBuilder builder = type.createFixedSizeBlockBuilder(size);

        Slice slice = columnData.getSlice();
        for (int i = 0; i < size; i++) {
            int offset = columnData.getOffset(i);
            int length = columnData.getLength(i);
            if (length == 0) {
                builder.appendNull();
            }
            else if (type.isShort()) {
                type.writeLong(builder, parseLong(slice, offset));
            }
            else {
                type.writeObject(builder, parseSlice(slice, offset));
            }
        }
        return builder.build();
    }

    @Override
    public int getValueOffset(Slice slice, int offset)
    {
        return 0;
    }

    @Override
    public int getValueLength(Slice slice, int offset)
    {
        // first vint is scale
        int scaleLength = decodeVIntSize(slice, offset);

        // second vint is data length
        int dataLengthLength = decodeVIntSize(slice, offset + scaleLength);
        int dataLength = toIntExact(readVInt(slice, offset + scaleLength));

        return scaleLength + dataLengthLength + dataLength;
    }

    @Override
    public void decodeValueInto(BlockBuilder builder, Slice slice, int offset, int length)
    {
        if (type.isShort()) {
            type.writeLong(builder, parseLong(slice, offset));
        }
        else {
            type.writeObject(builder, parseSlice(slice, offset));
        }
    }

    private long parseLong(Slice slice, int offset)
    {
        // first vint is scale
        int scale = toIntExact(readVInt(slice, offset));
        offset += decodeVIntSize(slice, offset);

        // second vint is length
        int length = toIntExact(readVInt(slice, offset));
        offset += decodeVIntSize(slice, offset);

        checkState(length <= 8);

        // reset the results buffer
        if (slice.getByte(offset) >= 0) {
            resultSlice.setLong(0, 0L);
        }
        else {
            resultSlice.setLong(0, 0xFFFF_FFFF_FFFF_FFFFL);
        }

        resultSlice.setBytes(8 - length, slice, offset, length);

        long value = Long.reverseBytes(resultSlice.getLong(0));
        if (scale != type.getScale()) {
            return rescale(value, scale, type.getScale());
        }

        return value;
    }

    private Int128 parseSlice(Slice slice, int offset)
    {
        // first vint is scale
        int scale = toIntExact(readVInt(slice, offset));
        offset += decodeVIntSize(slice, offset);

        // second vint is length
        int length = toIntExact(readVInt(slice, offset));
        offset += decodeVIntSize(slice, offset);

        checkState(length <= BYTES_IN_LONG_DECIMAL);

        // reset the results buffer
        if (slice.getByte(offset) >= 0) {
            resultSlice.setLong(0, 0L);
            resultSlice.setLong(8, 0L);
        }
        else {
            resultSlice.setLong(0, 0xFFFF_FFFF_FFFF_FFFFL);
            resultSlice.setLong(8, 0xFFFF_FFFF_FFFF_FFFFL);
        }

        resultSlice.setBytes(BYTES_IN_LONG_DECIMAL - length, slice, offset, length);

        Int128 result = Int128.fromBigEndian(resultBytes);
        return Int128Math.rescale(result, type.getScale() - scale);
    }

    private void writeLong(SliceOutput output, long value)
    {
        // first vint is scale
        writeVInt(output, type.getScale());

        // second vint is length
        int length = getWriteByteCount(value);
        writeVInt(output, length);

        // write value (big endian)
        for (int i = length - 1; i >= 0; i--) {
            output.writeByte((int) (value >> (i * 8)));
        }
    }

    private static int getWriteByteCount(long value)
    {
        // if the value is negative flip the bits, so we can always count leading zero bytes
        if (value < 0) {
            value = ~value;
        }

        // count number of leading zero bytes
        return (Long.SIZE - Long.numberOfLeadingZeros(value)) / BITS_IN_BYTE + 1;
    }

    private void writeSlice(SliceOutput output, Block block, int position)
    {
        // first vint is scale
        writeVInt(output, type.getScale());

        // second vint is length
        // todo get rid of BigInteger
        BigInteger decimal = ((Int128) type.getObject(block, position)).toBigInteger();
        byte[] decimalBytes = decimal.toByteArray();
        writeVInt(output, decimalBytes.length);

        // write value (big endian)
        // NOTE: long decimals are stored in a slice in big endian encoding
        for (byte decimalByte : decimalBytes) {
            output.write(decimalByte);
        }
    }
}
