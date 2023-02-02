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
package io.trino.parquet.reader.decoders;

import io.trino.parquet.reader.SimpleSliceInputStream;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.parquet.ParquetReaderUtils.ceilDiv;
import static io.trino.parquet.ParquetReaderUtils.readUleb128Int;
import static io.trino.parquet.ParquetReaderUtils.readUleb128Long;
import static io.trino.parquet.ParquetReaderUtils.zigzagDecode;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

/**
 * Implementation of the encoding described in
 * <a href="https://github.com/apache/parquet-format/blob/master/Encodings.md#delta-encoding-delta_binary_packed--5">...</a>
 */
public final class DeltaBinaryPackedDecoders
{
    // Block size is a multiple of 128
    // Mini-block size is a multiple of 32
    // Mini-block count per block is typically equal to 4
    private static final int COMMON_MINI_BLOCKS_NUMBER = 4;
    private static final int FIRST_VALUE = -1;

    private DeltaBinaryPackedDecoders() {}

    public static class DeltaBinaryPackedByteDecoder
            extends DeltaBinaryPackedDecoder<byte[]>
    {
        @Override
        protected byte[] createMiniBlockBuffer(int size)
        {
            return new byte[size];
        }

        @Override
        protected void setValue(byte[] values, int offset, long value)
        {
            values[offset] = (byte) value;
        }

        @Override
        public void read(byte[] values, int offset, int length)
        {
            readInternal(values, offset, length);
        }

        @Override
        protected long unpack(byte[] output, int outputOffset, int length, SimpleSliceInputStream input, long minDelta, byte bitWidth)
        {
            DeltaPackingUtils.unpackDelta(output, outputOffset, length, input, minDelta, bitWidth);
            return output[outputOffset + length - 1];
        }
    }

    public static class DeltaBinaryPackedShortDecoder
            extends DeltaBinaryPackedDecoder<short[]>
    {
        @Override
        protected short[] createMiniBlockBuffer(int size)
        {
            return new short[size];
        }

        @Override
        protected void setValue(short[] values, int offset, long value)
        {
            values[offset] = (short) value;
        }

        @Override
        public void read(short[] values, int offset, int length)
        {
            readInternal(values, offset, length);
        }

        @Override
        protected long unpack(short[] output, int outputOffset, int length, SimpleSliceInputStream input, long minDelta, byte bitWidth)
        {
            DeltaPackingUtils.unpackDelta(output, outputOffset, length, input, minDelta, bitWidth);
            return output[outputOffset + length - 1];
        }
    }

    public static class DeltaBinaryPackedIntDecoder
            extends DeltaBinaryPackedDecoder<int[]>
    {
        @Override
        protected int[] createMiniBlockBuffer(int size)
        {
            return new int[size];
        }

        @Override
        protected void setValue(int[] values, int offset, long value)
        {
            values[offset] = (int) value;
        }

        @Override
        public void read(int[] values, int offset, int length)
        {
            readInternal(values, offset, length);
        }

        @Override
        protected long unpack(int[] output, int outputOffset, int length, SimpleSliceInputStream input, long minDelta, byte bitWidth)
        {
            DeltaPackingUtils.unpackDelta(output, outputOffset, length, input, minDelta, bitWidth);
            return output[outputOffset + length - 1];
        }
    }

    public static class DeltaBinaryPackedLongDecoder
            extends DeltaBinaryPackedDecoder<long[]>
    {
        @Override
        protected long[] createMiniBlockBuffer(int size)
        {
            return new long[size];
        }

        @Override
        protected void setValue(long[] values, int offset, long value)
        {
            values[offset] = value;
        }

        @Override
        public void read(long[] values, int offset, int length)
        {
            readInternal(values, offset, length);
        }

        @Override
        protected long unpack(long[] output, int outputOffset, int length, SimpleSliceInputStream input, long minDelta, byte bitWidth)
        {
            DeltaPackingUtils.unpackDelta(output, outputOffset, length, input, minDelta, bitWidth);
            return output[outputOffset + length - 1];
        }
    }

    private abstract static class DeltaBinaryPackedDecoder<ValuesType>
            implements ValueDecoder<ValuesType>
    {
        private SimpleSliceInputStream input;

        private int blockSize;
        private int miniBlockSize;
        // Last read value
        private long previousValue;

        private int miniBlocksInBlock;
        private byte[] bitWidths;

        private int alreadyReadInBlock;
        private ValuesType blockValues;
        private int valueCount;

        private long blockMinDelta;
        private int miniBlocksRemaining;

        @Override
        public void init(SimpleSliceInputStream input)
        {
            this.input = requireNonNull(input, "input is null");
            alreadyReadInBlock = FIRST_VALUE;
            readHeader();
            blockValues = createMiniBlockBuffer(blockSize + 1); // First index is reserved for the last read value
            bitWidths = new byte[miniBlocksInBlock];
        }

        protected abstract ValuesType createMiniBlockBuffer(int size);

        protected abstract void setValue(ValuesType values, int offset, long value);

        /**
         * This method needs to do two things:
         * <ul>
         * <li>Set output[outputOffset-1] to 'previousValue'. This way the inner loops are consistent and can handle iterations in batches of 32.
         * This is needed since some values might have been skipped</li>
         * <li>Delegate unpacking to the corresponding static method from DeltaPackingUtils class</li>
         * </ul>
         *
         * @return Last value read
         */
        protected abstract long unpack(ValuesType output, int outputOffset, int length, SimpleSliceInputStream input, long minDelta, byte bitWidth);

        private void readHeader()
        {
            blockSize = readUleb128Int(input);
            checkArgument(blockSize % 128 == 0, "Corrupted Parquet file: block size of the delta encoding needs to be a multiple of 128");
            miniBlockSize = blockSize / readUleb128Int(input);
            checkArgument(miniBlockSize % 32 == 0, "Corrupted Parquet file: mini block size of the delta encoding needs to be a multiple of 32");
            valueCount = readUleb128Int(input);
            miniBlocksRemaining = ceilDiv(valueCount - 1, miniBlockSize); // -1 as the first value is stored in a header
            previousValue = zigzagDecode(readUleb128Long(input));
            miniBlocksInBlock = blockSize / miniBlockSize;
        }

        @SuppressWarnings("SuspiciousSystemArraycopy")
        public void readInternal(ValuesType values, int offset, int length)
        {
            // This condition will be true only for the first time and then continue to
            // return false hopefully making branch prediction efficient
            if (alreadyReadInBlock == FIRST_VALUE && length > 0) {
                setValue(values, offset++, previousValue);
                length--;
                alreadyReadInBlock = 0;
            }

            if (alreadyReadInBlock != 0) { // Partially read block
                int chunkSize = min(length, blockSize - alreadyReadInBlock);
                // Leverage the fact that arrayCopy does not have array types specified
                System.arraycopy(blockValues, alreadyReadInBlock + 1, values, offset, chunkSize);
                markRead(chunkSize);

                offset += chunkSize;
                length -= chunkSize;
            }

            while (length > 0) {
                readBlockHeader();

                if (length <= blockSize) { // Read block partially
                    setValue(blockValues, 0, previousValue);
                    readBlock(blockValues, 1); // Write data to temporary buffer
                    System.arraycopy(blockValues, 1, values, offset, length);
                    markRead(length);
                    length = 0;
                }
                else { // read full block
                    if (offset == 0) {
                        // Special case: The decoder is in the middle of the page but the output offset is 0. This prevents
                        // us from leveraging output[offset-1] position to streamline the unpacking operation,
                        // The solution is to use the temporary buffer.
                        // This is a rare case that happens at most once every Trino page (~1MB or more)
                        setValue(blockValues, 0, previousValue);
                        readBlock(blockValues, 1); // Write data to temporary buffer
                        System.arraycopy(blockValues, 1, values, offset, blockSize);
                    }
                    else {
                        readBlock(values, offset); // Write data directly to output buffer
                    }
                    offset += blockSize;
                    length -= blockSize;
                }
            }
        }

        public int getValueCount()
        {
            return valueCount;
        }

        private boolean areBitWidthTheSame()
        {
            if (miniBlocksInBlock == COMMON_MINI_BLOCKS_NUMBER) {
                byte and = (byte) (bitWidths[0] & bitWidths[1] & bitWidths[2] & bitWidths[3]);
                byte or = (byte) (bitWidths[0] | bitWidths[1] | bitWidths[2] | bitWidths[3]);
                return and == or;
            }
            byte first = bitWidths[0];
            boolean same = true;
            for (int i = 1; i < miniBlocksInBlock; i++) {
                same &= bitWidths[i] == first;
            }
            return same;
        }

        @Override
        public void skip(int n)
        {
            if (n == 0) {
                return;
            }
            // This condition will be true only for the first time and then continue to
            // return false hopefully making branch prediction efficient
            if (alreadyReadInBlock == FIRST_VALUE) {
                n--;
                alreadyReadInBlock = 0;
            }

            if (alreadyReadInBlock != 0) { // Partially read mini block
                int chunkSize = min(n, blockSize - alreadyReadInBlock);
                markRead(chunkSize);
                n -= chunkSize;
            }

            while (n > 0) {
                readBlockHeader();
                setValue(blockValues, 0, previousValue);
                readBlock(blockValues, 1); // Write data to temporary buffer
                int chunkSize = min(n, blockSize);
                markRead(chunkSize);
                n -= chunkSize;
            }
        }

        /**
         * @param chunkSize Needs to be less or equal to the number of values remaining in the current mini block
         */
        private void markRead(int chunkSize)
        {
            alreadyReadInBlock += chunkSize;
            // Trick to skip conditional statement, does the same as:
            // if ( alreadyReadInMiniBlock == miniBlockSize) { currentMiniBlock++; }
            alreadyReadInBlock %= blockSize;
        }

        private void readBlock(ValuesType output, int outputOffset)
        {
            int miniBlocksToRead = Math.min(miniBlocksRemaining, miniBlocksInBlock);
            if (miniBlocksToRead == miniBlocksInBlock && areBitWidthTheSame()) {
                byte bitWidth = bitWidths[0];
                previousValue = unpack(output, outputOffset, blockSize, input, blockMinDelta, bitWidth);
            }
            else {
                for (int i = 0; i < miniBlocksToRead; i++) {
                    byte bitWidth = bitWidths[i];
                    previousValue = unpack(output, outputOffset, miniBlockSize, input, blockMinDelta, bitWidth);
                    outputOffset += miniBlockSize;
                }
            }

            miniBlocksRemaining -= miniBlocksToRead;
        }

        private void readBlockHeader()
        {
            blockMinDelta = zigzagDecode(readUleb128Long(input));
            if (miniBlocksInBlock == COMMON_MINI_BLOCKS_NUMBER) {
                int bitWidthsPacked = input.readInt();
                bitWidths[0] = (byte) bitWidthsPacked;
                bitWidths[1] = (byte) (bitWidthsPacked >> 8);
                bitWidths[2] = (byte) (bitWidthsPacked >> 16);
                bitWidths[3] = (byte) (bitWidthsPacked >> 24);
            }
            else {
                input.readBytes(bitWidths, 0, miniBlocksInBlock);
            }
        }
    }
}
