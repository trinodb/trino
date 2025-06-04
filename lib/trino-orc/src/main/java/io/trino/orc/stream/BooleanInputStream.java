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
package io.trino.orc.stream;

import io.trino.orc.checkpoint.BooleanStreamCheckpoint;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkState;

public class BooleanInputStream
        implements ValueInputStream<BooleanStreamCheckpoint>
{
    private static final int HIGH_BIT_MASK = 0b1000_0000;
    private final ByteInputStream byteStream;
    private byte data;
    private int bitsInData;

    public BooleanInputStream(OrcInputStream byteStream)
    {
        this.byteStream = new ByteInputStream(byteStream);
    }

    private void readByte()
            throws IOException
    {
        checkState(bitsInData == 0);
        data = byteStream.next();
        bitsInData = 8;
    }

    @SuppressWarnings("NarrowingCompoundAssignment")
    public boolean nextBit()
            throws IOException
    {
        // read more data if necessary
        if (bitsInData == 0) {
            readByte();
        }

        // read bit
        boolean result = (data & HIGH_BIT_MASK) != 0;

        // mark bit consumed
        data <<= 1;
        bitsInData--;

        return result;
    }

    @Override
    public void seekToCheckpoint(BooleanStreamCheckpoint checkpoint)
            throws IOException
    {
        byteStream.seekToCheckpoint(checkpoint.getByteStreamCheckpoint());
        bitsInData = 0;
        skip(checkpoint.getOffset());
    }

    @Override
    @SuppressWarnings("NarrowingCompoundAssignment")
    public void skip(long items)
            throws IOException
    {
        if (bitsInData >= items) {
            data <<= items;
            bitsInData -= items;
        }
        else {
            items -= bitsInData;
            bitsInData = 0;

            byteStream.skip(items >>> 3);
            items &= 0b111;

            if (items != 0) {
                readByte();
                data <<= items;
                bitsInData -= items;
            }
        }
    }

    public int countBitsSet(int items)
            throws IOException
    {
        int count = 0;

        // count buffered data
        if (items > bitsInData && bitsInData > 0) {
            count += bitCount(data);
            items -= bitsInData;
            bitsInData = 0;
        }

        // count whole bytes
        while (items > 8) {
            count += bitCount(byteStream.next());
            items -= 8;
        }

        // count remaining bits
        for (int i = 0; i < items; i++) {
            count += nextBit() ? 1 : 0;
        }

        return count;
    }

    /**
     * Gets a vector of bytes set to 1 if the bit is set.
     */
    public byte[] getSetBits(int batchSize)
            throws IOException
    {
        byte[] vector = new byte[batchSize];
        getSetBits(vector, batchSize);
        return vector;
    }

    /**
     * Sets the vector element to 1 if the bit is set.
     */
    @SuppressWarnings({"NarrowingCompoundAssignment", "PointlessBitwiseExpression", "PointlessArithmeticExpression", "UnusedAssignment"})
    public void getSetBits(byte[] vector, int batchSize)
            throws IOException
    {
        int offset = 0;

        // handle the head
        int count = Math.min(batchSize, bitsInData);
        if (count != 0) {
            int value = data >>> (8 - count);
            switch (count) {
                case 7:
                    vector[offset++] = (byte) ((value & 64) >>> 6);
                    // fall through
                case 6:
                    vector[offset++] = (byte) ((value & 32) >>> 5);
                    // fall through
                case 5:
                    vector[offset++] = (byte) ((value & 16) >>> 4);
                    // fall through
                case 4:
                    vector[offset++] = (byte) ((value & 8) >>> 3);
                    // fall through
                case 3:
                    vector[offset++] = (byte) ((value & 4) >>> 2);
                    // fall through
                case 2:
                    vector[offset++] = (byte) ((value & 2) >>> 1);
                    // fall through
                case 1:
                    vector[offset++] = (byte) ((value & 1) >>> 0);
            }
            data <<= count;
            bitsInData -= count;

            if (count == batchSize) {
                return;
            }
        }

        // the middle part
        while (offset < batchSize - 7) {
            byte value = byteStream.next();
            vector[offset + 0] = (byte) ((value & 128) >>> 7);
            vector[offset + 1] = (byte) ((value & 64) >>> 6);
            vector[offset + 2] = (byte) ((value & 32) >>> 5);
            vector[offset + 3] = (byte) ((value & 16) >>> 4);
            vector[offset + 4] = (byte) ((value & 8) >>> 3);
            vector[offset + 5] = (byte) ((value & 4) >>> 2);
            vector[offset + 6] = (byte) ((value & 2) >>> 1);
            vector[offset + 7] = (byte) (value & 1);
            offset += 8;
        }

        // the tail
        int remaining = batchSize - offset;
        if (remaining > 0) {
            byte data = byteStream.next();
            int value = data >>> (8 - remaining);
            switch (remaining) {
                case 7:
                    vector[offset++] = (byte) ((value & 64) >>> 6);
                    // fall through
                case 6:
                    vector[offset++] = (byte) ((value & 32) >>> 5);
                    // fall through
                case 5:
                    vector[offset++] = (byte) ((value & 16) >>> 4);
                    // fall through
                case 4:
                    vector[offset++] = (byte) ((value & 8) >>> 3);
                    // fall through
                case 3:
                    vector[offset++] = (byte) ((value & 4) >>> 2);
                    // fall through
                case 2:
                    vector[offset++] = (byte) ((value & 2) >>> 1);
                    // fall through
                case 1:
                    vector[offset++] = (byte) ((value & 1) >>> 0);
            }
            this.data = (byte) (data << remaining);
            bitsInData = 8 - remaining;
        }
    }

    /**
     * Sets the vector element to true if the bit is not set.
     */
    @SuppressWarnings({"NarrowingCompoundAssignment", "PointlessArithmeticExpression", "UnusedAssignment"})
    public int getUnsetBits(int batchSize, boolean[] vector)
            throws IOException
    {
        int unsetCount = 0;
        int offset = 0;

        // handle the head
        int count = Math.min(batchSize, bitsInData);
        if (count != 0) {
            int value = (data & 0xFF) >>> (8 - count);
            unsetCount += (count - Integer.bitCount(value));
            switch (count) {
                case 7:
                    vector[offset++] = (value & 64) == 0;
                    // fall through
                case 6:
                    vector[offset++] = (value & 32) == 0;
                    // fall through
                case 5:
                    vector[offset++] = (value & 16) == 0;
                    // fall through
                case 4:
                    vector[offset++] = (value & 8) == 0;
                    // fall through
                case 3:
                    vector[offset++] = (value & 4) == 0;
                    // fall through
                case 2:
                    vector[offset++] = (value & 2) == 0;
                    // fall through
                case 1:
                    vector[offset++] = (value & 1) == 0;
            }
            data <<= count;
            bitsInData -= count;

            if (count == batchSize) {
                return unsetCount;
            }
        }

        // the middle part
        while (offset < batchSize - 7) {
            byte value = byteStream.next();
            unsetCount += (8 - Integer.bitCount(value & 0xFF));
            vector[offset + 0] = (value & 128) == 0;
            vector[offset + 1] = (value & 64) == 0;
            vector[offset + 2] = (value & 32) == 0;
            vector[offset + 3] = (value & 16) == 0;
            vector[offset + 4] = (value & 8) == 0;
            vector[offset + 5] = (value & 4) == 0;
            vector[offset + 6] = (value & 2) == 0;
            vector[offset + 7] = (value & 1) == 0;
            offset += 8;
        }

        // the tail
        int remaining = batchSize - offset;
        if (remaining > 0) {
            byte data = byteStream.next();
            int value = (data & 0xff) >> (8 - remaining);
            unsetCount += (remaining - Integer.bitCount(value));
            switch (remaining) {
                case 7:
                    vector[offset++] = (value & 64) == 0;
                    // fall through
                case 6:
                    vector[offset++] = (value & 32) == 0;
                    // fall through
                case 5:
                    vector[offset++] = (value & 16) == 0;
                    // fall through
                case 4:
                    vector[offset++] = (value & 8) == 0;
                    // fall through
                case 3:
                    vector[offset++] = (value & 4) == 0;
                    // fall through
                case 2:
                    vector[offset++] = (value & 2) == 0;
                    // fall through
                case 1:
                    vector[offset++] = (value & 1) == 0;
            }
            this.data = (byte) (data << remaining);
            bitsInData = 8 - remaining;
        }

        return unsetCount;
    }

    private static int bitCount(byte data)
    {
        return Integer.bitCount(data & 0xFF);
    }
}
