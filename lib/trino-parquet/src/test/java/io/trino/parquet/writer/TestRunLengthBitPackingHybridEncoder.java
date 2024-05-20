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
package io.trino.parquet.writer;

import io.trino.parquet.writer.valuewriter.RunLengthBitPackingHybridEncoder;
import org.apache.parquet.column.values.bitpacking.BytePacker;
import org.apache.parquet.column.values.bitpacking.Packer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.parquet.bytes.BytesUtils.readIntLittleEndianOnOneByte;
import static org.apache.parquet.bytes.BytesUtils.readIntLittleEndianOnTwoBytes;
import static org.apache.parquet.bytes.BytesUtils.readUnsignedVarInt;
import static org.assertj.core.api.Assertions.assertThat;

public class TestRunLengthBitPackingHybridEncoder
{
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testRLEOnly(boolean useWriteRepeated)
            throws Exception
    {
        RunLengthBitPackingHybridEncoder encoder = getRunLengthBitPackingHybridEncoder();
        writeRepeatedValue(encoder, 4, 100, useWriteRepeated);
        writeRepeatedValue(encoder, 5, 100, useWriteRepeated);

        ByteArrayInputStream is = new ByteArrayInputStream(encoder.toBytes().toByteArray());

        // header = 100 << 1 = 200
        assertThat(readUnsignedVarInt(is)).isEqualTo(200);
        // payload = 4
        assertThat(readIntLittleEndianOnOneByte(is)).isEqualTo(4);

        // header = 100 << 1 = 200
        assertThat(readUnsignedVarInt(is)).isEqualTo(200);
        // payload = 5
        assertThat(readIntLittleEndianOnOneByte(is)).isEqualTo(5);

        // end of stream
        assertThat(is.read()).isEqualTo(-1);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testRepeatedZeros(boolean useWriteRepeated)
            throws Exception
    {
        // previousValue is initialized to 0
        // make sure that repeated 0s at the beginning
        // of the stream don't trip up the repeat count

        RunLengthBitPackingHybridEncoder encoder = getRunLengthBitPackingHybridEncoder();
        writeRepeatedValue(encoder, 0, 10, useWriteRepeated);

        ByteArrayInputStream is = new ByteArrayInputStream(encoder.toBytes().toByteArray());

        // header = 10 << 1 = 20
        assertThat(readUnsignedVarInt(is)).isEqualTo(20);
        // payload = 4
        assertThat(readIntLittleEndianOnOneByte(is)).isEqualTo(0);

        // end of stream
        assertThat(is.read()).isEqualTo(-1);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testBitWidthZero(boolean useWriteRepeated)
            throws Exception
    {
        RunLengthBitPackingHybridEncoder encoder = getRunLengthBitPackingHybridEncoder(0, 64);
        writeRepeatedValue(encoder, 0, 10, useWriteRepeated);

        ByteArrayInputStream is = new ByteArrayInputStream(encoder.toBytes().toByteArray());

        // header = 10 << 1 = 20
        assertThat(readUnsignedVarInt(is)).isEqualTo(20);

        // end of stream
        assertThat(is.read()).isEqualTo(-1);
    }

    @Test
    public void testBitPackingOnly()
            throws Exception
    {
        RunLengthBitPackingHybridEncoder encoder = getRunLengthBitPackingHybridEncoder();
        for (int i = 0; i < 100; i++) {
            encoder.writeInt(i % 3);
        }

        ByteArrayInputStream is = new ByteArrayInputStream(encoder.toBytes().toByteArray());

        // header = ((104/8) << 1) | 1 = 27
        assertThat(readUnsignedVarInt(is)).isEqualTo(27);

        List<Integer> values = unpack(3, 104, is);

        for (int i = 0; i < 100; i++) {
            assertThat((int) values.get(i)).isEqualTo(i % 3);
        }

        // end of stream
        assertThat(is.read()).isEqualTo(-1);
    }

    @Test
    public void testBitPackingOverflow()
            throws Exception
    {
        RunLengthBitPackingHybridEncoder encoder = getRunLengthBitPackingHybridEncoder();

        for (int i = 0; i < 1000; i++) {
            encoder.writeInt(i % 3);
        }

        ByteArrayInputStream is = new ByteArrayInputStream(encoder.toBytes().toByteArray());

        // 504 is the max number of values in a bit packed run
        // that still has a header of 1 byte
        // header = ((504/8) << 1) | 1 = 127
        assertThat(readUnsignedVarInt(is)).isEqualTo(127);
        List<Integer> values = unpack(3, 504, is);

        for (int i = 0; i < 504; i++) {
            assertThat((int) values.get(i)).isEqualTo(i % 3);
        }

        // there should now be 496 values in another bit-packed run
        // header = ((496/8) << 1) | 1 = 125
        assertThat(readUnsignedVarInt(is)).isEqualTo(125);
        values = unpack(3, 496, is);
        for (int i = 0; i < 496; i++) {
            assertThat((int) values.get(i)).isEqualTo((i + 504) % 3);
        }

        // end of stream
        assertThat(is.read()).isEqualTo(-1);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testTransitionFromBitPackingToRle(boolean useWriteRepeated)
            throws Exception
    {
        RunLengthBitPackingHybridEncoder encoder = getRunLengthBitPackingHybridEncoder();

        // 5 obviously bit-packed values
        encoder.writeInt(0);
        encoder.writeInt(1);
        encoder.writeInt(0);
        encoder.writeInt(1);
        encoder.writeInt(0);

        // three repeated values, that ought to be bit-packed as well
        writeRepeatedValue(encoder, 2, 3, useWriteRepeated);

        // lots more repeated values, that should be rle-encoded
        writeRepeatedValue(encoder, 2, 100, useWriteRepeated);

        ByteArrayInputStream is = new ByteArrayInputStream(encoder.toBytes().toByteArray());

        // header = ((8/8) << 1) | 1 = 3
        assertThat(readUnsignedVarInt(is)).isEqualTo(3);

        List<Integer> values = unpack(3, 8, is);
        assertThat(values).isEqualTo(Arrays.asList(0, 1, 0, 1, 0, 2, 2, 2));

        // header = 100 << 1 = 200
        assertThat(readUnsignedVarInt(is)).isEqualTo(200);
        // payload = 2
        assertThat(readIntLittleEndianOnOneByte(is)).isEqualTo(2);

        // end of stream
        assertThat(is.read()).isEqualTo(-1);
    }

    @Test
    public void testPaddingZerosOnUnfinishedBitPackedRuns()
            throws Exception
    {
        RunLengthBitPackingHybridEncoder encoder = getRunLengthBitPackingHybridEncoder(5, 64);
        for (int i = 0; i < 9; i++) {
            encoder.writeInt(i + 1);
        }

        ByteArrayInputStream is = new ByteArrayInputStream(encoder.toBytes().toByteArray());

        // header = ((16/8) << 1) | 1 = 5
        assertThat(readUnsignedVarInt(is)).isEqualTo(5);

        List<Integer> values = unpack(5, 16, is);

        assertThat(values).isEqualTo(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 0, 0, 0, 0, 0, 0));

        assertThat(is.read()).isEqualTo(-1);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testSwitchingModes(boolean useWriteRepeated)
            throws Exception
    {
        RunLengthBitPackingHybridEncoder encoder = getRunLengthBitPackingHybridEncoder(9, 1000);

        // rle first
        writeRepeatedValue(encoder, 17, 25, useWriteRepeated);

        // bit-packing
        writeRepeatedValue(encoder, 7, 7, useWriteRepeated);

        encoder.writeInt(8);
        encoder.writeInt(9);
        encoder.writeInt(10);

        // bit-packing followed by rle
        writeRepeatedValue(encoder, 6, 25, useWriteRepeated);

        // followed by a different rle
        writeRepeatedValue(encoder, 5, 8, useWriteRepeated);

        ByteArrayInputStream is = new ByteArrayInputStream(encoder.toBytes().toByteArray());

        // header = 25 << 1 = 50
        assertThat(readUnsignedVarInt(is)).isEqualTo(50);
        // payload = 17, stored in 2 bytes
        assertThat(readIntLittleEndianOnTwoBytes(is)).isEqualTo(17);

        // header = ((16/8) << 1) | 1 = 5
        assertThat(readUnsignedVarInt(is)).isEqualTo(5);
        List<Integer> values = unpack(9, 16, is);
        int v = 0;
        for (int i = 0; i < 7; i++) {
            assertThat((int) values.get(v)).isEqualTo(7);
            v++;
        }

        assertThat((int) values.get(v++)).isEqualTo(8);
        assertThat((int) values.get(v++)).isEqualTo(9);
        assertThat((int) values.get(v++)).isEqualTo(10);

        for (int i = 0; i < 6; i++) {
            assertThat((int) values.get(v)).isEqualTo(6);
            v++;
        }

        // header = 19 << 1 = 38
        assertThat(readUnsignedVarInt(is)).isEqualTo(38);
        // payload = 6, stored in 2 bytes
        assertThat(readIntLittleEndianOnTwoBytes(is)).isEqualTo(6);

        // header = 8 << 1  = 16
        assertThat(readUnsignedVarInt(is)).isEqualTo(16);
        // payload = 5, stored in 2 bytes
        assertThat(readIntLittleEndianOnTwoBytes(is)).isEqualTo(5);

        // end of stream
        assertThat(is.read()).isEqualTo(-1);
    }

    private static List<Integer> unpack(int bitWidth, int numValues, ByteArrayInputStream is)
    {
        BytePacker packer = Packer.LITTLE_ENDIAN.newBytePacker(bitWidth);
        int[] unpacked = new int[8];
        byte[] next8Values = new byte[bitWidth];

        List<Integer> values = new ArrayList<>(numValues);
        while (values.size() < numValues) {
            for (int i = 0; i < bitWidth; i++) {
                next8Values[i] = (byte) is.read();
            }

            packer.unpack8Values(next8Values, 0, unpacked, 0);

            for (int v = 0; v < 8; v++) {
                values.add(unpacked[v]);
            }
        }
        return values;
    }

    private static RunLengthBitPackingHybridEncoder getRunLengthBitPackingHybridEncoder()
    {
        return getRunLengthBitPackingHybridEncoder(3, 64);
    }

    private static RunLengthBitPackingHybridEncoder getRunLengthBitPackingHybridEncoder(int bitWidth, int maxCapacityHint)
    {
        return new RunLengthBitPackingHybridEncoder(bitWidth, maxCapacityHint);
    }

    private static void writeRepeatedValue(RunLengthBitPackingHybridEncoder encoder, int value, int repetitions, boolean useWriteRepeated)
            throws Exception
    {
        if (useWriteRepeated) {
            encoder.writeRepeatedInteger(value, repetitions);
        }
        else {
            for (int i = 0; i < repetitions; i++) {
                encoder.writeInt(value);
            }
        }
    }
}
