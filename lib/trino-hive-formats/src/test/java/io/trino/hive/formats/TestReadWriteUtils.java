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
package io.trino.hive.formats;

import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import org.apache.hadoop.io.WritableUtils;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;

import static io.trino.hive.formats.ReadWriteUtils.calculateTruncationLength;
import static io.trino.hive.formats.ReadWriteUtils.computeVIntLength;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.lang.Math.min;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

public class TestReadWriteUtils
{
    @Test
    public void testCalculateTruncationLength()
    {
        testCalculateTruncationLength('a');
        testCalculateTruncationLength(30528);
        testCalculateTruncationLength(128165);
    }

    private static void testCalculateTruncationLength(int codePoint)
    {
        testCalculateTruncationLength(codePoint, 50, 50, 20);
        testCalculateTruncationLength(codePoint, 20, 20, 50);
        testCalculateTruncationLength(codePoint, 100, 50, 20);
        testCalculateTruncationLength(codePoint, 100, 20, 50);
    }

    private static void testCalculateTruncationLength(int codePoint, int inputSize, int fieldSize, int truncatedSize)
    {
        assertThat(inputSize).isGreaterThanOrEqualTo(fieldSize);

        int[] inputCodePoints = new int[inputSize];
        Arrays.fill(inputCodePoints, codePoint);

        byte[] inputBytes = new String(inputCodePoints, 0, inputSize).getBytes(UTF_8);
        assertThat(new String(inputBytes, UTF_8).codePoints().count()).isEqualTo(inputSize);

        byte[] fieldBytes = new String(inputCodePoints, 0, fieldSize).getBytes(UTF_8);
        assertThat(new String(fieldBytes, UTF_8).codePoints().count()).isEqualTo(fieldSize);

        byte[] expectedBytes = new String(inputCodePoints, 0, min(truncatedSize, fieldSize)).getBytes(UTF_8);
        assertThat(new String(expectedBytes, UTF_8).codePoints().count()).isEqualTo(min(truncatedSize, fieldSize));

        Slice slice = Slices.wrappedBuffer(inputBytes);
        assertThat(calculateTruncationLength(createVarcharType(truncatedSize), slice, 0, fieldBytes.length))
                .isEqualTo(expectedBytes.length);
    }

    @Test
    public void testVIntLength()
    {
        SliceOutput output = Slices.allocate(100).getOutput();

        assertThat(calculateVintLength(output, Integer.MAX_VALUE)).isEqualTo(5);

        assertThat(calculateVintLength(output, 16777216)).isEqualTo(5);
        assertThat(calculateVintLength(output, 16777215)).isEqualTo(4);

        assertThat(calculateVintLength(output, 65536)).isEqualTo(4);
        assertThat(calculateVintLength(output, 65535)).isEqualTo(3);

        assertThat(calculateVintLength(output, 256)).isEqualTo(3);
        assertThat(calculateVintLength(output, 255)).isEqualTo(2);

        assertThat(calculateVintLength(output, 128)).isEqualTo(2);
        assertThat(calculateVintLength(output, 127)).isEqualTo(1);

        assertThat(calculateVintLength(output, -112)).isEqualTo(1);
        assertThat(calculateVintLength(output, -113)).isEqualTo(2);

        assertThat(calculateVintLength(output, -256)).isEqualTo(2);
        assertThat(calculateVintLength(output, -257)).isEqualTo(3);

        assertThat(calculateVintLength(output, -65536)).isEqualTo(3);
        assertThat(calculateVintLength(output, -65537)).isEqualTo(4);

        assertThat(calculateVintLength(output, -16777216)).isEqualTo(4);
        assertThat(calculateVintLength(output, -16777217)).isEqualTo(5);

        assertThat(calculateVintLength(output, Integer.MIN_VALUE)).isEqualTo(5);
    }

    private static int calculateVintLength(SliceOutput output, int value)
    {
        // write vint to compute expected size
        output.reset();
        ReadWriteUtils.writeVLong(output, value);
        int expectedSize = output.size();

        assertThat(computeVIntLength(value)).isEqualTo(expectedSize);
        return expectedSize;
    }

    @Test
    public void testVInt()
            throws Exception
    {
        Slice slice = Slices.allocate(100);
        SliceOutput output = slice.getOutput();

        assertVIntRoundTrip(output, 0);
        assertVIntRoundTrip(output, 1);
        assertVIntRoundTrip(output, -1);
        assertVIntRoundTrip(output, Integer.MAX_VALUE);
        assertVIntRoundTrip(output, Integer.MAX_VALUE + 1L);
        assertVIntRoundTrip(output, Integer.MAX_VALUE - 1L);
        assertVIntRoundTrip(output, Integer.MIN_VALUE);
        assertVIntRoundTrip(output, Integer.MIN_VALUE + 1L);
        assertVIntRoundTrip(output, Integer.MIN_VALUE - 1L);
        assertVIntRoundTrip(output, Long.MAX_VALUE);
        assertVIntRoundTrip(output, Long.MAX_VALUE - 1);
        assertVIntRoundTrip(output, Long.MIN_VALUE + 1);

        for (int value = -100_000; value < 100_000; value++) {
            assertVIntRoundTrip(output, value);
        }
    }

    private static void assertVIntRoundTrip(SliceOutput output, long value)
            throws IOException
    {
        Slice oldBytes = writeVint(output, value);

        long readValueOld = WritableUtils.readVLong(oldBytes.getInput());
        assertThat(readValueOld).isEqualTo(value);

        long readValueNew = ReadWriteUtils.readVInt(oldBytes, 0);
        assertThat(readValueNew).isEqualTo(value);

        long readValueNewStream = ReadWriteUtils.readVInt(oldBytes.getInput());
        assertThat(readValueNewStream).isEqualTo(value);
    }

    private static Slice writeVint(SliceOutput output, long value)
            throws IOException
    {
        output.reset();
        WritableUtils.writeVLong(output, value);
        Slice vLongOld = output.slice().copy();

        output.reset();
        ReadWriteUtils.writeVLong(output, value);
        Slice vLongNew = output.slice().copy();
        assertThat(vLongNew).isEqualTo(vLongOld);

        if (value == (int) value) {
            output.reset();
            WritableUtils.writeVInt(output, (int) value);
            Slice vIntOld = output.slice().copy();
            assertThat(vIntOld).isEqualTo(vLongOld);

            output.reset();
            ReadWriteUtils.writeVInt(output, (int) value);
            Slice vIntNew = output.slice().copy();
            assertThat(vIntNew).isEqualTo(vLongOld);

            assertThat(computeVIntLength((int) value)).isEqualTo(vIntNew.length());
        }
        return vLongOld;
    }
}
