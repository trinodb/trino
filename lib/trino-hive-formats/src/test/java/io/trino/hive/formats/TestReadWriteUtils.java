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
import org.testng.annotations.Test;

import java.io.IOException;

import static io.trino.hive.formats.ReadWriteUtils.computeVIntLength;
import static org.testng.Assert.assertEquals;

public class TestReadWriteUtils
{
    @Test
    public void testVIntLength()
    {
        SliceOutput output = Slices.allocate(100).getOutput();

        assertEquals(calculateVintLength(output, Integer.MAX_VALUE), 5);

        assertEquals(calculateVintLength(output, 16777216), 5);
        assertEquals(calculateVintLength(output, 16777215), 4);

        assertEquals(calculateVintLength(output, 65536), 4);
        assertEquals(calculateVintLength(output, 65535), 3);

        assertEquals(calculateVintLength(output, 256), 3);
        assertEquals(calculateVintLength(output, 255), 2);

        assertEquals(calculateVintLength(output, 128), 2);
        assertEquals(calculateVintLength(output, 127), 1);

        assertEquals(calculateVintLength(output, -112), 1);
        assertEquals(calculateVintLength(output, -113), 2);

        assertEquals(calculateVintLength(output, -256), 2);
        assertEquals(calculateVintLength(output, -257), 3);

        assertEquals(calculateVintLength(output, -65536), 3);
        assertEquals(calculateVintLength(output, -65537), 4);

        assertEquals(calculateVintLength(output, -16777216), 4);
        assertEquals(calculateVintLength(output, -16777217), 5);

        assertEquals(calculateVintLength(output, Integer.MIN_VALUE), 5);
    }

    private static int calculateVintLength(SliceOutput output, int value)
    {
        // write vint to compute expected size
        output.reset();
        ReadWriteUtils.writeVLong(output, value);
        int expectedSize = output.size();

        assertEquals(computeVIntLength(value), expectedSize);
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
        assertEquals(readValueOld, value);

        long readValueNew = ReadWriteUtils.readVInt(oldBytes, 0);
        assertEquals(readValueNew, value);

        long readValueNewStream = ReadWriteUtils.readVInt(oldBytes.getInput());
        assertEquals(readValueNewStream, value);
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
        assertEquals(vLongNew, vLongOld);

        if (value == (int) value) {
            output.reset();
            WritableUtils.writeVInt(output, (int) value);
            Slice vIntOld = output.slice().copy();
            assertEquals(vIntOld, vLongOld);

            output.reset();
            ReadWriteUtils.writeVInt(output, (int) value);
            Slice vIntNew = output.slice().copy();
            assertEquals(vIntNew, vLongOld);

            assertEquals(computeVIntLength((int) value), vIntNew.length());
        }
        return vLongOld;
    }
}
