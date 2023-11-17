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
package io.trino.orc;

import io.airlift.slice.DynamicSliceOutput;
import io.trino.orc.metadata.CompressionKind;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static io.airlift.slice.Slices.wrappedBuffer;
import static org.assertj.core.api.Assertions.assertThat;

public class TestOrcOutputBuffer
{
    @Test
    public void testWriteHugeByteChucks()
    {
        int size = 1024 * 1024;
        byte[] largeByteArray = new byte[size];
        Arrays.fill(largeByteArray, (byte) 0xA);
        OrcOutputBuffer sliceOutput = new OrcOutputBuffer(CompressionKind.NONE, 256 * 1024);

        DynamicSliceOutput output = new DynamicSliceOutput(size);
        sliceOutput.writeBytes(largeByteArray, 10, size - 10);
        assertThat(sliceOutput.writeDataTo(output)).isEqualTo(size - 10);
        assertThat(output.slice()).isEqualTo(wrappedBuffer(largeByteArray, 10, size - 10));

        sliceOutput.reset();
        output.reset();
        sliceOutput.writeBytes(wrappedBuffer(largeByteArray), 100, size - 100);
        assertThat(sliceOutput.writeDataTo(output)).isEqualTo(size - 100);
        assertThat(output.slice()).isEqualTo(wrappedBuffer(largeByteArray, 100, size - 100));
    }

    @Test
    public void testGrowCapacity()
    {
        byte[] largeByteArray = new byte[4096];
        OrcOutputBuffer sliceOutput = new OrcOutputBuffer(CompressionKind.NONE, 3000);

        // write some data that can fit the initial capacity = 256
        sliceOutput.writeBytes(largeByteArray, 0, 200);
        assertThat(sliceOutput.getBufferCapacity()).isEqualTo(256);

        // write some more data to exceed the capacity = 256; the capacity will double
        sliceOutput.writeBytes(largeByteArray, 0, 200);
        assertThat(sliceOutput.getBufferCapacity()).isEqualTo(512);

        // write a lot more data to exceed twice the capacity = 512 X 2; the capacity will be the required data size
        sliceOutput.writeBytes(largeByteArray, 0, 1200);
        assertThat(sliceOutput.getBufferCapacity()).isEqualTo(1200);

        // write some more data to double the capacity again
        sliceOutput.writeBytes(largeByteArray, 0, 2000);
        assertThat(sliceOutput.getBufferCapacity()).isEqualTo(2400);

        // make the buffer to reach the max buffer capacity
        sliceOutput.writeBytes(largeByteArray, 0, 2500);
        assertThat(sliceOutput.getBufferCapacity()).isEqualTo(3000);

        // make sure we didn't miss anything
        DynamicSliceOutput output = new DynamicSliceOutput(6000);
        sliceOutput.close();
        assertThat(sliceOutput.writeDataTo(output)).isEqualTo(200 + 200 + 1200 + 2000 + 2500);
    }
}
