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
package io.trino.filesystem.s3;

import io.trino.filesystem.s3.S3OutputStream.LinkedBuffer;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SuppressWarnings("resource")
public class TestLinkedBuffer
{
    @Test
    void testInvalidConfiguration()
    {
        assertThatThrownBy(() -> new LinkedBuffer(100, 10))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("initialBufferSize must be less than or equal to maxBufferSize");
    }

    @Test
    void testEqualConfiguration()
            throws IOException
    {
        LinkedBuffer buffer = new LinkedBuffer(10, 10);
        assertThat(buffer.size()).isZero();
        assertThat(buffer.takeInputStream().readAllBytes()).isEmpty();
    }

    @Test
    void testEmptyBuffer()
            throws IOException
    {
        LinkedBuffer buffer = new LinkedBuffer(10, 100);

        assertThat(buffer.size()).isZero();
        assertThat(buffer.takeInputStream().readAllBytes()).isEmpty();
    }

    @Test
    void testSingleWrite()
            throws IOException
    {
        LinkedBuffer buffer = new LinkedBuffer(10, 100);

        buffer.write(42);

        assertThat(buffer.size()).isEqualTo(1);
        assertThat(buffer.takeInputStream().readAllBytes()).containsExactly(42);
    }

    @Test
    void testMultipleWrite()
            throws IOException
    {
        LinkedBuffer buffer = new LinkedBuffer(10, 50);
        byte[] chunk1 = {1, 2, 3};
        byte[] chunk2 = {4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14};

        buffer.write(chunk1, 0, chunk1.length);
        buffer.write(chunk2, 0, chunk2.length);

        assertThat(buffer.size()).isEqualTo(14);

        byte[] expected = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14};
        assertThat(buffer.takeInputStream().readAllBytes()).isEqualTo(expected);
    }

    @Test
    void testSmallWriteWithOffsetAndLength()
            throws IOException
    {
        LinkedBuffer buffer = new LinkedBuffer(10, 100);
        byte[] source = {0, 0, 1, 2, 3, 0, 0};

        buffer.write(source, 2, 3);

        assertThat(buffer.size()).isEqualTo(3);
        assertThat(buffer.takeInputStream().readAllBytes()).containsExactly(1, 2, 3);
    }

    @Test
    void testLargeWriteWithOffsetAndLength()
            throws IOException
    {
        LinkedBuffer buffer = new LinkedBuffer(10, 100);
        byte[] source = testBytes(1000);

        buffer.write(source, 300, 700);

        assertThat(buffer.size()).isEqualTo(700);
        assertThat(buffer.takeInputStream().readAllBytes()).isEqualTo(Arrays.copyOfRange(source, 300, 1000));
    }

    @Test
    void testBufferExpansion()
            throws IOException
    {
        LinkedBuffer buffer = new LinkedBuffer(10, 100);
        byte[] data = testBytes(25);

        buffer.write(data, 0, data.length);

        // writing 25 bytes should trigger expansion
        assertThat(buffer.parts.size()).isEqualTo(1);
        assertThat(buffer.size()).isEqualTo(25);
        assertThat(buffer.takeInputStream().readAllBytes()).isEqualTo(data);
    }

    @Test
    void testMaxBufferSizeCeiling()
            throws IOException
    {
        LinkedBuffer buffer = new LinkedBuffer(10, 30);
        byte[] data = testBytes(100);

        buffer.write(data, 0, data.length);

        // writing 100 should hit max buffer size
        assertThat(buffer.currentBufferSize).isEqualTo(30);
        assertThat(buffer.size()).isEqualTo(100);
        assertThat(buffer.takeInputStream().readAllBytes()).isEqualTo(data);
    }

    @Test
    void testReset()
            throws IOException
    {
        LinkedBuffer buffer = new LinkedBuffer(10, 100);
        byte[] data = {1, 2, 3, 4, 5};

        buffer.write(data, 0, data.length);
        assertThat(buffer.size()).isEqualTo(5);

        buffer.reset();

        assertThat(buffer.size()).isZero();
        assertThat(buffer.takeInputStream().readAllBytes()).isEmpty();

        buffer.write(data, 0, data.length);
        assertThat(buffer.size()).isEqualTo(5);
        assertThat(buffer.takeInputStream().readAllBytes()).isEqualTo(data);
    }

    @Test
    void testTakeInputStream()
            throws IOException
    {
        LinkedBuffer buffer = new LinkedBuffer(10, 30);
        byte[] data = testBytes(50);

        buffer.write(data, 0, data.length);

        byte[] firstRead = buffer.takeInputStream().readAllBytes();
        byte[] secondRead = buffer.takeInputStream().readAllBytes();

        assertThat(firstRead).isEqualTo(data);
        assertThat(secondRead).isEqualTo(data);
    }

    @Test
    void testAllocatedSize()
    {
        LinkedBuffer buffer = new LinkedBuffer(10, 50);
        byte[] data = testBytes(50);

        buffer.write(data, 0, data.length);

        assertThat(buffer.size()).isEqualTo(data.length);
        // internal buffer doubles twice
        // 10 + 20 + 40 = 70
        assertThat(buffer.allocated()).isEqualTo(70);
    }

    @Test
    void testLazyBufferAllocation()
    {
        LinkedBuffer buffer = new LinkedBuffer(10, 50);
        byte[] data = testBytes(30);

        buffer.write(data, 0, data.length);

        assertThat(buffer.size()).isEqualTo(data.length);
        // internal buffer doubles twice, but should not allocate until the next write
        // 10 + 20 = 30
        assertThat(buffer.allocated()).isEqualTo(30);
    }

    @Test
    void testAllocatedSizeMaxBufferSize()
    {
        LinkedBuffer buffer = new LinkedBuffer(10, 30);
        byte[] data = testBytes(50);

        buffer.write(data, 0, data.length);

        assertThat(buffer.size()).isEqualTo(data.length);
        // internal buffer doubles twice, but hits the maximum buffer size
        // 10 + 20 + 30 = 60
        assertThat(buffer.allocated()).isEqualTo(60);
    }

    private static byte[] testBytes(int length)
    {
        byte[] result = new byte[length];
        for (int i = 0; i < length; i++) {
            result[i] = (byte) (i % 256);
        }
        return result;
    }
}
