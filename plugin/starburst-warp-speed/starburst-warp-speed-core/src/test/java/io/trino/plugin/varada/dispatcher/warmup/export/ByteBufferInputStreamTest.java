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
package io.trino.plugin.varada.dispatcher.warmup.export;

import io.varada.tools.ByteBufferInputStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("ResultOfMethodCallIgnored")
public class ByteBufferInputStreamTest
{
    private ByteBuffer nativeJuffer;

    @BeforeEach
    public void before()
    {
        this.nativeJuffer = ByteBuffer.allocate(100);
    }

    @Test
    public void testSimpleRead()
            throws IOException
    {
        nativeJuffer.put((byte) 1);
        try (ByteBufferInputStream byteBufferInputStream = new ByteBufferInputStream(nativeJuffer, 10)) {
            int read = byteBufferInputStream.read();
            assertThat(byteBufferInputStream.available()).isNotEqualTo(-1);
            assertThat(read).isEqualTo(1);
        }
    }

    @Test
    public void testReadMoreThenOneBuffer()
            throws IOException
    {
        try (ByteBufferInputStream byteBufferInputStream = new ByteBufferInputStream(nativeJuffer, 10)) {
            IntStream.range(0, 10).forEach((i) -> byteBufferInputStream.read());
            byteBufferInputStream.read();
            assertThat(byteBufferInputStream.available()).isZero();
        }
    }

    @Test
    public void testSkip()
            throws IOException
    {
        byte[] data = new byte[] {1, 2, 3, 4, 5};
        nativeJuffer.put(data);
        try (ByteBufferInputStream byteBufferInputStream = new ByteBufferInputStream(nativeJuffer, data.length)) {
            // Skip a negative amount of bytes and read the 1st byte
            assertThat(byteBufferInputStream.skip(-7)).isEqualTo(0);
            assertThat(byteBufferInputStream.available()).isEqualTo(data.length);
            int readByte = byteBufferInputStream.read();
            assertThat(readByte).isEqualTo(data[0]);

            // Skip the 2nd byte and read the 3rd byte
            long skippedBytes = byteBufferInputStream.skip(1);
            assertThat(skippedBytes).isEqualTo(1);
            assertThat(byteBufferInputStream.available()).isEqualTo(data.length - 2);
            readByte = byteBufferInputStream.read();
            assertThat(readByte).isEqualTo(data[2]);

            // Skip more bytes than available
            int availableBytes = byteBufferInputStream.available();
            skippedBytes = byteBufferInputStream.skip(availableBytes + 1);
            assertThat(skippedBytes).isEqualTo(availableBytes);
            assertThat(byteBufferInputStream.available()).isEqualTo(0);
        }
    }

    @Test
    public void testMarkAndReset()
            throws IOException
    {
        byte[] data = new byte[] {1, 2, 3};
        nativeJuffer.put(data);
        try (ByteBufferInputStream byteBufferInputStream = new ByteBufferInputStream(nativeJuffer, data.length)) {
            assertThat(byteBufferInputStream.markSupported()).isTrue();

            byteBufferInputStream.read(); // Read the 1st byte
            byteBufferInputStream.mark(100); // Mark position on the 2nd byte
            int readByteAfterMark = byteBufferInputStream.read(); // Read the 2nd byte
            byteBufferInputStream.read(); // Read the 3rd byte
            assertThat(byteBufferInputStream.available()).isEqualTo(0);
            byteBufferInputStream.reset(); // Reset back to the 2nd byte
            assertThat(byteBufferInputStream.available()).isEqualTo(2);
            int readByteAfterReset = byteBufferInputStream.read(); // Read the 2nd byte again
            assertThat(readByteAfterReset).isEqualTo(readByteAfterMark);
        }
    }
}
