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
package io.trino.plugin.varada.juffer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

@SuppressWarnings("ResultOfMethodCallIgnored")
public class ByteBufferInputStreamTest
{
    private ByteBuffer byteBuffer;

    @BeforeEach
    public void before()
    {
        this.byteBuffer = ByteBuffer.allocate(100);
    }

    @Test
    public void testSimpleRead()
            throws IOException
    {
        int dataPosition = 10;
        byteBuffer.put(dataPosition, (byte) 1);
        try (ByteBufferInputStream byteBufferInputStream = new ByteBufferInputStream(byteBuffer, dataPosition)) {
            assertThat(byteBufferInputStream.readByte()).isEqualTo((byte) 1);
        }
    }

    @Test
    public void testReadMoreThenOneBuffer()
            throws IOException
    {
        byteBuffer.position(0);
        try (ByteBufferInputStream byteBufferInputStream = new ByteBufferInputStream(byteBuffer)) {
            IntStream.range(0, 100 / Integer.BYTES).forEach((i) -> byteBufferInputStream.readInt());
            try {
                byteBufferInputStream.readInt(); // should be beyond the buffer limits
                fail("should not get here");
            }
            catch (Exception e) {
            }
        }
    }

    @Test
    public void testSkip()
            throws IOException
    {
        byte[] data = new byte[] {1, 2, 3, 4, 5};
        byteBuffer.position(0);
        byteBuffer.put(data);
        try (ByteBufferInputStream byteBufferInputStream = new ByteBufferInputStream(byteBuffer)) {
            // Skip a negative amount of bytes and read the 1st byte
            byteBufferInputStream.skip(2);
            assertThat(byteBufferInputStream.readByte()).isEqualTo((byte) 3);
        }
    }
}
