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
package io.trino.plugin.hudi.io;

import io.trino.filesystem.TrinoInputStream;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestInlineSeekableDataInputStream
{
    public static final String CONST_STR_FOR_BYTES = "0123456789ABCDEFGHIJ";

    @Test
    void testStreamIsSeekableToStartOffsetUponInitialization()
            throws IOException
    {
        // Create a test stream with data at various positions
        byte[] data = CONST_STR_FOR_BYTES.getBytes(StandardCharsets.UTF_8);
        TestTrinoInputStream stream = new TestTrinoInputStream(data);

        long startOffset = 5;
        long length = 10;

        // Initialize InlineSeekableDataInputStream
        InlineSeekableDataInputStream inlineStream = new InlineSeekableDataInputStream(stream, startOffset, length);

        // Verify the stream was seeked to the startOffset during initialization
        assertThat(stream.getPosition()).isEqualTo(startOffset);

        // Verify getPos() returns 0 (relative to startOffset)
        assertThat(inlineStream.getPos()).isEqualTo(0);
    }

    @Test
    void testGetPosReturnsRelativePosition()
            throws IOException
    {
        byte[] data = CONST_STR_FOR_BYTES.getBytes(StandardCharsets.UTF_8);
        TestTrinoInputStream stream = new TestTrinoInputStream(data);

        long startOffset = 3;
        long length = 10;

        InlineSeekableDataInputStream inlineStream = new InlineSeekableDataInputStream(stream, startOffset, length);

        // Initially at position 0 (relative)
        assertThat(inlineStream.getPos()).isEqualTo(0);

        // Seek to position 5 (relative)
        inlineStream.seek(5);
        assertThat(inlineStream.getPos()).isEqualTo(5);

        // Verify underlying stream is at startOffset + 5
        assertThat(stream.getPosition()).isEqualTo(startOffset + 5);
    }

    @Test
    void testSeekWithinBounds()
            throws IOException
    {
        byte[] data = CONST_STR_FOR_BYTES.getBytes(StandardCharsets.UTF_8);
        TestTrinoInputStream stream = new TestTrinoInputStream(data);

        long startOffset = 2;
        long length = 8;

        InlineSeekableDataInputStream inlineStream = new InlineSeekableDataInputStream(stream, startOffset, length);

        // Seek to the middle
        inlineStream.seek(4);
        assertThat(inlineStream.getPos()).isEqualTo(4);
        assertThat(stream.getPosition()).isEqualTo(startOffset + 4);

        // Seek to the end (length is exclusive, so seeking to length should work)
        inlineStream.seek(8);
        assertThat(inlineStream.getPos()).isEqualTo(8);
        assertThat(stream.getPosition()).isEqualTo(startOffset + 8);
    }

    @Test
    void testSeekPastLengthThrowsException()
            throws IOException
    {
        byte[] data = CONST_STR_FOR_BYTES.getBytes(StandardCharsets.UTF_8);
        TestTrinoInputStream stream = new TestTrinoInputStream(data);

        long startOffset = 5;
        long length = 10;

        InlineSeekableDataInputStream inlineStream = new InlineSeekableDataInputStream(stream, startOffset, length);

        // Attempting to seek past the length should throw IOException
        assertThatThrownBy(() -> inlineStream.seek(11))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Attempting to seek past inline content")
                .hasMessageContaining("position to seek to is 11")
                .hasMessageContaining("but the length is 10");
    }

    @Test
    void testReadDataAtCorrectOffset()
            throws IOException
    {
        byte[] data = CONST_STR_FOR_BYTES.getBytes(StandardCharsets.UTF_8);
        TestTrinoInputStream stream = new TestTrinoInputStream(data);

        long startOffset = 5; // Start at '5'
        long length = 5;      // Read 5 bytes: "56789"

        InlineSeekableDataInputStream inlineStream = new InlineSeekableDataInputStream(stream, startOffset, length);

        // Read first byte should be '5' (byte at position startOffset)
        int firstByte = inlineStream.read();
        assertThat(firstByte).isEqualTo('5');
        assertThat(inlineStream.getPos()).isEqualTo(1);

        // Read next byte should be '6'
        int secondByte = inlineStream.read();
        assertThat(secondByte).isEqualTo('6');
        assertThat(inlineStream.getPos()).isEqualTo(2);
    }

    @Test
    void testSeekAndRead()
            throws IOException
    {
        byte[] data = CONST_STR_FOR_BYTES.getBytes(StandardCharsets.UTF_8);
        TestTrinoInputStream stream = new TestTrinoInputStream(data);

        long startOffset = 10; // Start at 'A'
        long length = 10;      // Length covers "ABCDEFGHIJ"

        InlineSeekableDataInputStream inlineStream = new InlineSeekableDataInputStream(stream, startOffset, length);

        // Seek to position 3 (relative) - should be at 'D' (position 13 absolute)
        inlineStream.seek(3);
        assertThat(inlineStream.getPos()).isEqualTo(3);

        // Read should get 'D'
        int readByte = inlineStream.read();
        assertThat(readByte).isEqualTo('D');
        assertThat(inlineStream.getPos()).isEqualTo(4);
    }

    @Test
    void testZeroLengthInlineStream()
            throws IOException
    {
        byte[] data = "0123456789".getBytes(StandardCharsets.UTF_8);
        TestTrinoInputStream stream = new TestTrinoInputStream(data);

        long startOffset = 5;
        long length = 0;

        InlineSeekableDataInputStream inlineStream = new InlineSeekableDataInputStream(stream, startOffset, length);

        // Position should be 0
        assertThat(inlineStream.getPos()).isEqualTo(0);

        // Seeking to any position > 0 should fail
        assertThatThrownBy(() -> inlineStream.seek(1))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Attempting to seek past inline content");
    }

    /**
     * Test implementation of TrinoInputStream for unit testing.
     */
    private static class TestTrinoInputStream
            extends TrinoInputStream
    {
        private final byte[] data;
        private int position;

        public TestTrinoInputStream(byte[] data)
        {
            this.data = data;
            this.position = 0;
        }

        @Override
        public long getPosition()
        {
            return position;
        }

        @Override
        public void seek(long position)
                throws IOException
        {
            if (position < 0 || position > data.length) {
                throw new IOException("Invalid seek position: " + position);
            }
            this.position = (int) position;
        }

        @Override
        public int read()
                throws IOException
        {
            if (position >= data.length) {
                return -1;
            }
            return data[position++] & 0xFF;
        }

        @Override
        public int read(byte[] b, int off, int len)
                throws IOException
        {
            if (position >= data.length) {
                return -1;
            }
            int available = data.length - position;
            int toRead = Math.min(len, available);
            System.arraycopy(data, position, b, off, toRead);
            position += toRead;
            return toRead;
        }

        @Override
        public void close()
                throws IOException
        {
            // No-op for test
        }
    }
}
