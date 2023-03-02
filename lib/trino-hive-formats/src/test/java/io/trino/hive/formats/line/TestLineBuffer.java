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
package io.trino.hive.formats.line;

import com.google.common.primitives.Bytes;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.lang.Math.min;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

public class TestLineBuffer
{
    private static final int LINE_BUFFER_INSTANCE_SIZE = instanceSize(LineBuffer.class);

    @Test
    public void testInvalidConstructorArgs()
    {
        assertThatThrownBy(() -> new LineBuffer(0, 100))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new LineBuffer(-1, 100))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new LineBuffer(100, 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new LineBuffer(100, -1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new LineBuffer(100, (1024 * 1024 * 1024) + 1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new LineBuffer(101, 100))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testInitialBuffer()
    {
        LineBuffer lineBuffer = new LineBuffer(7, 100);
        assertThat(lineBuffer.getBuffer()).hasSize(7);
        assertThat(lineBuffer.getLength()).isEqualTo(0);
        assertThat(lineBuffer.isEmpty()).isTrue();
    }

    @Test
    public void testAppend()
            throws IOException
    {
        byte[] input = "hello world".getBytes(UTF_8);
        LineBuffer lineBuffer = new LineBuffer(7, 100);
        assertBufferCopy(lineBuffer, input, input.length);

        Collections.reverse(Bytes.asList(input));
        assertBufferCopy(lineBuffer, input, input.length);
    }

    @Test
    public void testMaxLength()
            throws IOException
    {
        byte[] input = "hello world".getBytes(UTF_8);
        int maxLength = 100;
        LineBuffer lineBuffer = new LineBuffer(7, maxLength);
        int size = 0;
        while (size + input.length < maxLength) {
            lineBuffer.write(input, 0, input.length);
            size += input.length;
            assertThat(lineBuffer.getLength()).isEqualTo(size);
        }

        byte[] currentBuffer = lineBuffer.getBuffer();
        assertThat(currentBuffer).hasSizeLessThanOrEqualTo(maxLength);
        assertThatThrownBy(() -> lineBuffer.write(input, 0, input.length))
                .isInstanceOf(IOException.class);
        // buffer instance should not change
        assertThat(lineBuffer.getBuffer()).isSameAs(currentBuffer);
        // size should not change
        assertThat(lineBuffer.getLength()).isEqualTo(size);
    }

    @Test
    public void testExactMaxLength()
            throws IOException
    {
        byte[] input = new byte[256];
        for (int i = 0; i < input.length; i++) {
            input[i] = (byte) i;
        }

        LineBuffer lineBuffer = new LineBuffer(1, input.length);
        assertBufferCopy(lineBuffer, input, input.length);

        byte[] currentBuffer = lineBuffer.getBuffer();
        assertThat(currentBuffer.length).isEqualTo(input.length);

        assertThatThrownBy(() -> lineBuffer.write(new byte[1], 0, 1))
                .isInstanceOf(IOException.class);

        // buffer instance should not change
        assertThat(currentBuffer).isSameAs(currentBuffer);
        // size should not change
        assertThat(lineBuffer.getLength()).isEqualTo(input.length);
    }

    private static void assertBufferCopy(LineBuffer lineBuffer, byte[] input, int length)
            throws IOException
    {
        for (int copySize = 1; copySize <= length; copySize++) {
            lineBuffer.reset();
            assertLineBuffer(lineBuffer, input, 0);
            for (int i = 0; i < length; i += copySize) {
                int actualCopySize = min(copySize, input.length - lineBuffer.getLength());
                lineBuffer.write(input, i, actualCopySize);
                assertLineBuffer(lineBuffer, input, i + actualCopySize);
            }
        }
    }

    private static void assertLineBuffer(LineBuffer lineBuffer, byte[] input, int inputLength)
    {
        assertThat(lineBuffer.getBuffer()).hasSizeGreaterThanOrEqualTo(inputLength);
        assertThat(lineBuffer.getLength()).isEqualTo(inputLength);
        assertThat(lineBuffer.isEmpty()).isEqualTo(inputLength == 0);
        assertThat(Arrays.equals(lineBuffer.getBuffer(), 0, inputLength, input, 0, inputLength)).isTrue();
        assertThat(lineBuffer.getRetainedSize()).isEqualTo(LINE_BUFFER_INSTANCE_SIZE + sizeOf(lineBuffer.getBuffer()));
    }
}
