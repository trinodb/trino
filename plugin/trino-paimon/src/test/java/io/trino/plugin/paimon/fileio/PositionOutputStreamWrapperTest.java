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
package io.trino.plugin.paimon.fileio;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class PositionOutputStreamWrapperTest
{
    @Test
    public void testTracksPositionAfterSuccessfulWrites()
            throws IOException
    {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        PositionOutputStreamWrapper stream = new PositionOutputStreamWrapper(output, 7);

        stream.write(1);
        assertThat(stream.getPos()).isEqualTo(8);

        stream.write(new byte[] {2, 3, 4});
        assertThat(stream.getPos()).isEqualTo(11);

        stream.write(new byte[] {5, 6, 7, 8}, 1, 2);
        assertThat(stream.getPos()).isEqualTo(13);
        assertThat(output.toByteArray()).containsExactly(1, 2, 3, 4, 6, 7);
    }

    @Test
    public void testPositionDoesNotAdvanceWhenWriteFails()
            throws IOException
    {
        PositionOutputStreamWrapper stream = new PositionOutputStreamWrapper(new FailingOutputStream(), 11);

        assertThatThrownBy(() -> stream.write(1))
                .isInstanceOf(IOException.class)
                .hasMessage("write failed");
        assertThat(stream.getPos()).isEqualTo(11);

        assertThatThrownBy(() -> stream.write(new byte[] {1, 2, 3}))
                .isInstanceOf(IOException.class)
                .hasMessage("write failed");
        assertThat(stream.getPos()).isEqualTo(11);

        assertThatThrownBy(() -> stream.write(new byte[] {1, 2, 3}, 1, 1))
                .isInstanceOf(IOException.class)
                .hasMessage("write failed");
        assertThat(stream.getPos()).isEqualTo(11);
    }

    private static class FailingOutputStream
            extends OutputStream
    {
        @Override
        public void write(int b)
                throws IOException
        {
            throw new IOException("write failed");
        }

        @Override
        public void write(byte[] bytes, int offset, int length)
                throws IOException
        {
            throw new IOException("write failed");
        }
    }
}
