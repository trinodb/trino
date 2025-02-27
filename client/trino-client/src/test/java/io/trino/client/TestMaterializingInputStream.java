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
package io.trino.client;

import io.airlift.units.DataSize;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

class TestMaterializingInputStream
{
    @Test
    void testHeadBufferOverflow()
            throws IOException
    {
        InputStream stream = new ByteArrayInputStream("abcd".repeat(1337).getBytes(UTF_8));
        MaterializingInputStream reader = new MaterializingInputStream(stream, DataSize.ofBytes(4));

        int remainingBytes = 4 * 1337 - 4;

        reader.transferTo(new ByteArrayOutputStream()); // Trigger reading
        assertThat(reader.getHeadString())
                .isEqualTo("abcd... [" + remainingBytes + " more bytes]");
    }

    @Test
    void testHeadBufferNotFullyUsed()
            throws IOException
    {
        InputStream stream = new ByteArrayInputStream("abcdabc".getBytes(UTF_8));
        MaterializingInputStream reader = new MaterializingInputStream(stream, DataSize.ofBytes(8));

        reader.transferTo(new ByteArrayOutputStream()); // Trigger reading
        assertThat(reader.getHeadString()).isEqualTo("abcdabc");
    }

    @Test
    void testHeadBufferFullyUsed()
            throws IOException
    {
        InputStream stream = new ByteArrayInputStream("a".repeat(8).getBytes(UTF_8));
        MaterializingInputStream reader = new MaterializingInputStream(stream, DataSize.ofBytes(8));

        reader.transferTo(new ByteArrayOutputStream()); // Trigger reading
        assertThat(reader.getHeadString()).isEqualTo("a".repeat(8));
    }
}
