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
package io.trino.hive.formats.line.sequence;

import com.google.common.collect.ImmutableMap;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.memory.MemoryInputFile;
import io.trino.hive.formats.line.LineBuffer;
import io.trino.hive.formats.line.LineReader;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestSequenceFileWriterFactory
{
    @Test
    public void testHeaderFooterConstraints()
            throws Exception
    {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (SequenceFileWriter writer = new SequenceFileWriter(
                out,
                Optional.empty(),
                false,
                ImmutableMap.of())) {
            writer.write(utf8Slice("header"));
            for (int i = 0; i < 1000; i++) {
                writer.write(utf8Slice("data " + i));
            }
        }
        TrinoInputFile file = new MemoryInputFile(Location.of("memory:///test"), wrappedBuffer(out.toByteArray()));

        SequenceFileReaderFactory readerFactory = new SequenceFileReaderFactory(1024, 8096);
        assertThatThrownBy(() -> readerFactory.createLineReader(file, 1, 7, 2, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageMatching("file cannot be split.* header.*");

        assertThatThrownBy(() -> readerFactory.createLineReader(file, 1, 7, 0, 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageMatching("file cannot be split.* footer.*");

        // single header allowed in split file
        LineBuffer lineBuffer = new LineBuffer(1, 20);
        LineReader lineReader = readerFactory.createLineReader(file, 0, 2, 1, 0);
        int count = 0;
        while (lineReader.readLine(lineBuffer)) {
            assertThat(new String(lineBuffer.getBuffer(), 0, lineBuffer.getLength(), StandardCharsets.UTF_8)).isEqualTo("data " + count);
            count++;
        }
        // The value here was obtained experimentally, but should be stable because the sequence file code is deterministic.
        // The exact number of lines is not important, but it should be more than 1.
        assertThat(count).isEqualTo(487);

        lineReader = readerFactory.createLineReader(file, 2, file.length() - 2, 1, 0);
        while (lineReader.readLine(lineBuffer)) {
            assertThat(new String(lineBuffer.getBuffer(), 0, lineBuffer.getLength(), StandardCharsets.UTF_8)).isEqualTo("data " + count);
            count++;
        }
        assertThat(count).isEqualTo(1000);
    }
}
