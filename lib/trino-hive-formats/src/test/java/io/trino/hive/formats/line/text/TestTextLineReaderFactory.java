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
package io.trino.hive.formats.line.text;

import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.memory.MemoryInputFile;
import io.trino.hive.formats.line.LineBuffer;
import io.trino.hive.formats.line.LineReader;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;

import static io.airlift.slice.Slices.utf8Slice;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestTextLineReaderFactory
{
    @Test
    public void testHeaderFooterConstraints()
            throws Exception
    {
        TextLineReaderFactory readerFactory = new TextLineReaderFactory(1024, 1024, 8096);
        TrinoInputFile file = new MemoryInputFile(Location.of("memory:///test"), utf8Slice("header\ndata"));

        assertThatThrownBy(() -> readerFactory.createLineReader(file, 1, 7, 2, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageMatching("file cannot be split.* header.*");

        assertThatThrownBy(() -> readerFactory.createLineReader(file, 1, 7, 0, 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageMatching("file cannot be split.* footer.*");

        // single header allowed in split file
        LineBuffer lineBuffer = new LineBuffer(1, 20);
        LineReader lineReader = readerFactory.createLineReader(file, 0, 2, 1, 0);
        assertFalse(lineReader.readLine(lineBuffer));

        lineReader = readerFactory.createLineReader(file, 2, file.length() - 2, 1, 0);
        assertTrue(lineReader.readLine(lineBuffer));
        assertThat(new String(lineBuffer.getBuffer(), 0, lineBuffer.getLength(), StandardCharsets.UTF_8)).isEqualTo("data");
    }
}
