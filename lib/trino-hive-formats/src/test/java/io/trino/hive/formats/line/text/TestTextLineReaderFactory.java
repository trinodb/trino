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
import org.testng.annotations.Test;

import static io.airlift.slice.Slices.wrappedBuffer;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestTextLineReaderFactory
{
    @Test
    public void testHeaderFooterConstraints()
            throws Exception
    {
        TextLineReaderFactory readerFactory = new TextLineReaderFactory(1024, 1024, 8096);
        TrinoInputFile file = new MemoryInputFile(Location.of("memory:///test"), wrappedBuffer(new byte[10]));

        assertThatThrownBy(() -> readerFactory.createLineReader(file, 1, 7, 2, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageMatching("file cannot be split.* header.*");

        assertThatThrownBy(() -> readerFactory.createLineReader(file, 1, 7, 0, 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageMatching("file cannot be split.* footer.*");

        // single header allowed in split file
        readerFactory.createLineReader(file, 0, 7, 1, 0);
        readerFactory.createLineReader(file, 2, 7, 1, 0);
    }
}
