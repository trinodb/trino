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

import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;

import java.io.IOException;
import java.util.Set;

public interface LineReaderFactory
{
    Set<String> getHiveInputFormatClassNames();

    LineBuffer createLineBuffer();

    LineReader createLineReader(
            TrinoInputFile inputFile,
            long start,
            long length,
            int headerCount,
            int footerCount)
            throws IOException;

    TrinoInputFile newInputFile(
            TrinoFileSystem trinoFileSystem,
            Location path,
            long estimatedFileSize,
            long fileModifiedTime);
}
