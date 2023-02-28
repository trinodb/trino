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
package io.trino.filesystem;

import java.io.IOException;
import java.util.Collection;

public interface TrinoFileSystem
{
    TrinoInputFile newInputFile(String location);

    TrinoInputFile newInputFile(String location, long length);

    TrinoOutputFile newOutputFile(String location);

    void deleteFile(String location)
            throws IOException;

    /**
     * Delete files in batches, possibly non-atomically.
     * If an error occurs, some files may have been deleted.
     */
    void deleteFiles(Collection<String> locations)
            throws IOException;

    void deleteDirectory(String location)
            throws IOException;

    void renameFile(String source, String target)
            throws IOException;

    FileIterator listFiles(String location)
            throws IOException;
}
