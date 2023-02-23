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
    TrinoInputFile newInputFile(String path);

    TrinoInputFile newInputFile(String path, long length);

    TrinoOutputFile newOutputFile(String path);

    void deleteFile(String path)
            throws IOException;

    /**
     * Delete paths in batches, it is not guaranteed to be atomic.
     *
     * @param paths collection of paths to be deleted
     * @throws IOException when there is a problem with deletion of one or more specific paths
     */
    void deleteFiles(Collection<String> paths)
            throws IOException;

    void deleteDirectory(String path)
            throws IOException;

    void renameFile(String source, String target)
            throws IOException;

    FileIterator listFiles(String path)
            throws IOException;
}
