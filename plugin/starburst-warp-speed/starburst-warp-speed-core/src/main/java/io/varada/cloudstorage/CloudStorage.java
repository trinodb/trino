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
package io.varada.cloudstorage;

import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoOutputFile;

import java.io.IOException;
import java.util.Optional;
import java.util.Set;

public interface CloudStorage
{
    TrinoInputFile newInputFile(Location location);

    TrinoOutputFile newOutputFile(Location location);

    void uploadFile(Location source, Location target)
            throws IOException;

    void downloadFile(Location source, Location target)
            throws IOException;

    void copyFile(Location source, Location destination)
            throws IOException;

    void copyFileReplaceTail(Location source, Location destination, long position, byte[] tailBuffer)
            throws IOException;

    void deleteFile(Location location)
            throws IOException;

    void renameFile(Location source, Location target)
            throws IOException;

    FileIterator listFiles(Location location)
            throws IOException;

    Optional<Boolean> directoryExists(Location location)
            throws IOException;

    Set<Location> listDirectories(Location location)
            throws IOException;
}
