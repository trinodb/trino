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
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoInputStream;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.spi.security.ConnectorIdentity;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public abstract class CloudStorageService
        implements CloudStorage
{
    protected final TrinoFileSystem fileSystem;

    protected CloudStorageService(TrinoFileSystemFactory fileSystemFactory)
    {
        this.fileSystem = requireNonNull(fileSystemFactory).create(ConnectorIdentity.ofUser("varada"));
    }

    @Override
    public TrinoInputFile newInputFile(Location location)
    {
        return fileSystem.newInputFile(location);
    }

    @Override
    public TrinoOutputFile newOutputFile(Location location)
    {
        return fileSystem.newOutputFile(location);
    }

    @Override
    public void copyFileReplaceTail(Location source, Location destination, long position, byte[] tailBuffer)
            throws IOException
    {
        TrinoInputFile inputFile = newInputFile(source);
        TrinoOutputFile outputFile = newOutputFile(destination);
        fileSystem.deleteFile(destination);
        try (TrinoInputStream inputStream = inputFile.newStream();
                OutputStream outputStream = outputFile.create()) {
            long offset = 0;
            byte[] bytes = new byte[8192 * 100]; // PageSize * 100
            int length;

            while ((offset < position) && ((length = inputStream.read(bytes)) > 0)) {
                int len = ((position - offset) >= length) ? length : (int) (position - offset);
                outputStream.write(bytes, 0, len);
                offset += len;
            }
            outputStream.write(tailBuffer);
        }
    }

    @Override
    public void deleteFile(Location location)
            throws IOException
    {
        fileSystem.deleteFile(location);
    }

    @Override
    public void renameFile(Location source, Location target)
            throws IOException
    {
        fileSystem.renameFile(source, target);
    }

    @Override
    public FileIterator listFiles(Location location)
            throws IOException
    {
        return fileSystem.listFiles(location);
    }

    @Override
    public Optional<Boolean> directoryExists(Location location)
            throws IOException
    {
        return fileSystem.directoryExists(location);
    }

    @Override
    public Set<Location> listDirectories(Location location)
            throws IOException
    {
        return fileSystem.listDirectories(location);
    }
}
