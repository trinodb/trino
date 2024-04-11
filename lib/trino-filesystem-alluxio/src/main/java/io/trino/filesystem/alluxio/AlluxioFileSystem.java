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
package io.trino.filesystem.alluxio;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.exception.AlluxioException;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoOutputFile;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Optional;
import java.util.Set;

public class AlluxioFileSystem
        implements TrinoFileSystem
{
    private final FileSystem fileSystem;

    public AlluxioFileSystem(FileSystem fileSystem)
    {
        this.fileSystem = fileSystem;
    }

    @Override
    public TrinoInputFile newInputFile(Location location)
    {
        return new AlluxioInputFile(location, null, fileSystem);
    }

    @Override
    public TrinoInputFile newInputFile(Location location, long length)
    {
        return new AlluxioInputFile(location, length, fileSystem);
    }

    @Override
    public TrinoOutputFile newOutputFile(Location location)
    {
        return new AlluxioOutputFile(location, fileSystem);
    }

    @Override
    public void deleteFile(Location location)
            throws IOException
    {
        try {
            fileSystem.delete(getAlluxioURI(location));
        }
        catch (AlluxioException e) {
            throw new IOException("Error deleteFile %s".formatted(location), e);
        }
    }

    @Override
    public void deleteDirectory(Location location)
            throws IOException
    {
        try {
            fileSystem.delete(getAlluxioURI(location));
        }
        catch (AlluxioException e) {
            throw new IOException("Error deleteDirectory %s".formatted(location), e);
        }
    }

    @Override
    public void renameFile(Location source, Location target)
            throws IOException
    {
        try {
            fileSystem.rename(getAlluxioURI(source), getAlluxioURI(target));
        }
        catch (AlluxioException e) {
            throw new IOException("Error renameFile from %s to %s".formatted(source, target), e);
        }
    }

    @Override
    public FileIterator listFiles(Location location)
            throws IOException
    {
        //TODO: create a new class AlluxioFileIterator to implement this method
        return null;
    }

    @Override
    public Optional<Boolean> directoryExists(Location location)
            throws IOException
    {
        if (location.path().isEmpty()) {
            return Optional.of(true);
        }
        try {
            URIStatus status = fileSystem.getStatus(getAlluxioURI(location));
            if (status != null && status.isFolder()) {
                return Optional.of(true);
            }
            return Optional.of(false);
        }
        catch (FileNotFoundException e) {
            return Optional.of(false);
        }
        catch (AlluxioException e) {
            throw new IOException("Error directoryExists %s".formatted(location), e);
        }
    }

    @Override
    public void createDirectory(Location location)
            throws IOException
    {
        try {
            fileSystem.createDirectory(getAlluxioURI(location));
        }
        catch (AlluxioException e) {
            throw new IOException("Error createDirectory %s".formatted(location), e);
        }
    }

    @Override
    public void renameDirectory(Location source, Location target)
            throws IOException
    {
        try {
            fileSystem.rename(getAlluxioURI(source), getAlluxioURI(target));
        }
        catch (AlluxioException e) {
            throw new IOException("Error renameDirectory from %s to %s".formatted(source, target), e);
        }
    }

    @Override
    public Set<Location> listDirectories(Location location)
            throws IOException
    {
        //TODO: implement this method
        throw new UnsupportedOperationException(
                "listDirectories(Location location) is not supported");
    }

    @Override
    public Optional<Location> createTemporaryDirectory(Location targetPath, String temporaryPrefix, String relativePrefix)
            throws IOException
    {
        //TODO: implement this method
        throw new UnsupportedOperationException(
                "createTemporaryDirectory(Location targetPath, String temporaryPrefix, String relativePrefix) is not supported");
    }

    private AlluxioURI getAlluxioURI(Location location)
    {
        //TODO: check if this way builds a correct AlluxioURI
        return new AlluxioURI(location.toString());
    }
}
