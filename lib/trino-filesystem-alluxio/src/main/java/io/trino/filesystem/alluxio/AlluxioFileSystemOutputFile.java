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

import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.exception.AlluxioException;
import alluxio.grpc.CreateFilePOptions;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.memory.context.AggregatedMemoryContext;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.FileAlreadyExistsException;

import static io.trino.filesystem.alluxio.AlluxioUtils.convertToAlluxioURI;
import static java.util.Objects.requireNonNull;

public class AlluxioFileSystemOutputFile
        implements TrinoOutputFile
{
    private final Location rootLocation;
    private final Location location;
    private final FileSystem fileSystem;
    private final String mountRoot;

    public AlluxioFileSystemOutputFile(Location rootLocation, Location location, FileSystem fileSystem, String mountRoot)
    {
        this.rootLocation = requireNonNull(rootLocation, "root location is null");
        this.location = requireNonNull(location, "location is null");
        this.fileSystem = requireNonNull(fileSystem, "fileSystem is null");
        this.mountRoot = requireNonNull(mountRoot, "mountRoot is null");
    }

    @Override
    public void createOrOverwrite(byte[] data)
            throws IOException
    {
        ensureOutputFileNotOutsideOfRoot(location);
        try (FileOutStream outStream = fileSystem.createFile(
                convertToAlluxioURI(location, mountRoot),
                CreateFilePOptions.newBuilder().setOverwrite(true).setRecursive(true).build())) {
            outStream.write(data);
        }
        catch (AlluxioException e) {
            throw new IOException("Error createOrOverwrite %s".formatted(location), e);
        }
    }

    @Override
    public OutputStream create(AggregatedMemoryContext memoryContext)
            throws IOException
    {
        throwIfAlreadyExists();
        try {
            return new AlluxioTrinoOutputStream(
                    location,
                    fileSystem.createFile(
                            convertToAlluxioURI(location, mountRoot),
                            CreateFilePOptions.newBuilder().setRecursive(true).build()));
        }
        catch (AlluxioException e) {
            throw new IOException("Error create %s".formatted(location), e);
        }
    }

    @Override
    public Location location()
    {
        return location;
    }

    @Override
    public String toString()
    {
        return location().toString();
    }

    private void ensureOutputFileNotOutsideOfRoot(Location location)
            throws IOException
    {
        String path = AlluxioUtils.simplifyPath(location.path());
        if (rootLocation != null && !path.startsWith(rootLocation.path())) {
            throw new IOException("Output file %s outside of root is not allowed".formatted(location));
        }
    }

    private void throwIfAlreadyExists()
            throws IOException
    {
        try {
            if (fileSystem.exists(convertToAlluxioURI(location, mountRoot))) {
                throw new FileAlreadyExistsException("File %s already exists".formatted(location));
            }
        }
        catch (AlluxioException e) {
            throw new IOException("Error create %s".formatted(location), e);
        }
    }
}
