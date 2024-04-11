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
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.exception.AlluxioException;
import alluxio.grpc.CreateFilePOptions;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.memory.context.AggregatedMemoryContext;

import java.io.IOException;
import java.io.OutputStream;

import static java.util.Objects.requireNonNull;

public class AlluxioOutputFile
        implements TrinoOutputFile
{
    private final Location location;

    private final FileSystem fileSystem;

    public AlluxioOutputFile(Location location, FileSystem fileSystem)
    {
        this.location = requireNonNull(location, "location is null");
        this.fileSystem = requireNonNull(fileSystem, "fileSystem is null");
    }

    @Override
    public void createOrOverwrite(byte[] data)
            throws IOException
    {
        try (FileOutStream outStream =
                fileSystem.createFile(new AlluxioURI(location.toString()), CreateFilePOptions.newBuilder().setOverwrite(true).build())) {
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
        try {
            return fileSystem.createFile(new AlluxioURI(location.toString()));
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
}
