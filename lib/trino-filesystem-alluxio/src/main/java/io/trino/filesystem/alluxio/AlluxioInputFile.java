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
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.exception.AlluxioException;
import alluxio.grpc.OpenFilePOptions;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoInputStream;

import java.io.IOException;
import java.time.Instant;

import static java.util.Objects.requireNonNull;

public class AlluxioInputFile
        implements TrinoInputFile
{
    private final Location location;

    private final FileSystem fileSystem;

    private Long length;

    private URIStatus status;

    public AlluxioInputFile(Location location, Long length, FileSystem fileSystem)
    {
        this.location = requireNonNull(location, "location is null");
        this.fileSystem = requireNonNull(fileSystem, "fileSystem is null");
        this.length = length;
    }

    @Override
    public TrinoInput newInput()
            throws IOException
    {
        try {
            return new AlluxioInput(openFile(), this);
        }
        catch (AlluxioException e) {
            throw new IOException("Error newInput() file: %s".formatted(location), e);
        }
    }

    @Override
    public TrinoInputStream newStream()
            throws IOException
    {
        try {
            AlluxioTrinoInputStream alluxioTrinoInputStream = new AlluxioTrinoInputStream(location, openFile());
            return alluxioTrinoInputStream;
        }
        catch (AlluxioException e) {
            throw new IOException("Error newStream() file: %s".formatted(location), e);
        }
    }

    private FileInStream openFile()
            throws IOException, AlluxioException
    {
        FileInStream fileInStream = fileSystem.openFile(lazyStatus(), OpenFilePOptions.getDefaultInstance());
        return fileInStream;
    }

    private URIStatus lazyStatus()
            throws IOException
    {
        if (status == null) {
            try {
                //TODO: create a URIStatus object based on the location field
                status = fileSystem.getStatus(new AlluxioURI(location.toString()));
            }
            catch (AlluxioException | IOException e) {
                throw new IOException("Get status for file %s failed: %s".formatted(location, e.getMessage()), e);
            }
        }
        return status;
    }

    @Override
    public long length()
            throws IOException
    {
        if (length == null) {
            length = lazyStatus().getLength();
        }
        return length;
    }

    @Override
    public Instant lastModified()
            throws IOException
    {
        return Instant.ofEpochMilli(lazyStatus().getLastModificationTimeMs());
    }

    @Override
    public boolean exists()
            throws IOException
    {
        try {
            //TODO: check if this way builds a correct AlluxioURI argument
            URIStatus status = lazyStatus();
            AlluxioURI alluxioURI = new AlluxioURI(status.getPath());
            return fileSystem.exists(alluxioURI);
        }
        catch (AlluxioException e) {
            throw new IOException("Error exists() file: %s".formatted(location), e);
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
