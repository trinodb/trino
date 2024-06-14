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

import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.exception.AlluxioException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.runtime.NotFoundRuntimeException;
import alluxio.grpc.OpenFilePOptions;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoInputStream;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.Instant;

import static io.trino.filesystem.alluxio.AlluxioUtils.convertToAlluxioURI;
import static java.util.Objects.requireNonNull;

public class AlluxioFileSystemInputFile
        implements TrinoInputFile
{
    private final Location location;

    private final FileSystem fileSystem;

    private final String mountRoot;

    private Long length;

    private URIStatus status;

    public AlluxioFileSystemInputFile(Location location, Long length, FileSystem fileSystem, String mountRoot)
    {
        this.location = requireNonNull(location, "location is null");
        this.fileSystem = requireNonNull(fileSystem, "fileSystem is null");
        this.mountRoot = requireNonNull(mountRoot, "mountRoot is null");
        this.length = length;
    }

    @Override
    public TrinoInput newInput()
            throws IOException
    {
        try {
            return new AlluxioFileSystemInput(openFile(), this);
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
            return new AlluxioTrinoInputStream(location, openFile(), getStatus());
        }
        catch (AlluxioException e) {
            throw new IOException("Error newStream() file: %s".formatted(location), e);
        }
    }

    private FileInStream openFile()
            throws IOException, AlluxioException
    {
        if (!exists()) {
            throw new FileNotFoundException("File does not exist: " + location);
        }
        return fileSystem.openFile(getStatus(), OpenFilePOptions.getDefaultInstance());
    }

    private URIStatus getStatus(boolean lazy)
            throws IOException
    {
        if (lazy) {
            if (status == null) {
                getStatus();
            }
            return status;
        }
        return getStatus();
    }

    private URIStatus getStatus()
            throws IOException
    {
        try {
            //TODO: create a URIStatus object based on the location field
            status = fileSystem.getStatus(convertToAlluxioURI(location, mountRoot));
        }
        catch (FileDoesNotExistException | NotFoundRuntimeException e) {
            return null;
        }
        catch (AlluxioException | IOException e) {
            throw new IOException("Get status for file %s failed: %s".formatted(location, e.getMessage()), e);
        }
        return status;
    }

    @Override
    public long length()
            throws IOException
    {
        if (length == null) {
            URIStatus status = getStatus(true);
            if (status == null) {
                throw new FileNotFoundException("File does not exist: %s".formatted(location));
            }
            length = status.getLength();
        }
        return length;
    }

    @Override
    public Instant lastModified()
            throws IOException
    {
        URIStatus status = getStatus(true);
        if (status == null) {
            throw new FileNotFoundException("File does not exist: %s".formatted(location));
        }
        return Instant.ofEpochMilli(status.getLastModificationTimeMs());
    }

    @Override
    public boolean exists()
            throws IOException
    {
        URIStatus status = getStatus();
        if (status == null || !status.isCompleted()) {
            return false;
        }
        return true;
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
