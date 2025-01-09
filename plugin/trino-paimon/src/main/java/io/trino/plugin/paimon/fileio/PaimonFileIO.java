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
package io.trino.plugin.paimon.fileio;

import io.trino.filesystem.FileEntry;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.security.ConnectorIdentity;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Trino file io for paimon.
 */
public class PaimonFileIO
        implements FileIO
{
    private final TrinoFileSystemFactory trinoFileSystemFactory;
    private final boolean objectStore;

    private TrinoFileSystem trinoFileSystem;
    private ConnectorSession session;

    public PaimonFileIO(
            TrinoFileSystemFactory trinoFileSystemFactory,
            ConnectorIdentity connectorIdentity,
            @Nullable Path path)
    {
        this.trinoFileSystemFactory = trinoFileSystemFactory;
        this.trinoFileSystem = trinoFileSystemFactory.create(connectorIdentity);
        this.objectStore = path == null || checkObjectStore(path.toUri().getScheme());
    }

    private static boolean checkObjectStore(String scheme)
    {
        scheme = scheme.toLowerCase(Locale.getDefault());
        if (!scheme.startsWith("s3")
                && !scheme.startsWith("emr")
                && !scheme.startsWith("oss")
                && !scheme.startsWith("wasb")) {
            return scheme.startsWith("http") || scheme.startsWith("ftp");
        }
        else {
            return true;
        }
    }

    public void setConnectorSession(ConnectorSession session)
    {
        if (!session.equals(this.session)) {
            this.session = session;
            trinoFileSystem = trinoFileSystemFactory.create(session);
        }
    }

    @Override
    public boolean isObjectStore()
    {
        return objectStore;
    }

    @Override
    public void configure(CatalogContext catalogContext) {}

    @Override
    public SeekableInputStream newInputStream(Path path)
            throws IOException
    {
        return new PaimonInputStreamWrapper(
                trinoFileSystem.newInputFile(Location.of(path.toString())).newStream());
    }

    @Override
    public PositionOutputStream newOutputStream(Path path, boolean overwrite)
            throws IOException
    {
        TrinoOutputFile trinoOutputFile =
                trinoFileSystem.newOutputFile(Location.of(path.toString()));

        try {
            return new PositionOutputStreamWrapper(trinoOutputFile.create());
        }
        catch (FileAlreadyExistsException e) {
            if (overwrite) {
                trinoFileSystem.deleteFile(Location.of(path.toString()));
                return new PositionOutputStreamWrapper(trinoOutputFile.create());
            }
            throw e;
        }
    }

    @Override
    public FileStatus getFileStatus(Path path)
            throws IOException
    {
        return status(path);
    }

    private FileStatus status(Path path)
            throws IOException
    {
        if (trinoFileSystem.directoryExists(Location.of(path.toString())).orElse(false)) {
            return new PaimonDirectoryFileStatus(path);
        }
        else {
            TrinoInputFile trinoInputFile =
                    trinoFileSystem.newInputFile(Location.of(path.toString()));
            return new PaimonFileStatus(
                    trinoInputFile.length(), path, trinoInputFile.lastModified().getEpochSecond());
        }
    }

    @Override
    public FileStatus[] listStatus(Path path)
            throws IOException
    {
        List<FileStatus> fileStatusList = new ArrayList<>();
        Location location = Location.of(path.toString());
        if (trinoFileSystem.directoryExists(location).orElse(false)) {
            FileIterator fileIterator = trinoFileSystem.listFiles(location);
            while (fileIterator.hasNext()) {
                FileEntry fileEntry = fileIterator.next();
                fileStatusList.add(
                        new PaimonFileStatus(
                                fileEntry.length(),
                                new Path(fileEntry.location().toString()),
                                fileEntry.lastModified().getEpochSecond()));
            }
            trinoFileSystem
                    .listDirectories(Location.of(path.toString()))
                    .forEach(
                            l ->
                                    fileStatusList.add(
                                            new PaimonDirectoryFileStatus(new Path(l.toString()))));
        }
        return fileStatusList.toArray(new FileStatus[0]);
    }

    @Override
    public FileStatus[] listDirectories(Path path)
            throws IOException
    {
        return trinoFileSystem.listDirectories(Location.of(path.toString())).stream()
                .map(l -> new PaimonDirectoryFileStatus(new Path(l.toString())))
                .toArray(FileStatus[]::new);
    }

    @Override
    public boolean exists(Path path)
            throws IOException
    {
        return trinoFileSystem.directoryExists(Location.of(path.toString())).orElse(false)
                || existFile(Location.of(path.toString()));
    }

    private boolean existFile(Location location)
            throws IOException
    {
        try {
            return trinoFileSystem.newInputFile(location).exists();
        }
        catch (IllegalArgumentException e) {
            return false;
        }
    }

    @Override
    public boolean delete(Path path, boolean recursive)
            throws IOException
    {
        Location location = Location.of(path.toString());
        if (trinoFileSystem.directoryExists(location).orElse(false)) {
            if (!recursive) {
                if (trinoFileSystem.listFiles(location).hasNext()) {
                    throw new IOException("Directory " + location + " is not empty");
                }
            }
            trinoFileSystem.deleteDirectory(location);
            return true;
        }
        else if (existFile(location)) {
            trinoFileSystem.deleteFile(location);
            return true;
        }

        return false;
    }

    @Override
    public boolean mkdirs(Path path)
            throws IOException
    {
        trinoFileSystem.createDirectory(Location.of(path.toString()));
        return true;
    }

    @Override
    public boolean rename(Path source, Path target)
            throws IOException
    {
        Location sourceLocation = Location.of(source.toString());
        Location targetLocation = Location.of(target.toString());
        if (trinoFileSystem.directoryExists(sourceLocation).orElse(false)) {
            trinoFileSystem.renameDirectory(sourceLocation, targetLocation);
        }
        else {
            trinoFileSystem.renameFile(sourceLocation, targetLocation);
        }
        return true;
    }
}
