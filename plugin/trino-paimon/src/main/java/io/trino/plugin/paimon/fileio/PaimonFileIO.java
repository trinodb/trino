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
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoInputStream;
import io.trino.filesystem.TrinoOutputFile;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.FileAlreadyExistsException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class PaimonFileIO
        implements FileIO
{
    private static final ReentrantLock RENAME_LOCK = new ReentrantLock();

    private final boolean objectStore;

    private final TrinoFileSystem fileSystem;

    public PaimonFileIO(
            TrinoFileSystem fileSystem,
            @Nullable Path path)
    {
        this.fileSystem = requireNonNull(fileSystem, "fileSystem is null");
        this.objectStore = path == null || checkObjectStore(path.toUri().getScheme());
    }

    private static boolean checkObjectStore(String scheme)
    {
        scheme = scheme.toLowerCase(ENGLISH);
        if (!scheme.startsWith("s3")
                && !scheme.startsWith("emr")
                && !scheme.startsWith("oss")
                && !scheme.startsWith("wasb")) {
            return scheme.startsWith("http") || scheme.startsWith("ftp");
        }
        return true;
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
        return new PaimonInputStreamWrapper(fileSystem.newInputFile(Location.of(path.toString())).newStream());
    }

    @Override
    public PositionOutputStream newOutputStream(Path path, boolean overwrite)
            throws IOException
    {
        TrinoOutputFile outputFile = fileSystem.newOutputFile(Location.of(path.toString()));
        try {
            return new PositionOutputStreamWrapper(outputFile.create(), 0);
        }
        catch (FileAlreadyExistsException e) {
            if (overwrite) {
                fileSystem.deleteFile(Location.of(path.toString()));
                return new PositionOutputStreamWrapper(outputFile.create(), 0);
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
        if (fileSystem.directoryExists(Location.of(path.toString())).orElse(false)) {
            return new PaimonDirectoryFileStatus(path);
        }
        TrinoInputFile trinoInputFile = fileSystem.newInputFile(Location.of(path.toString()));
        return new PaimonFileStatus(trinoInputFile.length(), path, trinoInputFile.lastModified().getEpochSecond());
    }

    @Override
    public FileStatus[] listStatus(Path path)
            throws IOException
    {
        List<FileStatus> fileStatusList = new ArrayList<>();
        Location location = Location.of(path.toString());
        if (fileSystem.directoryExists(location).orElse(false)) {
            FileIterator fileIterator = fileSystem.listFiles(location);
            while (fileIterator.hasNext()) {
                FileEntry fileEntry = fileIterator.next();
                fileStatusList.add(
                        new PaimonFileStatus(
                                fileEntry.length(),
                                new Path(fileEntry.location().toString()),
                                fileEntry.lastModified().getEpochSecond()));
            }
            fileSystem.listDirectories(Location.of(path.toString()))
                    .forEach(directory -> fileStatusList.add(new PaimonDirectoryFileStatus(new Path(directory.toString()))));
        }
        return fileStatusList.toArray(new FileStatus[0]);
    }

    @Override
    public FileStatus[] listDirectories(Path path)
            throws IOException
    {
        return fileSystem.listDirectories(Location.of(path.toString())).stream()
                .map(location -> new PaimonDirectoryFileStatus(new Path(location.toString())))
                .toArray(FileStatus[]::new);
    }

    @Override
    public boolean exists(Path path)
            throws IOException
    {
        return fileSystem.directoryExists(Location.of(path.toString())).orElse(false)
                || existFile(Location.of(path.toString()));
    }

    private boolean existFile(Location location)
            throws IOException
    {
        try {
            return fileSystem.newInputFile(location).exists();
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
        if (fileSystem.directoryExists(location).orElse(false)) {
            if (!recursive) {
                if (fileSystem.listFiles(location).hasNext()) {
                    throw new IOException("Directory " + location + " is not empty");
                }
            }
            fileSystem.deleteDirectory(location);
            return true;
        }
        else if (existFile(location)) {
            fileSystem.deleteFile(location);
            return true;
        }

        return false;
    }

    @Override
    public boolean mkdirs(Path path)
            throws IOException
    {
        fileSystem.createDirectory(Location.of(path.toString()));
        return true;
    }

    @Override
    public boolean rename(Path source, Path target)
            throws IOException
    {
        boolean local = "file".equals(source.toUri().getScheme());
        try {
            if (local) {
                RENAME_LOCK.lock();
            }
            Location sourceLocation = Location.of(source.toString());
            Location targetLocation = Location.of(target.toString());
            if (fileSystem.directoryExists(sourceLocation).orElse(false)) {
                fileSystem.renameDirectory(sourceLocation, targetLocation);
            }
            else {
                renameFileAnyway(sourceLocation, targetLocation);
            }
        }
        catch (IOException e) {
            if (e.getMessage().contains("Target location already exists") || e.getMessage().contains("rename failed")) {
                return false;
            }
            throw e;
        }
        finally {
            if (local) {
                RENAME_LOCK.unlock();
            }
        }
        return true;
    }

    private void renameFileAnyway(Location source, Location target)
            throws IOException
    {
        try {
            fileSystem.renameFile(source, target);
        }
        catch (IOException e) {
            try (TrinoInputStream input = fileSystem.newInputFile(source).newStream();
                    OutputStream outputStream = fileSystem.newOutputFile(target).create()) {
                byte[] btyes = new byte[1024 * 8];
                int len;
                while ((len = input.read(btyes)) != -1) {
                    outputStream.write(btyes, 0, len);
                }
            }
            fileSystem.deleteFile(source);
        }
    }
}
