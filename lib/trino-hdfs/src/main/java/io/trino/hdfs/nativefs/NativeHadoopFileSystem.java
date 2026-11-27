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
package io.trino.hdfs.nativefs;

import io.trino.filesystem.FileEntry;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.security.ConnectorIdentity;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Progressable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Hadoop {@link FileSystem} backed by a native Trino {@link TrinoFileSystem}.
 * <p>
 * Hadoop file systems that resolve child file systems inside the Hadoop client, such as ViewFS
 * mount links to {@code s3a://}, instantiate the child by scheme using {@code fs.<scheme>.impl}.
 * {@link NativeFileSystemConfigurationInitializer} registers this class for the schemes that
 * have a native Trino file system, and the native file system factory is looked up in
 * {@link NativeFileSystemRegistry} using the id stored in the {@link Configuration}.
 */
public class NativeHadoopFileSystem
        extends FileSystem
{
    public static final String REGISTRY_ID_KEY = "trino.native-file-system.registry-id";

    // Native file systems do not expose a block size; report a conventional value
    private static final long BLOCK_SIZE = 64 * 1024 * 1024;

    private URI uri;
    private Path workingDirectory;
    private ClassLoader nativeClassLoader;
    private TrinoFileSystem fileSystem;

    @Override
    public void initialize(URI name, Configuration conf)
            throws IOException
    {
        super.initialize(name, conf);
        setConf(conf);
        // Locations without an authority, such as memory:///path, are rooted at scheme:///
        uri = name.getAuthority() == null ? URI.create(name.getScheme() + ":///") : URI.create(name.getScheme() + "://" + name.getAuthority());
        workingDirectory = new Path(uri.getAuthority() == null ? uri.toString() : uri + "/");

        String registryId = conf.get(REGISTRY_ID_KEY);
        if (registryId == null) {
            throw new IOException("Native file system is not configured: %s is not set".formatted(REGISTRY_ID_KEY));
        }
        TrinoFileSystemFactory factory = NativeFileSystemRegistry.get(registryId, name.getScheme())
                .orElseThrow(() -> new IOException("No native file system registered for scheme: " + name.getScheme()));
        // Hadoop calls run with the HDFS class loader as context class loader, which cannot see the classes of
        // the native file system, such as the AWS SDK. Native calls switch to the class loader of the factory.
        nativeClassLoader = factory.getClass().getClassLoader();
        String user = UserGroupInformation.getCurrentUser().getShortUserName();
        try (ThreadContextClassLoader _ = new ThreadContextClassLoader(nativeClassLoader)) {
            fileSystem = factory.create(ConnectorIdentity.ofUser(user));
        }
    }

    @Override
    public String getScheme()
    {
        return uri.getScheme();
    }

    @Override
    public URI getUri()
    {
        return uri;
    }

    @Override
    public FSDataInputStream open(Path path, int bufferSize)
            throws IOException
    {
        try (ThreadContextClassLoader _ = new ThreadContextClassLoader(nativeClassLoader)) {
            TrinoInputFile inputFile = fileSystem.newInputFile(location(path));
            return new FSDataInputStream(new NativeHadoopInputStream(inputFile.newStream(), nativeClassLoader));
        }
    }

    @Override
    public FSDataOutputStream create(Path path, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress)
            throws IOException
    {
        try (ThreadContextClassLoader _ = new ThreadContextClassLoader(nativeClassLoader)) {
            Location location = location(path);
            if (!overwrite && isFile(location)) {
                throw new FileAlreadyExistsException("File already exists: " + path);
            }
            TrinoOutputFile outputFile = fileSystem.newOutputFile(location);
            try {
                return new FSDataOutputStream(new NativeHadoopOutputStream(outputFile.create(), nativeClassLoader), statistics);
            }
            catch (java.nio.file.FileAlreadyExistsException e) {
                // Some file systems fail instead of overwriting an existing file on create; delete it and retry,
                // so the delete is only paid for when the file exists
                if (!overwrite) {
                    throw new FileAlreadyExistsException("File already exists: " + path);
                }
                fileSystem.deleteFile(location);
                return new FSDataOutputStream(new NativeHadoopOutputStream(outputFile.create(), nativeClassLoader), statistics);
            }
        }
    }

    @Override
    public FSDataOutputStream append(Path path, int bufferSize, Progressable progress)
            throws IOException
    {
        throw new IOException("Append is not supported: " + path);
    }

    @Override
    public boolean rename(Path source, Path target)
            throws IOException
    {
        try (ThreadContextClassLoader _ = new ThreadContextClassLoader(nativeClassLoader)) {
            Location sourceLocation = location(source);
            Location targetLocation = location(target);
            if (isFile(sourceLocation)) {
                fileSystem.renameFile(sourceLocation, targetLocation);
                return true;
            }
            if (isDirectory(sourceLocation)) {
                fileSystem.renameDirectory(sourceLocation, targetLocation);
                return true;
            }
            // Hadoop contract: renaming a missing path returns false
            return false;
        }
    }

    @Override
    public boolean delete(Path path, boolean recursive)
            throws IOException
    {
        try (ThreadContextClassLoader _ = new ThreadContextClassLoader(nativeClassLoader)) {
            Location location = location(path);
            if (location.path().isEmpty()) {
                throw new IOException("Cannot delete the root of the file system: " + path);
            }
            if (isFile(location)) {
                fileSystem.deleteFile(location);
                return true;
            }
            boolean hasChildren = hasChildren(location);
            if (!hasChildren && !isDirectory(location)) {
                // Hadoop contract: deleting a missing path returns false
                return false;
            }
            if (!recursive && hasChildren) {
                throw new IOException("Directory is not empty: " + path);
            }
            fileSystem.deleteDirectory(location);
            return true;
        }
    }

    @Override
    public FileStatus[] listStatus(Path path)
            throws IOException
    {
        try (ThreadContextClassLoader _ = new ThreadContextClassLoader(nativeClassLoader)) {
            Location location = location(path);

            List<FileStatus> statuses = new ArrayList<>();
            for (Location directory : fileSystem.listDirectories(location)) {
                statuses.add(directoryStatus(directory));
            }
            // Native file systems list files recursively, keep only the files directly within the directory
            String directoryPath = withoutTrailingSlash(location.path());
            FileIterator files = fileSystem.listFiles(location);
            while (files.hasNext()) {
                FileEntry file = files.next();
                if (withoutTrailingSlash(file.location().parentDirectory().path()).equals(directoryPath)) {
                    statuses.add(fileStatus(file.location(), file.length(), file.lastModified()));
                }
            }
            if (!statuses.isEmpty()) {
                return statuses.toArray(FileStatus[]::new);
            }

            // Nothing below the location: it is a file, an empty directory, or missing
            Optional<FileStatus> file = existingFileStatus(location);
            if (file.isPresent()) {
                return new FileStatus[] {file.get()};
            }
            // Object stores do not distinguish a missing directory from an empty one, for example a
            // directory represented only by a "dir/" marker object, which the listing of its parent reports
            return new FileStatus[0];
        }
    }

    @Override
    public void setWorkingDirectory(Path path)
    {
        workingDirectory = path;
    }

    @Override
    public Path getWorkingDirectory()
    {
        return workingDirectory;
    }

    @Override
    public boolean mkdirs(Path path, FsPermission permission)
            throws IOException
    {
        try (ThreadContextClassLoader _ = new ThreadContextClassLoader(nativeClassLoader)) {
            fileSystem.createDirectory(location(path));
            return true;
        }
    }

    @Override
    public FileStatus getFileStatus(Path path)
            throws IOException
    {
        try (ThreadContextClassLoader _ = new ThreadContextClassLoader(nativeClassLoader)) {
            Location location = location(path);
            Optional<FileStatus> file = existingFileStatus(location);
            if (file.isPresent()) {
                return file.get();
            }
            if (isDirectory(location)) {
                return directoryStatus(location);
            }
            throw new FileNotFoundException("File does not exist: " + path);
        }
    }

    private Optional<FileStatus> existingFileStatus(Location location)
            throws IOException
    {
        if (location.path().isEmpty()) {
            return Optional.empty();
        }
        // A single input file, so that S3 serves the existence check, length and modification time with one request
        TrinoInputFile inputFile = fileSystem.newInputFile(location);
        if (!inputFile.exists()) {
            return Optional.empty();
        }
        return Optional.of(fileStatus(location, inputFile.length(), inputFile.lastModified()));
    }

    private boolean isFile(Location location)
            throws IOException
    {
        return !location.path().isEmpty() && fileSystem.newInputFile(location).exists();
    }

    private boolean hasChildren(Location location)
            throws IOException
    {
        return fileSystem.listFiles(location).hasNext() || !fileSystem.listDirectories(location).isEmpty();
    }

    private boolean isDirectory(Location location)
            throws IOException
    {
        // Object stores report an empty optional for prefixes without objects, treat them as missing
        return location.path().isEmpty() || fileSystem.directoryExists(location).orElse(false);
    }

    private Location location(Path path)
    {
        return Location.of(makeQualified(path).toString());
    }

    private static String withoutTrailingSlash(String path)
    {
        return path.endsWith("/") ? path.substring(0, path.length() - 1) : path;
    }

    private static Path toPath(Location location)
    {
        return new Path(location.toString());
    }

    private static FileStatus fileStatus(Location location, long length, Instant lastModified)
    {
        return new FileStatus(length, false, 1, BLOCK_SIZE, lastModified.toEpochMilli(), toPath(location));
    }

    private static FileStatus directoryStatus(Location location)
    {
        return new FileStatus(0, true, 1, 0, 0, toPath(location));
    }
}
