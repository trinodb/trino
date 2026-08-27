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
import io.trino.filesystem.TrinoOutputFile;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.TwoPhaseOutputStream;
import org.apache.paimon.utils.FileIOUtils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class PaimonFileIO
        implements FileIO
{
    private static final String DIRECTORY_MARKER_FILE_NAME = "_trino_paimon_directory_marker";

    private final TrinoFileSystem trinoFileSystem;
    private final boolean objectStore;

    public PaimonFileIO(TrinoFileSystem trinoFileSystem, Path path)
    {
        this.trinoFileSystem = requireNonNull(trinoFileSystem, "trinoFileSystem is null");
        this.objectStore = checkObjectStore(requireNonNull(path, "path is null").toUri().getScheme());
    }

    private static boolean checkObjectStore(String scheme)
    {
        if (scheme == null) {
            return false;
        }
        return FileIOUtils.isObjectStore(scheme.toLowerCase(Locale.ENGLISH));
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
        return new PaimonInputStreamWrapper(trinoFileSystem.newInputFile(Location.of(path.toString())).newStream());
    }

    @Override
    public PositionOutputStream newOutputStream(Path path, boolean overwrite)
            throws IOException
    {
        Location location = Location.of(path.toString());
        TrinoOutputFile trinoOutputFile = trinoFileSystem.newOutputFile(location);

        if (objectStore) {
            if (!overwrite && existFile(location)) {
                throw new FileAlreadyExistsException(path.toString());
            }
            try {
                return new PositionOutputStreamWrapper(trinoOutputFile.create());
            }
            catch (FileAlreadyExistsException e) {
                if (overwrite) {
                    return new ObjectStoreOverwriteOutputStream(trinoOutputFile);
                }
                throw e;
            }
        }

        try {
            return new PositionOutputStreamWrapper(trinoOutputFile.create());
        }
        catch (FileAlreadyExistsException e) {
            if (overwrite) {
                trinoFileSystem.deleteFile(location);
                return new PositionOutputStreamWrapper(trinoOutputFile.create());
            }
            throw e;
        }
    }

    @Override
    public TwoPhaseOutputStream newTwoPhaseOutputStream(Path path, boolean overwrite)
            throws IOException
    {
        if (!objectStore) {
            return FileIO.super.newTwoPhaseOutputStream(path, overwrite);
        }

        Location location = Location.of(path.toString());
        if (existFile(location)) {
            if (!overwrite) {
                throw new FileAlreadyExistsException(path.toString());
            }
            throw new IOException("Object-store two-phase overwrite is not supported for existing file: " + path);
        }

        return new DirectObjectStoreTwoPhaseOutputStream(
                path,
                location,
                trinoFileSystem,
                new PositionOutputStreamWrapper(trinoFileSystem.newOutputFile(location).create()));
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
        Location location = Location.of(path.toString());
        if (objectStore) {
            IOException fileProbeFailure = null;
            try {
                Optional<FileStatus> fileStatus = fileStatusIfExists(location, path);
                if (fileStatus.isPresent()) {
                    return fileStatus.get();
                }
            }
            catch (IOException e) {
                fileProbeFailure = e;
            }
            if (isDirectory(location, false)) {
                return new PaimonDirectoryFileStatus(path);
            }
            if (fileProbeFailure != null) {
                throw fileProbeFailure;
            }
            return fileStatus(location, path);
        }
        if (isDirectory(location)) {
            return new PaimonDirectoryFileStatus(path);
        }
        return fileStatus(location, path);
    }

    @Override
    public FileStatus[] listStatus(Path path)
            throws IOException
    {
        List<FileStatus> fileStatusList = new ArrayList<>();
        Location location = Location.of(path.toString());
        if (objectStore) {
            IOException fileProbeFailure = null;
            try {
                fileStatusIfExists(location, path).ifPresent(fileStatusList::add);
            }
            catch (IOException e) {
                if (!isObjectNotFound(e)) {
                    fileProbeFailure = e;
                }
            }
            if (fileStatusList.isEmpty() && isDirectory(location, false)) {
                addDirectoryEntries(fileStatusList, location);
            }
            if (fileProbeFailure != null && fileStatusList.isEmpty()) {
                throw fileProbeFailure;
            }
        }
        else if (isDirectory(location)) {
            addDirectoryEntries(fileStatusList, location);
        }
        else if (existFile(location)) {
            fileStatusList.add(status(path));
        }
        return fileStatusList.toArray(new FileStatus[0]);
    }

    private void addDirectoryEntries(List<FileStatus> fileStatusList, Location location)
            throws IOException
    {
        FileIterator fileIterator = trinoFileSystem.listFiles(location);
        while (fileIterator.hasNext()) {
            FileEntry fileEntry = fileIterator.next();
            if (isDirectChild(location, fileEntry.location()) && !isDirectoryMarker(fileEntry.location())) {
                fileStatusList.add(new PaimonFileStatus(
                        fileEntry.length(),
                        new Path(fileEntry.location().toString()),
                        fileEntry.lastModified().getEpochSecond()));
            }
        }
        trinoFileSystem.listDirectories(location)
                .forEach(l -> fileStatusList.add(new PaimonDirectoryFileStatus(new Path(l.toString()))));
    }

    @Override
    public FileStatus[] listDirectories(Path path)
            throws IOException
    {
        Location location = Location.of(path.toString());
        if (!isDirectoryForObjectStorePrefix(location)) {
            return new FileStatus[0];
        }
        return trinoFileSystem.listDirectories(location).stream()
                .map(l -> new PaimonDirectoryFileStatus(new Path(l.toString()))).toArray(FileStatus[]::new);
    }

    @Override
    public boolean exists(Path path)
            throws IOException
    {
        Location location = Location.of(path.toString());
        if (objectStore) {
            try {
                if (existFile(location)) {
                    return true;
                }
            }
            catch (IOException e) {
                boolean directory = isDirectory(location, false);
                if (directory || isObjectNotFound(e)) {
                    return directory;
                }
                throw e;
            }
            return isDirectory(location, false);
        }
        return isDirectory(location) || existFile(location);
    }

    @Override
    public void checkOrMkdirs(Path path)
            throws IOException
    {
        if (!objectStore) {
            FileIO.super.checkOrMkdirs(path);
            return;
        }

        Location location = Location.of(path.toString());
        if (isDirectory(location, false)) {
            return;
        }

        try {
            if (existFile(location)) {
                throw new IllegalArgumentException("The path '%s' should be a directory.".formatted(path));
            }
        }
        catch (IOException e) {
            if (!isObjectNotFound(e)) {
                throw e;
            }
            // Some S3-compatible stores fail HEAD on absent directory-prefix objects with a
            // not-found error. Let mkdirs perform the real write/access check.
        }
        mkdirs(path);
    }

    private FileStatus fileStatus(Location location, Path path)
            throws IOException
    {
        TrinoInputFile trinoInputFile = trinoFileSystem.newInputFile(location);
        return new PaimonFileStatus(trinoInputFile.length(), path, trinoInputFile.lastModified().getEpochSecond());
    }

    private Optional<FileStatus> fileStatusIfExists(Location location, Path path)
            throws IOException
    {
        try {
            TrinoInputFile trinoInputFile = trinoFileSystem.newInputFile(location);
            if (!trinoInputFile.exists()) {
                return Optional.empty();
            }
            return Optional.of(new PaimonFileStatus(trinoInputFile.length(), path, trinoInputFile.lastModified().getEpochSecond()));
        }
        catch (IOException e) {
            if (isObjectNotFound(e)) {
                return Optional.empty();
            }
            throw e;
        }
    }

    private boolean existFile(Location location)
            throws IOException
    {
        try {
            return trinoFileSystem.newInputFile(location).exists();
        }
        catch (IOException e) {
            if (isObjectNotFound(e)) {
                return false;
            }
            throw e;
        }
    }

    @Override
    public boolean delete(Path path, boolean recursive)
            throws IOException
    {
        Location location = Location.of(path.toString());
        if (isDirectoryForObjectStorePrefix(location)) {
            if (!recursive) {
                if (hasChildForNonRecursiveDelete(location)) {
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
        Location location = Location.of(path.toString());
        trinoFileSystem.createDirectory(location);
        if (objectStore) {
            trinoFileSystem.newOutputFile(directoryMarker(location)).createOrOverwrite(new byte[0]);
        }
        return true;
    }

    @Override
    public boolean rename(Path source, Path target)
            throws IOException
    {
        Location sourceLocation = Location.of(source.toString());
        Location targetLocation = Location.of(target.toString());
        boolean sourceIsDirectory = isDirectoryForObjectStorePrefix(sourceLocation);
        if (!sourceIsDirectory && !existFile(sourceLocation)) {
            return false;
        }

        if (sourceIsDirectory && objectStore) {
            throw new IOException("S3 does not support directory renames");
        }

        if (isDirectoryForObjectStorePrefix(targetLocation)) {
            targetLocation = targetLocation.appendPath(source.getName());
            target = new Path(targetLocation.toString());
        }
        if (isDirectoryForObjectStorePrefix(targetLocation) || existFile(targetLocation)) {
            return false;
        }

        if (sourceIsDirectory) {
            trinoFileSystem.renameDirectory(sourceLocation, targetLocation);
        }
        else if (objectStore) {
            try {
                copyFile(source, target, false);
            }
            catch (FileAlreadyExistsException e) {
                return false;
            }
            trinoFileSystem.deleteFile(sourceLocation);
        }
        else {
            trinoFileSystem.renameFile(sourceLocation, targetLocation);
        }
        return true;
    }

    private boolean isDirectory(Location location)
            throws IOException
    {
        return isDirectory(location, true);
    }

    private boolean isDirectoryForObjectStorePrefix(Location location)
            throws IOException
    {
        if (!objectStore) {
            return isDirectory(location);
        }
        try {
            if (existFile(location)) {
                return false;
            }
        }
        catch (IOException e) {
            if (!isObjectNotFound(e)) {
                throw e;
            }
            // Some S3-compatible stores fail HEAD for absent directory-prefix objects with a
            // not-found error. Continue with directory marker/list probes, which are the
            // authoritative object-store checks.
        }
        return isDirectory(location, false);
    }

    private static boolean isObjectNotFound(IOException exception)
    {
        Throwable throwable = exception;
        while (throwable != null) {
            if (throwable instanceof FileNotFoundException || throwable instanceof NoSuchFileException) {
                return true;
            }
            String simpleName = throwable.getClass().getSimpleName();
            if (simpleName.equals("NoSuchKeyException") || simpleName.equals("NoSuchObjectException")) {
                return true;
            }
            throwable = throwable.getCause();
        }
        return false;
    }

    private boolean isDirectory(Location location, boolean checkExactFile)
            throws IOException
    {
        if (checkExactFile && objectStore && existFile(location)) {
            return false;
        }
        if (trinoFileSystem.directoryExists(location).orElse(false)) {
            return true;
        }
        if (!objectStore) {
            return false;
        }
        return directoryMarkerExists(location)
                || !trinoFileSystem.listDirectories(location).isEmpty()
                || hasDirectChildFile(location);
    }

    private boolean directoryMarkerExists(Location location)
            throws IOException
    {
        try {
            return trinoFileSystem.newInputFile(directoryMarker(location)).exists();
        }
        catch (IOException e) {
            if (!isObjectNotFound(e)) {
                throw e;
            }
            return false;
        }
    }

    private boolean hasChildForNonRecursiveDelete(Location location)
            throws IOException
    {
        if (hasDirectChildFile(location)) {
            return true;
        }
        return !trinoFileSystem.listDirectories(location).isEmpty();
    }

    private boolean hasDirectChildFile(Location location)
            throws IOException
    {
        FileIterator fileIterator = trinoFileSystem.listFiles(location);
        while (fileIterator.hasNext()) {
            Location child = fileIterator.next().location();
            if (isDirectChild(location, child) && !isDirectoryMarker(child)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isDirectChild(Location parent, Location child)
    {
        if (!isEquivalentScheme(parent.scheme(), child.scheme()) || !parent.host().equals(child.host())) {
            return false;
        }
        String parentPath = normalizeDirectoryPath(parent);
        String childPath = normalizeDirectoryPath(child);
        if (parentPath.isEmpty()) {
            return !childPath.isEmpty() && childPath.indexOf('/') < 0;
        }
        if (!childPath.startsWith(parentPath + "/")) {
            return false;
        }
        return childPath.indexOf('/', parentPath.length() + 1) < 0;
    }

    private static boolean isEquivalentScheme(Optional<String> scheme1, Optional<String> scheme2)
    {
        if (scheme1.equals(scheme2)) {
            return true;
        }
        // Trino LocalFileSystem returns "local" scheme in listFiles results, but Paimon uses "file" scheme.
        // Both schemes refer to the same local filesystem and should be treated as equivalent.
        boolean isLocal1 = scheme1.isPresent() && (scheme1.get().equals("file") || scheme1.get().equals("local"));
        boolean isLocal2 = scheme2.isPresent() && (scheme2.get().equals("file") || scheme2.get().equals("local"));
        return isLocal1 && isLocal2;
    }

    private static String normalizeDirectoryPath(Location location)
    {
        String path = location.path();
        if (path.endsWith("/") && path.length() > 1) {
            return path.substring(0, path.length() - 1);
        }
        return path;
    }

    private static Location directoryMarker(Location location)
    {
        return location.appendPath(DIRECTORY_MARKER_FILE_NAME);
    }

    private static boolean isDirectoryMarker(Location location)
    {
        return location.fileName().equals(DIRECTORY_MARKER_FILE_NAME);
    }

    private static class ObjectStoreOverwriteOutputStream
            extends PositionOutputStream
    {
        private final TrinoOutputFile outputFile;
        private final java.nio.file.Path tempFile;
        private final OutputStream outputStream;

        private boolean closed;
        private long position;

        private ObjectStoreOverwriteOutputStream(TrinoOutputFile outputFile)
                throws IOException
        {
            this.outputFile = requireNonNull(outputFile, "outputFile is null");
            this.tempFile = Files.createTempFile("trino-paimon-object-store-overwrite-", ".tmp");
            this.outputStream = Files.newOutputStream(tempFile);
        }

        @Override
        public long getPos()
        {
            return position;
        }

        @Override
        public void write(int b)
                throws IOException
        {
            outputStream.write(b);
            position++;
        }

        @Override
        public void write(byte[] bytes)
                throws IOException
        {
            outputStream.write(bytes);
            position += bytes.length;
        }

        @Override
        public void write(byte[] bytes, int off, int len)
                throws IOException
        {
            outputStream.write(bytes, off, len);
            position += len;
        }

        @Override
        public void flush()
                throws IOException
        {
            outputStream.flush();
        }

        @Override
        public void close()
                throws IOException
        {
            if (closed) {
                return;
            }

            try {
                outputStream.close();
                outputFile.createOrOverwrite(Files.readAllBytes(tempFile));
            }
            finally {
                closed = true;
                Files.deleteIfExists(tempFile);
            }
        }
    }

    private static class DirectObjectStoreTwoPhaseOutputStream
            extends TwoPhaseOutputStream
    {
        private final Path targetPath;
        private final Location targetLocation;
        private final TrinoFileSystem trinoFileSystem;
        private final PositionOutputStream outputStream;

        private boolean outputClosed;
        private boolean commitClosed;
        private boolean abortCloseStarted;
        private boolean abortCleanupFinished;

        private DirectObjectStoreTwoPhaseOutputStream(Path targetPath, Location targetLocation, TrinoFileSystem trinoFileSystem, PositionOutputStream outputStream)
        {
            this.targetPath = requireNonNull(targetPath, "targetPath is null");
            this.targetLocation = requireNonNull(targetLocation, "targetLocation is null");
            this.trinoFileSystem = requireNonNull(trinoFileSystem, "trinoFileSystem is null");
            this.outputStream = requireNonNull(outputStream, "outputStream is null");
        }

        @Override
        public void write(int b)
                throws IOException
        {
            outputStream.write(b);
        }

        @Override
        public void write(byte[] bytes)
                throws IOException
        {
            outputStream.write(bytes);
        }

        @Override
        public void write(byte[] bytes, int off, int len)
                throws IOException
        {
            outputStream.write(bytes, off, len);
        }

        @Override
        public void flush()
                throws IOException
        {
            outputStream.flush();
        }

        @Override
        public long getPos()
                throws IOException
        {
            return outputStream.getPos();
        }

        @Override
        public void close()
                throws IOException
        {
            if (commitClosed || (outputClosed && abortCleanupFinished)) {
                return;
            }

            abortCloseStarted = true;
            IOException failure = null;
            try {
                closeOutput();
            }
            catch (IOException e) {
                failure = e;
            }
            if (!abortCleanupFinished) {
                try {
                    trinoFileSystem.deleteFile(targetLocation);
                    abortCleanupFinished = true;
                }
                catch (IOException e) {
                    if (failure != null) {
                        failure.addSuppressed(e);
                    }
                    else {
                        failure = e;
                    }
                }
            }
            if (failure != null) {
                throw failure;
            }
        }

        @Override
        public Committer closeForCommit()
                throws IOException
        {
            if (commitClosed || abortCloseStarted || outputClosed) {
                throw new IOException("Stream is already closed");
            }
            closeOutput();
            commitClosed = true;
            return new DirectObjectStoreCommitter(targetPath);
        }

        private void closeOutput()
                throws IOException
        {
            if (!outputClosed) {
                outputStream.close();
                outputClosed = true;
            }
        }
    }

    private static class DirectObjectStoreCommitter
            implements TwoPhaseOutputStream.Committer
    {
        private static final long serialVersionUID = 1L;

        private final Path targetPath;

        private DirectObjectStoreCommitter(Path targetPath)
        {
            this.targetPath = requireNonNull(targetPath, "targetPath is null");
        }

        @Override
        public void commit(FileIO fileIO)
        {
            // Trino object-store streams publish data when closeForCommit closes the upload.
        }

        @Override
        public void discard(FileIO fileIO)
                throws IOException
        {
            fileIO.deleteQuietly(targetPath);
        }

        @Override
        public Path targetPath()
        {
            return targetPath;
        }

        @Override
        public void clean(FileIO fileIO) {}
    }
}
