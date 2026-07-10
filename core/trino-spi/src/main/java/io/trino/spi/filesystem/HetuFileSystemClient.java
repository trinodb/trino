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
package io.trino.spi.filesystem;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.stream.Stream;

/**
 * HetuFileSystemClient provides APIs which highly resemble those APIs provided by
 * {@link java.nio.file.spi.FileSystemProvider} and {@link java.nio.file.Files}.
 * This is to be used as the file system in different modules for file-related operations on different file systems
 * Eg. local filesystem, HDFS, etc.
 *
 * @since 2020-03-30
 */
public interface HetuFileSystemClient
        extends AutoCloseable
{
    /**
     * Create directories according to the given dir.
     * It creates all intermediate directories if they do not exist.
     * No exception will be thrown if the directory already exists.
     * Therefore for a path /A/B/C this method always ensures the result of /A/B/C/, no matter /A/B/ and C exist or not.
     *
     * @param dir Directory to create
     * @return Path to the created directory
     * @throws IOException When IO errors occur during the operation
     */
    Path createDirectories(Path dir)
            throws IOException;

    /**
     * Create an empty directory in a directory that already exists.
     * For a path /A/B/C, the directory /A/B must already exist otherwise an exception will be thrown.
     * It will also throw an exception if the directory C already exists.
     *
     * @param dir Directory to create
     * @return Path to the created directory
     * @throws NoSuchFileException When the parent directory does not exist
     * @throws FileAlreadyExistsException When the directory already exists
     * @throws IOException Other exceptions during creation.
     */
    Path createDirectory(Path dir)
            throws IOException;

    /**
     * Delete a given file or directory. If the given path is a directory it must be empty.
     *
     * @param path Path to delete.
     * @throws NoSuchFileException If the given file does not exist.
     * @throws DirectoryNotEmptyException If the path is a directory and not empty.
     * @throws IOException Other exceptions during deletion.
     */
    void delete(Path path)
            throws IOException;

    /**
     * Delete a given file or directory. If the given path is a directory it must be empty.
     * Return the result of deletion.
     *
     * @param path Path to delete.
     * @return Whether the deletion is successful. If the file does not exist, return {@code false}.
     * @throws DirectoryNotEmptyException If the path is a directory and not empty.
     * @throws IOException Other exceptions during deletion.
     */
    boolean deleteIfExists(Path path)
            throws IOException;

    /**
     * Delete the given file, or the given path recursively.
     * If the given path does not exist it will return false WITHOUT throwing an exception.
     * This operation is not atomic and have no guarantee of the deletion.
     * It is recommended to check the result manually otherwise it is used at user's own risk.
     *
     * @param path Path to delete.
     * @return true if deletion is successful otherwise false.
     * @throws IOException Other exceptions during deletion.
     */
    boolean deleteRecursively(Path path)
            throws IOException;

    /**
     * Return if the given file or directory exists.
     *
     * @param path Path to the file or directory.
     * @return Whether the path exists. If an IOException occurs in this process, return {@code false}.
     */
    boolean exists(Path path);

    /**
     * Move the file from source position to target position.
     * Also used for renaming.
     *
     * @param source Path to the file.
     * @param target Target path to the file.
     * @throws NoSuchFileException If the source file does not exist.
     * @throws IOException If any exception occurs during the process.
     */
    void move(Path source, Path target)
            throws IOException;

    /**
     * Read from a file.
     *
     * @param path Path to the file.
     * @return An opened {@code InputStream} to read the file.
     * @throws NoSuchFileException If the given file does not exist.
     * @throws IOException Other exceptions occur during the process.
     */
    InputStream newInputStream(Path path)
            throws IOException;

    /**
     * Write to a file.
     *
     * @param path Path to the file.
     * @param options Open options to the file. Currently supports: {@code java.nio.file.StandardOpenOption.CREATE_NEW}.
     * @return An opened {@code OutputStream} to write the file.
     * @throws UnsupportedOperationException If the provided OpenOptions are not supported by the filesystem implementation.
     * @throws NoSuchFileException If the parent directory does not exist.
     * @throws FileAlreadyExistsException If the file already exists and open option is set to CREATE_NEW.
     * @throws IOException If any exception occurs during the process.
     */
    OutputStream newOutputStream(Path path, OpenOption... options)
            throws IOException;

    /**
     * Get the attribute of a file or directory.
     * <p>
     * The supported attributes are:
     * <ul>
     * <li>{@code Long lastModifiedTime}: last modified time in milliseconds since epoch as Long</li>
     * <li>{@code Long size}: size of the file as Long</li>
     * </ul>
     *
     * @param path Path to the file.
     * @param attribute Attribute to get. Currently these attributes are supported:
     * {@code lastModifiedTime}, {@code size}.
     * @return Attribute value object.
     * @throws NoSuchFileException If the given file does not exist.
     * @throws IOException Other exceptions occur during the access.
     */
    Object getAttribute(Path path, String attribute)
            throws IOException;

    /**
     * Return if the given path is a directory.
     *
     * @param path Path to the file or directory.
     * @return {@code true} if the file is a directory; {@code false} if the file does not exist, is not a directory, or an exception occurs.
     */
    boolean isDirectory(Path path);

    /**
     * Traverse a directory and return files and sub-folders inside.
     *
     * @param dir Directory to traverse.
     * @return A stream of files and sub-folders in a directory.
     * @throws NoSuchFileException If the given path does not exist.
     * @throws IOException If any other IO exception occurs during the process.
     */
    Stream<Path> list(Path dir)
            throws IOException;

    /**
     * Walk through all paths (files and directories) rooted on the given path.
     *
     * @param dir Root to traverse from.
     * @return A stream of all files rooted on this directory. This contains all directories, including the root.
     * @throws NoSuchFileException If the given path does not exist.
     * @throws IOException If any other IO exception occurs during the process.
     */
    Stream<Path> walk(Path dir)
            throws IOException;

    /**
     * Close the filesystem client.
     */
    @Override
    void close()
            throws IOException;

    long getUsableSpace(Path path)
            throws IOException;

    long getTotalSpace(Path path)
            throws IOException;

    Path createTemporaryFile(Path path, String prefix, String suffix)
            throws IOException;

    Path createFile(Path path)
            throws IOException;

    Stream<Path> getDirectoryStream(Path path, String prefix, String suffix)
            throws IOException;
}
