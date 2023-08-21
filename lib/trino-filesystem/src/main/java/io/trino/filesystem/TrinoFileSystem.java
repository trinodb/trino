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
package io.trino.filesystem;

import java.io.IOException;
import java.util.Collection;
import java.util.Optional;

/**
 * TrinoFileSystem is the main abstraction for Trino to interact with data in cloud-like storage
 * systems. This replaces uses HDFS APIs in Trino.  This is not a full replacement of the HDFS, and
 * the APIs present are limited to only what Trino needs. This API supports both hierarchical and
 * blob storage systems, but they have slightly different behavior due to path resolution in
 * hierarchical storage systems.
 * <p>
 * Hierarchical file systems have directories containing files and directories. HDFS and the OS local
 * file system are examples of hierarchical file systems. The file path in a hierarchical file system
 * contains an optional list of directory names separated by '/' followed by a file name. Hierarchical
 * paths can contain relative directory references such as '.' or '..'. This means it is possible
 * for the same file to be referenced by multiple paths.  Additionally, the path of a hierarchical
 * file system can have restrictions on what elements are allowed.  For example, most hierarchical file
 * systems do not allow empty directory names, so '//' would not be legal in a path.
 * <p>
 * Blob file systems use a simple key to reference data (blobs). The file system typically applies
 * very few restrictions to the key, and generally allows keys that are illegal in hierarchical file
 * systems. This flexibility can be a problem when accessing a blob file system through a hierarchical
 * file system API, such as HDFS, as there can be blobs that cannot be referenced. To reduce these
 * issues, it is recommended that the keys do not contain '/../', '/./', or '//'.
 * <p>
 * When performing file operations, the location path cannot be empty, and must not end with a slash
 * or whitespace.
 * <p>
 * For directory operations, the location path can be empty, and can end with slash.  An empty path
 * is a reference to the root of the file system.  For blob file systems, if the location does not
 * end with a slash, one is appended, and this prefix is checked against all file locations.
 */
// NOTE: take care when adding to these APIs.  The intention is to have the minimal API surface area,
// so it is easier to maintain existing implementations and add new file system implementations.
public interface TrinoFileSystem
{
    /**
     * Creates a TrinoInputFile which can be used to read the file data. The file location path
     * cannot be empty, and must not end with a slash or whitespace.
     *
     * @throws IllegalArgumentException if location is not valid for this file system
     */
    TrinoInputFile newInputFile(Location location);

    /**
     * Creates a TrinoInputFile with a predeclared length which can be used to read the file data.
     * The length will be returned from {@link TrinoInputFile#length()} and the actual file length
     * will never be checked. The file location path cannot be empty, and must not end with a slash
     * or whitespace.
     *
     * @throws IllegalArgumentException if location is not valid for this file system
     */
    TrinoInputFile newInputFile(Location location, long length);

    /**
     * Creates a TrinoOutputFile which can be used to create or overwrite the file. The file
     * location path cannot be empty, and must not end with a slash or whitespace.
     *
     * @throws IllegalArgumentException if location is not valid for this file system
     */
    TrinoOutputFile newOutputFile(Location location);

    /**
     * Deletes the specified file. The file location path cannot be empty, and must not end with
     * a slash or whitespace. If the file is a director, an exception is raised.
     *
     * @throws IllegalArgumentException if location is not valid for this file system
     * @throws IOException if the file does not exist (optional) or was not deleted
     */
    void deleteFile(Location location)
            throws IOException;

    /**
     * Delete specified files. This operation is <b>not</b> required to be atomic, so if an error
     * occurs, all, some, or, none of the files may be deleted. This operation may be faster than simply
     * looping over the locations as some file systems support batch delete operations natively.
     *
     * @throws IllegalArgumentException if location is not valid for this file system
     * @throws IOException if a file does not exist (optional) or was not deleted
     */
    default void deleteFiles(Collection<Location> locations)
            throws IOException
    {
        for (var location : locations) {
            deleteFile(location);
        }
    }

    /**
     * Deletes all files and directories within the specified directory recursively, and deletes
     * the directory itself. If the location does not exist, this method is a noop. If the location
     * does not have a path, all files and directories in the file system are deleted.
     * <p>
     * For hierarchical file systems (e.g. HDFS), if the path is not a directory, an exception is
     * raised.
     * <p>
     * For blob file systems (e.g., S3), if the location does not end with a slash, one is appended,
     * and all blobs that start with that prefix are deleted.
     * <p>
     * If this operation fails, some, none, or all of the directory contents may
     * have been deleted.
     *
     * @param location the directory to delete
     * @throws IllegalArgumentException if location is not valid for this file system
     */
    void deleteDirectory(Location location)
            throws IOException;

    /**
     * Rename source to target without overwriting target.  This method is not required
     * to be atomic, but it is required that if an error occurs, the source, target, or both
     * must exist with the data from the source.  This operation may or may not preserve the
     * last modified time.
     *
     * @throws IllegalArgumentException if either location is not valid for this file system
     */
    void renameFile(Location source, Location target)
            throws IOException;

    /**
     * Lists all files within the specified directory recursively. The location can be empty,
     * listing all files in the file system, otherwise the location must end with a slash. If the
     * location does not exist, an empty iterator is returned.
     * <p>
     * For hierarchical file systems, if the path is not a directory, an exception is
     * raised.
     * For hierarchical file systems, if the path does not reference an existing
     * directory, an empty iterator is returned. For blob file systems, all blobs
     * that start with the location are listed. In the rare case that a blob exists with the
     * exact name of the prefix, it is not included in the results.
     * <p>
     * The returned FileEntry locations will start with the specified location exactly.
     *
     * @param location the directory to list
     * @throws IllegalArgumentException if location is not valid for this file system
     */
    FileIterator listFiles(Location location)
            throws IOException;

    /**
     * Checks if a directory exists at the specified location. For all file system types,
     * this returns <tt>true</tt> if the location is empty (the root of the file system)
     * or if any files exist within the directory, as determined by {@link #listFiles(Location)}.
     * Otherwise:
     * <ul>
     * <li>For hierarchical file systems, this returns <tt>true</tt> if the
     *     location is an empty directory, else it returns <tt>false</tt>.
     * <li>For non-hierarchical file systems, an <tt>Optional.empty()</tt> is returned,
     *     indicating that the file system has no concept of an empty directory.
     * </ul>
     *
     * @param location the location to check for a directory
     * @throws IllegalArgumentException if the location is not valid for this file system
     */
    Optional<Boolean> directoryExists(Location location)
            throws IOException;

    /**
     * Creates the specified directory and any parent directories that do not exist.
     * For hierarchical file systems, if the location already exists but is not a
     * directory, or if the directory cannot be created, an exception is raised.
     * This method does nothing for non-hierarchical file systems or if the directory
     * already exists.
     *
     * @throws IllegalArgumentException if location is not valid for this file system
     */
    void createDirectory(Location location)
            throws IOException;

    /**
     * Renames source to target. An exception is raised if the target already exists,
     * or on non-hierarchical file systems.
     *
     * @throws IllegalArgumentException if location is not valid for this file system
     */
    void renameDirectory(Location source, Location target)
            throws IOException;
}
