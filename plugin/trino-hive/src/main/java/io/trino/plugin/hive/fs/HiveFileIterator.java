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
package io.trino.plugin.hive.fs;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.AbstractIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.metastore.Table;
import io.trino.spi.TrinoException;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_FILESYSTEM_ERROR;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_FILE_NOT_FOUND;
import static io.trino.plugin.hive.fs.HiveFileIterator.NestedDirectoryPolicy.FAIL;
import static io.trino.plugin.hive.fs.HiveFileIterator.NestedDirectoryPolicy.RECURSE;
import static java.util.Objects.requireNonNull;

public class HiveFileIterator
        extends AbstractIterator<TrinoFileStatus>
{
    public enum NestedDirectoryPolicy
    {
        IGNORED,
        RECURSE,
        FAIL
    }

    private final Location location;
    private final NestedDirectoryPolicy nestedDirectoryPolicy;
    private final Iterator<TrinoFileStatus> remoteIterator;

    public HiveFileIterator(
            Table table,
            Location location,
            TrinoFileSystem fileSystem,
            DirectoryLister directoryLister,
            NestedDirectoryPolicy nestedDirectoryPolicy)
    {
        this.location = requireNonNull(location, "location is null");
        this.nestedDirectoryPolicy = requireNonNull(nestedDirectoryPolicy, "nestedDirectoryPolicy is null");
        this.remoteIterator = new FileStatusIterator(table, location, fileSystem, directoryLister, nestedDirectoryPolicy);
    }

    @Override
    protected TrinoFileStatus computeNext()
    {
        while (remoteIterator.hasNext()) {
            TrinoFileStatus status = remoteIterator.next();

            // Ignore hidden files and directories
            if (nestedDirectoryPolicy == RECURSE) {
                // Search the full sub-path under the listed prefix for hidden directories
                if (isHiddenOrWithinHiddenParentDirectory(Location.of(status.getPath()), location)) {
                    continue;
                }
            }
            else if (isHiddenFileOrDirectory(Location.of(status.getPath()))) {
                continue;
            }

            return status;
        }

        return endOfData();
    }

    @VisibleForTesting
    static boolean isHiddenFileOrDirectory(Location location)
    {
        // Only looks for the last part of the path
        String path = location.path();
        int lastSeparator = path.lastIndexOf('/');
        return containsHiddenPathPartAfterIndex(path, lastSeparator + 1);
    }

    @VisibleForTesting
    static boolean isHiddenOrWithinHiddenParentDirectory(Location path, Location rootLocation)
    {
        String pathString = path.toString();
        String prefix = rootLocation.toString();
        checkArgument(pathString.startsWith(prefix), "path %s does not start with prefix %s", pathString, prefix);
        return containsHiddenPathPartAfterIndex(pathString, prefix.endsWith("/") ? prefix.length() : prefix.length() + 1);
    }

    @VisibleForTesting
    static boolean containsHiddenPathPartAfterIndex(String pathString, int startFromIndex)
    {
        // Ignore hidden files and directories. Hive ignores files starting with _ and . as well.
        while (startFromIndex < pathString.length()) {
            char firstNameChar = pathString.charAt(startFromIndex);
            if (firstNameChar == '.' || firstNameChar == '_') {
                return true;
            }
            int nextSeparator = pathString.indexOf('/', startFromIndex);
            if (nextSeparator < 0) {
                break;
            }
            startFromIndex = nextSeparator + 1;
        }
        return false;
    }

    private static class FileStatusIterator
            implements Iterator<TrinoFileStatus>
    {
        private final Location location;
        private final RemoteIterator<TrinoFileStatus> fileStatusIterator;

        private FileStatusIterator(
                Table table,
                Location location,
                TrinoFileSystem fileSystem,
                DirectoryLister directoryLister,
                NestedDirectoryPolicy nestedDirectoryPolicy)
        {
            this.location = requireNonNull(location, "location is null");
            try {
                if (nestedDirectoryPolicy == RECURSE) {
                    this.fileStatusIterator = directoryLister.listFilesRecursively(fileSystem, table, location);
                }
                else {
                    this.fileStatusIterator = new DirectoryListingFilter(
                            location,
                            directoryLister.listFilesRecursively(fileSystem, table, location),
                            nestedDirectoryPolicy == FAIL);
                }
            }
            catch (IOException e) {
                throw processException(e);
            }
        }

        @Override
        public boolean hasNext()
        {
            try {
                return fileStatusIterator.hasNext();
            }
            catch (IOException e) {
                throw processException(e);
            }
        }

        @Override
        public TrinoFileStatus next()
        {
            try {
                return fileStatusIterator.next();
            }
            catch (IOException e) {
                throw processException(e);
            }
        }

        private TrinoException processException(IOException exception)
        {
            if (exception instanceof FileNotFoundException) {
                return new TrinoException(HIVE_FILE_NOT_FOUND, "Partition location does not exist: " + location);
            }
            return new TrinoException(HIVE_FILESYSTEM_ERROR, "Failed to list directory: " + location, exception);
        }
    }

    public static class NestedDirectoryNotAllowedException
            extends RuntimeException
    {
        private final String nestedDirectoryPath;

        public NestedDirectoryNotAllowedException(String nestedDirectoryPath)
        {
            super("Nested sub-directories are not allowed: " + nestedDirectoryPath);
            this.nestedDirectoryPath = requireNonNull(nestedDirectoryPath, "nestedDirectoryPath is null");
        }

        public String getNestedDirectoryPath()
        {
            return nestedDirectoryPath;
        }
    }
}
