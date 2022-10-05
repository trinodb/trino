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
import io.airlift.stats.TimeStat;
import io.trino.plugin.hive.NamenodeStats;
import io.trino.plugin.hive.metastore.Table;
import io.trino.spi.TrinoException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_FILESYSTEM_ERROR;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_FILE_NOT_FOUND;
import static io.trino.plugin.hive.fs.HiveFileIterator.NestedDirectoryPolicy.RECURSE;
import static java.util.Collections.emptyIterator;
import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.fs.Path.SEPARATOR_CHAR;

public class HiveFileIterator
        extends AbstractIterator<TrinoFileStatus>
{
    public enum NestedDirectoryPolicy
    {
        IGNORED,
        RECURSE,
        FAIL
    }

    private final String pathPrefix;
    private final Table table;
    private final FileSystem fileSystem;
    private final DirectoryLister directoryLister;
    private final NamenodeStats namenodeStats;
    private final NestedDirectoryPolicy nestedDirectoryPolicy;
    private final boolean ignoreAbsentPartitions;
    private final Iterator<TrinoFileStatus> remoteIterator;

    public HiveFileIterator(
            Table table,
            Path path,
            FileSystem fileSystem,
            DirectoryLister directoryLister,
            NamenodeStats namenodeStats,
            NestedDirectoryPolicy nestedDirectoryPolicy,
            boolean ignoreAbsentPartitions)
    {
        this.pathPrefix = path.toUri().getPath();
        this.table = requireNonNull(table, "table is null");
        this.fileSystem = requireNonNull(fileSystem, "fileSystem is null");
        this.directoryLister = requireNonNull(directoryLister, "directoryLister is null");
        this.namenodeStats = requireNonNull(namenodeStats, "namenodeStats is null");
        this.nestedDirectoryPolicy = requireNonNull(nestedDirectoryPolicy, "nestedDirectoryPolicy is null");
        this.ignoreAbsentPartitions = ignoreAbsentPartitions;
        this.remoteIterator = getLocatedFileStatusRemoteIterator(path);
    }

    @Override
    protected TrinoFileStatus computeNext()
    {
        while (remoteIterator.hasNext()) {
            TrinoFileStatus status = getLocatedFileStatus(remoteIterator);

            // Ignore hidden files and directories
            if (nestedDirectoryPolicy == RECURSE) {
                // Search the full sub-path under the listed prefix for hidden directories
                if (isHiddenOrWithinHiddenParentDirectory(status.getPath(), pathPrefix)) {
                    continue;
                }
            }
            else if (isHiddenFileOrDirectory(status.getPath())) {
                continue;
            }

            if (status.isDirectory()) {
                switch (nestedDirectoryPolicy) {
                    case IGNORED:
                        continue;
                    case RECURSE:
                        // Recursive listings call listFiles which should not return directories, this is a contract violation
                        // and can be handled the same way as the FAIL case
                    case FAIL:
                        throw new NestedDirectoryNotAllowedException(status.getPath());
                }
            }
            return status;
        }

        return endOfData();
    }

    private Iterator<TrinoFileStatus> getLocatedFileStatusRemoteIterator(Path path)
    {
        try (TimeStat.BlockTimer ignored = namenodeStats.getListLocatedStatus().time()) {
            return new FileStatusIterator(table, path, fileSystem, directoryLister, namenodeStats, nestedDirectoryPolicy == RECURSE);
        }
        catch (TrinoException e) {
            if (ignoreAbsentPartitions) {
                try {
                    if (!fileSystem.exists(path)) {
                        return emptyIterator();
                    }
                }
                catch (Exception ee) {
                    TrinoException trinoException = new TrinoException(HIVE_FILESYSTEM_ERROR, "Failed to check if path exists: " + path, ee);
                    trinoException.addSuppressed(e);
                    throw trinoException;
                }
            }
            throw e;
        }
    }

    private TrinoFileStatus getLocatedFileStatus(Iterator<TrinoFileStatus> iterator)
    {
        try (TimeStat.BlockTimer ignored = namenodeStats.getRemoteIteratorNext().time()) {
            return iterator.next();
        }
    }

    @VisibleForTesting
    static boolean isHiddenFileOrDirectory(Path path)
    {
        // Only looks for the last part of the path
        String pathString = path.toUri().getPath();
        int lastSeparator = pathString.lastIndexOf(SEPARATOR_CHAR);
        return containsHiddenPathPartAfterIndex(pathString, lastSeparator + 1);
    }

    @VisibleForTesting
    static boolean isHiddenOrWithinHiddenParentDirectory(Path path, String prefix)
    {
        String pathString = path.toUri().getPath();
        checkArgument(pathString.startsWith(prefix), "path %s does not start with prefix %s", pathString, prefix);
        return containsHiddenPathPartAfterIndex(pathString, prefix.length() + 1);
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
            int nextSeparator = pathString.indexOf(SEPARATOR_CHAR, startFromIndex);
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
        private final Path path;
        private final NamenodeStats namenodeStats;
        private final RemoteIterator<TrinoFileStatus> fileStatusIterator;

        private FileStatusIterator(Table table, Path path, FileSystem fileSystem, DirectoryLister directoryLister, NamenodeStats namenodeStats, boolean recursive)
        {
            this.path = path;
            this.namenodeStats = namenodeStats;
            try {
                if (recursive) {
                    this.fileStatusIterator = directoryLister.listFilesRecursively(fileSystem, table, path);
                }
                else {
                    this.fileStatusIterator = directoryLister.list(fileSystem, table, path);
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
            namenodeStats.getRemoteIteratorNext().recordException(exception);
            if (exception instanceof FileNotFoundException) {
                return new TrinoException(HIVE_FILE_NOT_FOUND, "Partition location does not exist: " + path);
            }
            return new TrinoException(HIVE_FILESYSTEM_ERROR, "Failed to list directory: " + path, exception);
        }
    }

    public static class NestedDirectoryNotAllowedException
            extends RuntimeException
    {
        private final Path nestedDirectoryPath;

        public NestedDirectoryNotAllowedException(Path nestedDirectoryPath)
        {
            super("Nested sub-directories are not allowed: " + nestedDirectoryPath);
            this.nestedDirectoryPath = requireNonNull(nestedDirectoryPath, "nestedDirectoryPath is null");
        }

        public Path getNestedDirectoryPath()
        {
            return nestedDirectoryPath;
        }
    }
}
