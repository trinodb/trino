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

import com.google.common.collect.ImmutableList;
import io.trino.plugin.hive.metastore.Table;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * A cache key designed for use in {@link CachingDirectoryLister} and {@link TransactionScopeCachingDirectoryLister}
 * that allows distinct cache entries to be created for both recursive, files-only listings and shallow listings
 * (that also might contain directories) at the same {@link Path}, ie: {@link DirectoryLister#list(FileSystem, Table, Path)}
 * and {@link DirectoryLister#listFilesRecursively(FileSystem, Table, Path)} results.
 */
final class DirectoryListingCacheKey
{
    private final Path path;
    private final int hashCode; // precomputed hashCode
    private final boolean recursiveFilesOnly;

    public DirectoryListingCacheKey(Path path, boolean recursiveFilesOnly)
    {
        this.path = requireNonNull(path, "path is null");
        this.recursiveFilesOnly = recursiveFilesOnly;
        this.hashCode = Objects.hash(path, recursiveFilesOnly);
    }

    public Path getPath()
    {
        return path;
    }

    public boolean isRecursiveFilesOnly()
    {
        return recursiveFilesOnly;
    }

    @Override
    public int hashCode()
    {
        return hashCode;
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == null || (o.getClass() != this.getClass())) {
            return false;
        }
        DirectoryListingCacheKey other = (DirectoryListingCacheKey) o;
        return recursiveFilesOnly == other.recursiveFilesOnly
                && hashCode == other.hashCode
                && path.equals(other.path);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("path", path)
                .add("isRecursiveFilesOnly", recursiveFilesOnly)
                .toString();
    }

    public static List<DirectoryListingCacheKey> allKeysWithPath(Path path)
    {
        return ImmutableList.of(new DirectoryListingCacheKey(path, true), new DirectoryListingCacheKey(path, false));
    }
}
