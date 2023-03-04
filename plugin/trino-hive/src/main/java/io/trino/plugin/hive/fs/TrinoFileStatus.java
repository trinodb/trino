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
import io.trino.filesystem.FileEntry;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class TrinoFileStatus
        implements Comparable<TrinoFileStatus>
{
    private final List<BlockLocation> blockLocations;
    private final Path path;
    private final boolean isDirectory;
    private final long length;
    private final long modificationTime;

    public TrinoFileStatus(FileEntry entry)
    {
        this(
                entry.blockLocations()
                        .orElseGet(() -> List.of(new FileEntry.BlockLocation(List.of(), 0, entry.length())))
                        .stream()
                        .map(BlockLocation::new)
                        .collect(toImmutableList()),
                new Path(entry.path()),
                false,
                entry.length(),
                entry.lastModified().toEpochMilli());
    }

    public TrinoFileStatus(LocatedFileStatus fileStatus)
    {
        this(BlockLocation.fromHiveBlockLocations(fileStatus.getBlockLocations()),
                fileStatus.getPath(),
                fileStatus.isDirectory(),
                fileStatus.getLen(),
                fileStatus.getModificationTime());
    }

    public TrinoFileStatus(List<BlockLocation> blockLocations, Path path, boolean isDirectory, long length, long modificationTime)
    {
        this.blockLocations = ImmutableList.copyOf(requireNonNull(blockLocations, "blockLocations is null"));
        this.path = requireNonNull(path, "path is null");
        this.isDirectory = isDirectory;
        this.length = length;
        this.modificationTime = modificationTime;
    }

    public List<BlockLocation> getBlockLocations()
    {
        return blockLocations;
    }

    public Path getPath()
    {
        return path;
    }

    public boolean isDirectory()
    {
        return isDirectory;
    }

    public long getLength()
    {
        return length;
    }

    public long getModificationTime()
    {
        return modificationTime;
    }

    @Override
    public int compareTo(TrinoFileStatus other)
    {
        return path.compareTo(other.getPath());
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TrinoFileStatus that = (TrinoFileStatus) o;
        return isDirectory == that.isDirectory
                && length == that.length
                && modificationTime == that.modificationTime
                && blockLocations.equals(that.blockLocations)
                && path.equals(that.path);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(blockLocations, path, isDirectory, length, modificationTime);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("blockLocations", blockLocations)
                .add("path", path)
                .add("isDirectory", isDirectory)
                .add("length", length)
                .add("modificationTime", modificationTime)
                .toString();
    }
}
