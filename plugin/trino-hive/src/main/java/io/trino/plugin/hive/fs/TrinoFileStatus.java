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
import io.trino.filesystem.FileEntry.Block;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static java.util.Objects.requireNonNull;

public class TrinoFileStatus
        implements Comparable<TrinoFileStatus>
{
    private static final long INSTANCE_SIZE = instanceSize(TrinoFileStatus.class);

    private final List<BlockLocation> blockLocations;
    private final String path;
    private final boolean isDirectory;
    private final long length;
    private final long modificationTime;

    public TrinoFileStatus(FileEntry entry)
    {
        this(entry.blocks()
                        .orElseGet(() -> List.of(new Block(List.of(), 0, entry.length())))
                        .stream()
                        .map(BlockLocation::new)
                        .collect(toImmutableList()),
                entry.location().toString(),
                false,
                entry.length(),
                entry.lastModified().toEpochMilli());
    }

    public TrinoFileStatus(List<BlockLocation> blockLocations, String path, boolean isDirectory, long length, long modificationTime)
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

    public String getPath()
    {
        return path;
    }

    public long getLength()
    {
        return length;
    }

    public long getModificationTime()
    {
        return modificationTime;
    }

    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + estimatedSizeOf(blockLocations, BlockLocation::getRetainedSizeInBytes)
                + estimatedSizeOf(path);
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
