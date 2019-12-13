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
package io.prestosql.plugin.hive;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Stores original files related information.
 * To calculate the correct starting row ID of an original file, OriginalFilesUtils needs originalFiles list.
 */
public class OriginalFileLocations
{
    private final List<OriginalFileInfo> originalFiles;

    @JsonCreator
    public OriginalFileLocations(@JsonProperty("originalFiles") List<OriginalFileInfo> originalFiles)
    {
        this.originalFiles = ImmutableList.copyOf(requireNonNull(originalFiles, "originalFiles is null"));
    }

    @JsonProperty
    public List<OriginalFileInfo> getOriginalFiles()
    {
        return originalFiles;
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
        OriginalFileLocations that = (OriginalFileLocations) o;
        return Objects.equals(originalFiles, that.originalFiles);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(originalFiles);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("originalFiles", originalFiles)
                .toString();
    }

    public static class OriginalFileInfo
    {
        private final String name;
        private final long fileSize;

        @JsonCreator
        public OriginalFileInfo(@JsonProperty("name") String name,
                                @JsonProperty("fileSize") long fileSize)
        {
            this.name = requireNonNull(name, "name is null");
            checkArgument(fileSize > 0, "fileSize should be > 0");
            this.fileSize = fileSize;
        }

        @JsonProperty
        public String getName()
        {
            return name;
        }

        @JsonProperty
        public long getFileSize()
        {
            return fileSize;
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
            OriginalFileInfo that = (OriginalFileInfo) o;
            return fileSize == that.fileSize &&
                    Objects.equals(name, that.name);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(name, fileSize);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("name", name)
                    .add("fileSize", fileSize)
                    .toString();
        }
    }
}
