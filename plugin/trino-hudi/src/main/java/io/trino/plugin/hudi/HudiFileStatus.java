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
package io.trino.plugin.hudi;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class HudiFileStatus
{
    private final boolean isDirectory;
    private final String fileName;
    private final String location;
    private final long blockSize;
    private final long length;
    private final long offset;
    private final long modificationTime;

    @JsonCreator
    public HudiFileStatus(@JsonProperty("isDirectory") boolean isDirectory,
                          @JsonProperty("fileName") String fileName,
                          @JsonProperty("location") String location,
                          @JsonProperty("length") long length,
                          @JsonProperty("modificationTime") long modificationTime,
                          @JsonProperty("blockSize") long blockSize,
                          @JsonProperty("offset") long offset)
    {
        checkArgument(blockSize > 0, "blockSize myst be positive");
        checkArgument(length > 0, "length myst be positive");
        checkArgument(offset >= 0, "offset myst be positive");
        this.isDirectory = isDirectory;
        this.fileName = requireNonNull(fileName, "fileName is null");
        this.location = requireNonNull(location, "location is null");
        this.blockSize = blockSize;
        this.length = length;
        this.offset = offset;
        this.modificationTime = modificationTime;
    }

    @JsonProperty
    public boolean isDirectory()
    {
        return isDirectory;
    }

    @JsonProperty
    public String getFileName()
    {
        return fileName;
    }

    @JsonProperty
    public String getLocation()
    {
        return location;
    }

    @JsonProperty
    public long getBlockSize()
    {
        return blockSize;
    }

    @JsonProperty
    public long getLength()
    {
        return length;
    }

    @JsonProperty
    public long getOffset()
    {
        return offset;
    }

    @JsonProperty
    public long getModificationTime()
    {
        return modificationTime;
    }
}
