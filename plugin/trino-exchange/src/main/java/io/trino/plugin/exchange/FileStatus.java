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
package io.trino.plugin.exchange;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.concurrent.Immutable;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static java.util.Objects.requireNonNull;

@Immutable
public class FileStatus
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(FileStatus.class).instanceSize();

    private final String filePath;
    private final long fileSize;

    @JsonCreator
    public FileStatus(@JsonProperty("filePath") String filePath, @JsonProperty("fileSize") long fileSize)
    {
        this.filePath = requireNonNull(filePath, "path is null");
        this.fileSize = fileSize;
    }

    @JsonProperty
    public String getFilePath()
    {
        return filePath;
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
        FileStatus that = (FileStatus) o;
        return filePath.equals(that.getFilePath()) && fileSize == that.getFileSize();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(filePath, fileSize);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("filePath", filePath)
                .add("fileSize", fileSize)
                .toString();
    }

    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + estimatedSizeOf(filePath);
    }
}
