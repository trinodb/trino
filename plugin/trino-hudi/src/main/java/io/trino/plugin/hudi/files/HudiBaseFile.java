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
package io.trino.plugin.hudi.files;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.filesystem.FileEntry;
import io.trino.filesystem.Location;
import io.trino.plugin.hudi.HudiFileStatus;

import java.util.Objects;

import static io.trino.plugin.hudi.files.FSUtils.isLogFile;
import static java.util.Objects.requireNonNull;

public class HudiBaseFile
        implements HudiFile
{
    private final HudiFileStatus fileStatus;
    private final String fullPath;
    private final String fileName;
    private final long fileLen;

    public HudiBaseFile(FileEntry fileEntry)
    {
        this(new HudiFileStatus(
                false,
                fileEntry.location().fileName(),
                fileEntry.location().toString(),
                fileEntry.length(),
                fileEntry.lastModified().toEpochMilli(),
                fileEntry.blocks().map(listOfBlocks -> (!listOfBlocks.isEmpty()) ? listOfBlocks.get(0).length() : 0).orElse(0L),
                fileEntry.blocks().map(listOfBlocks -> (!listOfBlocks.isEmpty()) ? listOfBlocks.get(0).offset() : 0).orElse(0L)));
    }

    public HudiBaseFile(HudiFileStatus fileStatus)
    {
        this(fileStatus, fileStatus.getLocation(), fileStatus.getFileName(), fileStatus.getLength());
    }

    @JsonCreator
    public HudiBaseFile(@JsonProperty("fileStatus") HudiFileStatus fileStatus,
                        @JsonProperty("fullPath") String fullPath,
                        @JsonProperty("fileName") String fileName,
                        @JsonProperty("fileLen") long fileLen)
    {
        this.fileStatus = requireNonNull(fileStatus, "fileStatus is null");
        this.fullPath = requireNonNull(fullPath, "fullPath is null");
        this.fileLen = fileLen;
        this.fileName = requireNonNull(fileName, "fileName is null");
    }

    @JsonProperty
    public HudiFileStatus getFileStatus()
    {
        return fileStatus;
    }

    @JsonProperty
    public String getFullPath()
    {
        return fullPath;
    }

    @Override
    public Location getLocation()
    {
        return Location.of(fullPath);
    }

    @JsonProperty
    public String getFileName()
    {
        return fileName;
    }

    @Override
    @JsonProperty
    public long getFileLen()
    {
        return fileLen;
    }

    @Override
    public long getOffset()
    {
        return fileStatus.getOffset();
    }

    @JsonProperty
    @Override
    public long getFileModifiedTime()
    {
        return fileStatus.getModificationTime();
    }

    public String getFileId()
    {
        return getFileName().split("_")[0];
    }

    public String getCommitTime()
    {
        String fileName = getFileName();
        if (isLogFile(fileName)) {
            return fileName.split("_")[1].split("\\.")[0];
        }
        return fileName.split("_")[2].split("\\.")[0];
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
        HudiBaseFile dataFile = (HudiBaseFile) o;
        return Objects.equals(fullPath, dataFile.fullPath);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(fullPath);
    }

    @Override
    public String toString()
    {
        return "BaseFile{fullPath=" + fullPath + ", fileLen=" + fileLen + '}';
    }
}
