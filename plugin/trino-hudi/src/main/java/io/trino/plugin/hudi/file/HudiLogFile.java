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
package io.trino.plugin.hudi.file;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.hudi.common.model.HoodieLogFile;

import static com.google.common.base.Preconditions.checkArgument;

public class HudiLogFile
        implements HudiFile
{
    private final String path;
    private final String fileName;
    private final long fileSize;
    private final long modificationTime;
    private final long start;
    private final long length;

    public static HudiLogFile of(HoodieLogFile logFile)
    {
        return of(logFile, 0, logFile.getFileSize());
    }

    public static HudiLogFile of(HoodieLogFile logFile, long start, long length)
    {
        return new HudiLogFile(logFile, start, length);
    }

    @JsonCreator
    public HudiLogFile(@JsonProperty("path") String path,
            @JsonProperty("fileName") String fileName,
            @JsonProperty("fileSize") long fileSize,
            @JsonProperty("modificationTime") long modificationTime,
            @JsonProperty("start") long start,
            @JsonProperty("length") long length)
    {
        this.path = path;
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.modificationTime = modificationTime;
        this.start = start;
        this.length = length;
    }

    private HudiLogFile(HoodieLogFile logFile, long start, long length)
    {
        checkArgument(logFile != null, "logFile is null");
        checkArgument(start >= 0, "start must be positive");
        checkArgument(length >= 0, "length must be positive");
        checkArgument(start + length <= logFile.getFileSize(), "fileSize must be at least start + length");
        this.path = logFile.getPath().toString();
        this.fileName = logFile.getFileName();
        this.fileSize = logFile.getFileSize();
        this.modificationTime = logFile.getPathInfo().getModificationTime();
        this.start = start;
        this.length = length;
    }

    @JsonProperty
    @Override
    public String getPath()
    {
        return path;
    }

    @JsonProperty
    @Override
    public String getFileName()
    {
        return fileName;
    }

    @JsonProperty
    @Override
    public long getFileSize()
    {
        return fileSize;
    }

    @JsonProperty
    @Override
    public long getModificationTime()
    {
        return modificationTime;
    }

    @JsonProperty
    @Override
    public long getStart()
    {
        return start;
    }

    @JsonProperty
    @Override
    public long getLength()
    {
        return length;
    }
}
