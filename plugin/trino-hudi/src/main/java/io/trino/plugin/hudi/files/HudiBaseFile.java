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

import io.trino.filesystem.FileEntry;
import io.trino.filesystem.Location;

import java.util.Objects;

import static io.trino.plugin.hudi.files.FSUtils.isLogFile;
import static java.util.Objects.requireNonNull;

public class HudiBaseFile
{
    private transient FileEntry fileEntry;
    private final String fullPath;
    private final String fileName;
    private long fileLen;

    public HudiBaseFile(FileEntry fileEntry)
    {
        this(fileEntry,
                fileEntry.location().path(),
                fileEntry.location().fileName(),
                fileEntry.length());
    }

    private HudiBaseFile(FileEntry fileEntry, String fullPath, String fileName, long fileLen)
    {
        this.fileEntry = requireNonNull(fileEntry, "fileEntry is null");
        this.fullPath = requireNonNull(fullPath, "fullPath is null");
        this.fileLen = fileLen;
        this.fileName = requireNonNull(fileName, "fileName is null");
    }

    public String getPath()
    {
        return fullPath;
    }

    public Location getFullPath()
    {
        if (fileEntry != null) {
            return fileEntry.location();
        }

        return Location.of(fullPath);
    }

    public String getFileName()
    {
        return fileName;
    }

    public FileEntry getFileEntry()
    {
        return fileEntry;
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
