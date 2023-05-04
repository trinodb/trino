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

import java.util.Optional;
import java.util.TreeSet;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class FileSlice
{
    private final String baseInstantTime;

    private Optional<HudiBaseFile> baseFile;

    private final TreeSet<HudiLogFile> logFiles;

    public FileSlice(String baseInstantTime)
    {
        this.baseInstantTime = requireNonNull(baseInstantTime, "baseInstantTime is null");
        this.baseFile = Optional.empty();
        this.logFiles = new TreeSet<>(HudiLogFile.getReverseLogFileComparator());
    }

    public void setBaseFile(HudiBaseFile baseFile)
    {
        this.baseFile = Optional.ofNullable(baseFile);
    }

    public void addLogFile(HudiLogFile logFile)
    {
        this.logFiles.add(logFile);
    }

    public String getBaseInstantTime()
    {
        return baseInstantTime;
    }

    public Optional<HudiBaseFile> getBaseFile()
    {
        return baseFile;
    }

    public boolean isEmpty()
    {
        return (baseFile == null) && (logFiles.isEmpty());
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("baseInstantTime", baseInstantTime)
                .add("baseFile", baseFile)
                .add("logFiles", logFiles)
                .toString();
    }
}
