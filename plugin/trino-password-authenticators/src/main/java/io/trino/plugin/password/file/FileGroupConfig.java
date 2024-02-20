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
package io.trino.plugin.password.file;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.validation.FileExists;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;
import jakarta.validation.constraints.NotNull;

import java.io.File;

import static java.util.concurrent.TimeUnit.SECONDS;

public class FileGroupConfig
{
    private File groupFile;
    private Duration refreshPeriod = new Duration(5, SECONDS);

    @NotNull
    @FileExists
    public File getGroupFile()
    {
        return groupFile;
    }

    @Config("file.group-file")
    @ConfigDescription("Location of the file that provides user group membership")
    public FileGroupConfig setGroupFile(File groupFile)
    {
        this.groupFile = groupFile;
        return this;
    }

    @NotNull
    @MinDuration("1ms")
    public Duration getRefreshPeriod()
    {
        return refreshPeriod;
    }

    @Config("file.refresh-period")
    @ConfigDescription("How often to reload the group file")
    public FileGroupConfig setRefreshPeriod(Duration refreshPeriod)
    {
        this.refreshPeriod = refreshPeriod;
        return this;
    }
}
