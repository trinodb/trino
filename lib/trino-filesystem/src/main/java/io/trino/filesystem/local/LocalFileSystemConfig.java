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
package io.trino.filesystem.local;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.validation.FileExists;

import java.nio.file.Path;
import java.nio.file.Paths;

import static com.google.common.base.StandardSystemProperty.JAVA_IO_TMPDIR;

public class LocalFileSystemConfig
{
    private Path location = Paths.get(System.getProperty(JAVA_IO_TMPDIR.key()));

    public @FileExists Path getLocation()
    {
        return location;
    }

    @ConfigDescription("Local file system root directory")
    @Config("local.location")
    public LocalFileSystemConfig setLocation(Path location)
    {
        this.location = location;
        return this;
    }
}
