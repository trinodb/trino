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
package io.trino.metadata;

import io.airlift.configuration.Config;
import io.airlift.configuration.LegacyConfig;

import javax.validation.constraints.NotNull;

import java.io.File;

public class LocalFileFunctionJarStoreConfig
{
    private File jarConfigurationDir = new File("etc/jar/");
    private boolean readOnly;

    @NotNull
    public File getJarConfigurationDir()
    {
        return jarConfigurationDir;
    }

    @LegacyConfig("plugin.config-dir")
    @Config("function-jar.config-dir")
    public LocalFileFunctionJarStoreConfig setJarConfigurationDir(File dir)
    {
        this.jarConfigurationDir = dir;
        return this;
    }

    public boolean isReadOnly()
    {
        return readOnly;
    }

    @Config("function-jar.read-only")
    public LocalFileFunctionJarStoreConfig setReadOnly(boolean readOnly)
    {
        this.readOnly = readOnly;
        return this;
    }
}
