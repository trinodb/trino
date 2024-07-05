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
package io.trino.server.configuration;

import com.google.common.collect.ImmutableList;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.validation.FileExists;
import jakarta.validation.constraints.NotNull;

import java.io.File;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;

public class ConfigurationResolverConfig
{
    private File installedConfigurationResolver = new File("configuration-resolver");

    private List<File> configurationResolverFiles = ImmutableList.of();

    @FileExists
    public File getInstalledConfigurationResolver()
    {
        return installedConfigurationResolver;
    }

    @Config("configuration-resolver.dir")
    @ConfigDescription("Comma-separated list of configuration provider config files")
    public ConfigurationResolverConfig setInstalledConfigurationResolver(File installedConfigurationResolver)
    {
        this.installedConfigurationResolver = installedConfigurationResolver;
        return this;
    }

    @NotNull
    public List<@FileExists File> getConfigurationResolverFiles()
    {
        return configurationResolverFiles;
    }

    @Config("configuration-resolver.config-files")
    @ConfigDescription("Comma-separated list of configuration provider config files")
    public ConfigurationResolverConfig setConfigurationResolverFiles(List<String> configurationProviderFiles)
    {
        this.configurationResolverFiles = configurationProviderFiles.stream()
                .map(File::new)
                .collect(toImmutableList());
        return this;
    }
}
