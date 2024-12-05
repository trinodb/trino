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
package io.trino.plugin.ranger;

import com.google.common.collect.ImmutableList;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.validation.FileExists;
import jakarta.validation.constraints.NotEmpty;

import java.io.File;
import java.util.List;

public class RangerConfig
{
    private String serviceName;
    private List<File> pluginConfigResource = ImmutableList.of();
    private List<File> hadoopConfigResource = ImmutableList.of();

    @NotEmpty
    public String getServiceName()
    {
        return serviceName;
    }

    @Config("ranger.service.name")
    @ConfigDescription("Name of Ranger service containing policies to enforce")
    public RangerConfig setServiceName(String serviceName)
    {
        this.serviceName = serviceName;
        return this;
    }

    public List<@FileExists File> getPluginConfigResource()
    {
        return pluginConfigResource;
    }

    @Config("ranger.plugin.config.resource")
    @ConfigDescription("List of paths to Ranger plugin configuration files")
    public RangerConfig setPluginConfigResource(List<File> pluginConfigResource)
    {
        this.pluginConfigResource = ImmutableList.copyOf(pluginConfigResource);
        return this;
    }

    public List<@FileExists File> getHadoopConfigResource()
    {
        return hadoopConfigResource;
    }

    @Config("ranger.hadoop.config.resource")
    @ConfigDescription("List of paths to hadoop configuration files")
    @SuppressWarnings("unused")
    public RangerConfig setHadoopConfigResource(List<File> hadoopConfigResource)
    {
        this.hadoopConfigResource = ImmutableList.copyOf(hadoopConfigResource);
        return this;
    }
}
