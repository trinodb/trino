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
import java.util.Optional;

public class RangerConfig
{
    private String serviceName;
    private List<File> pluginConfigResource = ImmutableList.of();
    private List<File> hadoopConfigResource = ImmutableList.of();
    private Optional<String> extraCredentialsGroupsKey = Optional.empty();
    private String extraCredentialsGroupsSeparator = "|";

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

    public Optional<String> getExtraCredentialsGroupsKey()
    {
        return extraCredentialsGroupsKey;
    }

    @Config("ranger.extra-credentials.groups-key")
    @ConfigDescription("Extra credential key whose value contains pipe-separated group names to be included in Ranger access requests. " +
            "When set, groups injected by identity proxies via X-Trino-Extra-Credential are visible to Ranger group-based policies. " +
            "If not set (default), only Identity.getGroups() is used.")
    public RangerConfig setExtraCredentialsGroupsKey(String extraCredentialsGroupsKey)
    {
        this.extraCredentialsGroupsKey = Optional.ofNullable(extraCredentialsGroupsKey);
        return this;
    }

    public String getExtraCredentialsGroupsSeparator()
    {
        return extraCredentialsGroupsSeparator;
    }

    @Config("ranger.extra-credentials.groups-separator")
    @ConfigDescription("Separator used to split group names in the extra credential value specified by ranger.extra-credentials.groups-key. Defaults to '|'.")
    public RangerConfig setExtraCredentialsGroupsSeparator(String extraCredentialsGroupsSeparator)
    {
        this.extraCredentialsGroupsSeparator = extraCredentialsGroupsSeparator;
        return this;
    }
}
