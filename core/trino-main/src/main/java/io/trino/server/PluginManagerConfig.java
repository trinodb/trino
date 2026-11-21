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
package io.trino.server;

import com.google.common.collect.ImmutableList;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import jakarta.validation.constraints.NotNull;

import java.util.List;

public class PluginManagerConfig
{
    private List<String> sharedPackages = ImmutableList.of();

    @NotNull
    public List<String> getSharedPackages()
    {
        return sharedPackages;
    }

    @Config("plugin.shared-packages")
    @ConfigDescription("Comma separated list of package prefixes that plugins load from the server class path, as they load the SPI")
    public PluginManagerConfig setSharedPackages(List<String> sharedPackages)
    {
        this.sharedPackages = ImmutableList.copyOf(sharedPackages);
        return this;
    }
}
