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

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import io.airlift.configuration.Config;

import java.io.File;
import java.util.Set;

public class ServerPluginsProviderConfig
{
    private static final Splitter SPLITTER = Splitter.on(',').omitEmptyStrings().trimResults();
    private File installedPluginsDir = new File("plugin");
    private Set<String> disabledPlugins = ImmutableSet.of();

    public File getInstalledPluginsDir()
    {
        return installedPluginsDir;
    }

    @Config("plugin.dir")
    public ServerPluginsProviderConfig setInstalledPluginsDir(File installedPluginsDir)
    {
        this.installedPluginsDir = installedPluginsDir;
        return this;
    }

    public Set<String> getDisabledPlugins()
    {
        return disabledPlugins;
    }

    @Config("plugin.disabled")
    public ServerPluginsProviderConfig setDisabledPlugins(String disabledPlugins)
    {
        this.disabledPlugins = ImmutableSet.copyOf(SPLITTER.splitToList(disabledPlugins));
        return this;
    }
}
