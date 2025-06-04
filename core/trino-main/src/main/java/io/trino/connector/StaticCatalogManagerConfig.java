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
package io.trino.connector;

import com.google.common.collect.ImmutableList;
import io.airlift.configuration.Config;
import io.airlift.configuration.LegacyConfig;
import jakarta.validation.constraints.NotNull;

import java.io.File;
import java.util.List;

public class StaticCatalogManagerConfig
{
    private File catalogConfigurationDir = new File("etc/catalog/");
    private List<String> disabledCatalogs = ImmutableList.of();

    @NotNull
    public File getCatalogConfigurationDir()
    {
        return catalogConfigurationDir;
    }

    @LegacyConfig("plugin.config-dir")
    @Config("catalog.config-dir")
    public StaticCatalogManagerConfig setCatalogConfigurationDir(File dir)
    {
        this.catalogConfigurationDir = dir;
        return this;
    }

    public List<String> getDisabledCatalogs()
    {
        return disabledCatalogs;
    }

    @Config("catalog.disabled-catalogs")
    public StaticCatalogManagerConfig setDisabledCatalogs(List<String> catalogs)
    {
        this.disabledCatalogs = ImmutableList.copyOf(catalogs);
        return this;
    }
}
