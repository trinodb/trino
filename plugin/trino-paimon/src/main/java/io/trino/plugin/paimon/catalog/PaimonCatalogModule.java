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
package io.trino.plugin.paimon.catalog;

import com.google.inject.Binder;
import com.google.inject.Module;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.paimon.CatalogType;
import io.trino.plugin.paimon.PaimonConfig;
import io.trino.plugin.paimon.catalog.file.PaimonFileSystemCatalogModule;
import io.trino.plugin.paimon.catalog.hms.PaimonHiveCatalogModule;

import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static io.trino.plugin.paimon.CatalogType.FILESYSTEM;
import static io.trino.plugin.paimon.CatalogType.HIVE;

public class PaimonCatalogModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        bindCatalogModule(FILESYSTEM, new PaimonFileSystemCatalogModule());
        bindCatalogModule(HIVE, new PaimonHiveCatalogModule());
    }

    private void bindCatalogModule(CatalogType catalogType, Module module)
    {
        // TODO: enable more types of catalog
        install(conditionalModule(
                PaimonConfig.class,
                config -> config.getCatalogType() == catalogType,
                module));
    }
}
