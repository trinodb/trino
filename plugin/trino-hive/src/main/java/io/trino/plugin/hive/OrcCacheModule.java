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
package io.trino.plugin.hive;

import com.google.inject.Binder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.orc.OrcFileMetadataProvider;
import io.trino.orc.StorageOrcFileMetadataProvider;
import io.trino.plugin.hive.orc.InMemoryCachingOrcFileMetadataProvider;
import io.trino.plugin.hive.orc.OrcMetadataCacheConfig;

import static com.google.inject.Scopes.SINGLETON;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class OrcCacheModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(OrcMetadataCacheConfig.class);
        OrcMetadataCacheConfig config = buildConfigObject(OrcMetadataCacheConfig.class);
        if (config.isFileTailCacheEnabled()) {
            binder.bind(OrcFileMetadataProvider.class)
                    .to(InMemoryCachingOrcFileMetadataProvider.class)
                    .in(SINGLETON);
            binder.bind(InMemoryCachingOrcFileMetadataProvider.class).in(SINGLETON);
            newExporter(binder).export(InMemoryCachingOrcFileMetadataProvider.class).withGeneratedName();
        }
        else {
            binder.bind(OrcFileMetadataProvider.class)
                    .to(StorageOrcFileMetadataProvider.class)
                    .in(SINGLETON);
        }
    }
}
