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
package io.trino.plugin.deltalake;

import com.google.inject.Binder;
import com.google.inject.Module;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.local.LocalFileSystemFactory;
import io.trino.plugin.hive.metastore.file.FileHiveMetastoreConfig;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.inject.multibindings.MapBinder.newMapBinder;
import static com.google.inject.util.Modules.EMPTY_MODULE;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;

public class TestingDeltaLakePlugin
        extends DeltaLakePlugin
{
    private final Path localFileSystemRootPath;
    private final Optional<Module> metastoreModule;
    private final Optional<TrinoFileSystemFactory> fileSystemFactory;
    private final Module additionalModule;

    public TestingDeltaLakePlugin(Path localFileSystemRootPath)
    {
        this(localFileSystemRootPath, Optional.empty(), Optional.empty(), EMPTY_MODULE);
    }

    public TestingDeltaLakePlugin(Path localFileSystemRootPath, Optional<Module> metastoreModule, Optional<TrinoFileSystemFactory> fileSystemFactory, Module additionalModule)
    {
        this.localFileSystemRootPath = requireNonNull(localFileSystemRootPath, "localFileSystemRootPath is null");
        this.metastoreModule = requireNonNull(metastoreModule, "metastoreModule is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.additionalModule = requireNonNull(additionalModule, "additionalModule is null");
    }

    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        return List.of(new ConnectorFactory()
        {
            @Override
            public String getName()
            {
                return DeltaLakeConnectorFactory.CONNECTOR_NAME;
            }

            @Override
            public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
            {
                localFileSystemRootPath.toFile().mkdirs();
                return InternalDeltaLakeConnectorFactory.createConnector(
                        catalogName,
                        config,
                        context,
                        metastoreModule,
                        fileSystemFactory,
                        new AbstractConfigurationAwareModule()
                        {
                            @Override
                            protected void setup(Binder binder)
                            {
                                install(additionalModule);
                                install(new TestingDeltaLakeExtensionsModule());
                                newMapBinder(binder, String.class, TrinoFileSystemFactory.class)
                                        .addBinding("local").toInstance(new LocalFileSystemFactory(localFileSystemRootPath));
                                configBinder(binder).bindConfigDefaults(FileHiveMetastoreConfig.class, config -> config.setCatalogDirectory("local:///"));
                            }
                        });
            }
        });
    }
}
