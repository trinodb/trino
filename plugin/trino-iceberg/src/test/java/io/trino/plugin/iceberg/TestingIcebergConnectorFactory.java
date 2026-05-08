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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Module;
import io.airlift.configuration.ConfigurationAwareModule;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.local.LocalFileSystemFactory;
import io.trino.plugin.hive.metastore.file.FileHiveMetastoreConfig;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;

import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static com.google.inject.multibindings.MapBinder.newMapBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.iceberg.IcebergConnectorFactory.createConnector;
import static java.util.Objects.requireNonNull;

public class TestingIcebergConnectorFactory
        implements ConnectorFactory
{
    private final Path localFileSystemRootPath;
    private final Supplier<Optional<Module>> icebergCatalogModule;
    private final Supplier<Optional<Module>> additionalOverrideModule;

    public TestingIcebergConnectorFactory(Path localFileSystemRootPath)
    {
        this(localFileSystemRootPath, Optional::empty, Optional::empty);
    }

    @Deprecated
    public TestingIcebergConnectorFactory(
            Path localFileSystemRootPath,
            Supplier<Optional<Module>> icebergCatalogModule)
    {
        this(localFileSystemRootPath, icebergCatalogModule, Optional::empty);
    }

    public TestingIcebergConnectorFactory(
            Path localFileSystemRootPath,
            Supplier<Optional<Module>> icebergCatalogModule,
            Supplier<Optional<Module>> additionalOverrideModule)
    {
        this.localFileSystemRootPath = requireNonNull(localFileSystemRootPath, "localFileSystemRootPath is null");
        var rootPath = localFileSystemRootPath.toFile();
        var _ = rootPath.mkdirs();
        this.icebergCatalogModule = requireNonNull(icebergCatalogModule, "icebergCatalogModule is null");
        this.additionalOverrideModule = requireNonNull(additionalOverrideModule, "additionalOverrideModule is null");
    }

    @Override
    public String getName()
    {
        return "iceberg";
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        if (!config.containsKey("iceberg.catalog.type")) {
            config = ImmutableMap.<String, String>builder()
                    .putAll(config)
                    .put("iceberg.catalog.type", "TESTING_FILE_METASTORE")
                    .buildOrThrow();
        }
        Module localModule = binder -> {
            newMapBinder(binder, String.class, TrinoFileSystemFactory.class)
                    .addBinding("local").toInstance(new LocalFileSystemFactory(localFileSystemRootPath));
            configBinder(binder).bindConfigDefaults(
                    FileHiveMetastoreConfig.class,
                    metastoreConfig -> metastoreConfig.setCatalogDirectory("local:///" + catalogName));
        };
        Module module = additionalOverrideModule.get()
                .map(override -> (Module) ConfigurationAwareModule.combine(localModule, override))
                .orElse(localModule);
        return createConnector(catalogName, config, context, module, icebergCatalogModule.get());
    }
}
