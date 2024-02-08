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
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.local.LocalFileSystemFactory;
import io.trino.plugin.hive.metastore.file.FileHiveMetastoreConfig;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;

import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

import static com.google.inject.multibindings.MapBinder.newMapBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.iceberg.IcebergConnectorFactory.createConnector;
import static java.util.Objects.requireNonNull;

public class TestingIcebergConnectorFactory
        implements ConnectorFactory
{
    private final Optional<Module> icebergCatalogModule;
    private final Module module;

    public TestingIcebergConnectorFactory(Path localFileSystemRootPath)
    {
        this(localFileSystemRootPath, Optional.empty());
    }

    @Deprecated
    public TestingIcebergConnectorFactory(
            Path localFileSystemRootPath,
            Optional<Module> icebergCatalogModule)
    {
        localFileSystemRootPath.toFile().mkdirs();
        this.icebergCatalogModule = requireNonNull(icebergCatalogModule, "icebergCatalogModule is null");
        this.module = binder -> {
            newMapBinder(binder, String.class, TrinoFileSystemFactory.class)
                    .addBinding("local").toInstance(new LocalFileSystemFactory(localFileSystemRootPath));
            configBinder(binder).bindConfigDefaults(FileHiveMetastoreConfig.class, config -> config.setCatalogDirectory("local:///"));
        };
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
        return createConnector(catalogName, config, context, module, icebergCatalogModule);
    }
}
