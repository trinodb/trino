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

import com.google.inject.Module;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.local.LocalFileSystemFactory;
import io.trino.plugin.deltalake.transactionlog.writer.LocalTransactionLogSynchronizer;
import io.trino.plugin.deltalake.transactionlog.writer.TransactionLogSynchronizer;
import io.trino.plugin.hive.metastore.file.FileHiveMetastoreConfig;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.inject.multibindings.MapBinder.newMapBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.deltalake.DeltaLakeConnectorFactory.createConnector;
import static java.util.Objects.requireNonNull;

public class TestingDeltaLakePlugin
        extends DeltaLakePlugin
{
    private final Path localFileSystemRootPath;
    private final Optional<Module> metastoreModule;

    public TestingDeltaLakePlugin(Path localFileSystemRootPath)
    {
        this(localFileSystemRootPath, Optional.empty());
    }

    public TestingDeltaLakePlugin(Path localFileSystemRootPath, Optional<Module> metastoreModule)
    {
        this.localFileSystemRootPath = requireNonNull(localFileSystemRootPath, "localFileSystemRootPath is null");
        this.metastoreModule = requireNonNull(metastoreModule, "metastoreModule is null");
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
                return createConnector(
                        catalogName,
                        config,
                        context,
                        metastoreModule,
                        binder -> {
                            binder.install(new TestingDeltaLakeExtensionsModule());
                            LocalFileSystemFactory localFileSystemFactory = new LocalFileSystemFactory(localFileSystemRootPath);
                            newMapBinder(binder, String.class, TrinoFileSystemFactory.class)
                                    .addBinding("local").toInstance(localFileSystemFactory);
                            newMapBinder(binder, String.class, TransactionLogSynchronizer.class)
                                    .addBinding("local").toInstance(new LocalTransactionLogSynchronizer(localFileSystemFactory));
                            configBinder(binder).bindConfigDefaults(FileHiveMetastoreConfig.class, defaults -> defaults.setCatalogDirectory("local:///"));
                        });
            }
        });
    }
}
