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

import com.google.inject.Module;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;

import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.iceberg.InternalIcebergConnectorFactory.createConnector;
import static java.util.Objects.requireNonNull;

public class TestingIcebergConnectorFactory
        implements ConnectorFactory
{
    private final Optional<Module> icebergCatalogModule;
    private final Optional<TrinoFileSystemFactory> fileSystemFactory;
    private final Module module;

    public TestingIcebergConnectorFactory(Optional<Module> icebergCatalogModule, Optional<TrinoFileSystemFactory> fileSystemFactory, Module module)
    {
        this.icebergCatalogModule = requireNonNull(icebergCatalogModule, "icebergCatalogModule is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.module = requireNonNull(module, "module is null");
    }

    @Override
    public String getName()
    {
        return "iceberg";
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        return createConnector(catalogName, config, context, module, icebergCatalogModule, fileSystemFactory);
    }
}
