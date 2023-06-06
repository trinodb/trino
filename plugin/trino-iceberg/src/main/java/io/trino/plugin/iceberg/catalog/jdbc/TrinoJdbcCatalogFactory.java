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
package io.trino.plugin.iceberg.catalog.jdbc;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.iceberg.IcebergConfig;
import io.trino.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.catalog.TrinoCatalogFactory;
import io.trino.plugin.iceberg.fileio.ForwardingFileIo;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.TypeManager;
import jakarta.annotation.PreDestroy;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.jdbc.JdbcClientPool;

import java.util.Map;

import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.CatalogProperties.URI;
import static org.apache.iceberg.CatalogProperties.WAREHOUSE_LOCATION;
import static org.apache.iceberg.jdbc.JdbcCatalog.PROPERTY_PREFIX;

public class TrinoJdbcCatalogFactory
        implements TrinoCatalogFactory
{
    private final CatalogName catalogName;
    private final TypeManager typeManager;
    private final IcebergTableOperationsProvider tableOperationsProvider;
    private final TrinoFileSystemFactory fileSystemFactory;
    private final IcebergJdbcClient jdbcClient;
    private final String jdbcCatalogName;
    private final String defaultWarehouseDir;
    private final boolean isUniqueTableLocation;
    private final Map<String, String> catalogProperties;
    private final JdbcClientPool clientPool;

    @Inject
    public TrinoJdbcCatalogFactory(
            CatalogName catalogName,
            TypeManager typeManager,
            IcebergTableOperationsProvider tableOperationsProvider,
            TrinoFileSystemFactory fileSystemFactory,
            IcebergJdbcClient jdbcClient,
            IcebergJdbcCatalogConfig jdbcConfig,
            IcebergConfig icebergConfig)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.tableOperationsProvider = requireNonNull(tableOperationsProvider, "tableOperationsProvider is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.isUniqueTableLocation = requireNonNull(icebergConfig, "icebergConfig is null").isUniqueTableLocation();
        this.jdbcClient = requireNonNull(jdbcClient, "jdbcClient is null");
        this.jdbcCatalogName = jdbcConfig.getCatalogName();
        this.defaultWarehouseDir = jdbcConfig.getDefaultWarehouseDir();

        ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();
        properties.put(URI, jdbcConfig.getConnectionUrl());
        properties.put(WAREHOUSE_LOCATION, defaultWarehouseDir);
        jdbcConfig.getConnectionUser().ifPresent(user -> properties.put(PROPERTY_PREFIX + "user", user));
        jdbcConfig.getConnectionPassword().ifPresent(password -> properties.put(PROPERTY_PREFIX + "password", password));
        this.catalogProperties = properties.buildOrThrow();

        this.clientPool = new JdbcClientPool(jdbcConfig.getConnectionUrl(), catalogProperties);
    }

    @PreDestroy
    public void shutdown()
    {
        clientPool.close();
    }

    @Override
    public TrinoCatalog create(ConnectorIdentity identity)
    {
        JdbcCatalog jdbcCatalog = new JdbcCatalog(
                config -> new ForwardingFileIo(fileSystemFactory.create(identity)),
                config -> clientPool,
                false);

        jdbcCatalog.initialize(jdbcCatalogName, catalogProperties);

        return new TrinoJdbcCatalog(
                catalogName,
                typeManager,
                tableOperationsProvider,
                jdbcCatalog,
                jdbcClient,
                fileSystemFactory,
                isUniqueTableLocation,
                defaultWarehouseDir);
    }
}
