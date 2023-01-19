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
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.iceberg.IcebergConfig;
import io.trino.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.catalog.TrinoCatalogFactory;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.TypeManager;
import org.apache.iceberg.jdbc.JdbcCatalog;

import javax.annotation.concurrent.GuardedBy;
import javax.inject.Inject;

import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.CatalogProperties.URI;
import static org.apache.iceberg.CatalogProperties.WAREHOUSE_LOCATION;

public class TrinoJdbcCatalogFactory
        implements TrinoCatalogFactory
{
    private final CatalogName catalogName;
    private final TypeManager typeManager;
    private final IcebergTableOperationsProvider tableOperationsProvider;
    private final TrinoFileSystemFactory fileSystemFactory;
    private final String jdbcCatalogName;
    private final String connectionUrl;
    private final String defaultWarehouseDir;
    private final boolean isUniqueTableLocation;

    @GuardedBy("this")
    private JdbcCatalog icebergCatalog;

    @Inject
    public TrinoJdbcCatalogFactory(
            CatalogName catalogName,
            TypeManager typeManager,
            IcebergTableOperationsProvider tableOperationsProvider,
            TrinoFileSystemFactory fileSystemFactory,
            IcebergJdbcCatalogConfig jdbcConfig,
            IcebergConfig icebergConfig)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.tableOperationsProvider = requireNonNull(tableOperationsProvider, "tableOperationsProvider is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.isUniqueTableLocation = requireNonNull(icebergConfig, "icebergConfig is null").isUniqueTableLocation();
        this.jdbcCatalogName = jdbcConfig.getCatalogName();
        this.connectionUrl = jdbcConfig.getConnectionUrl();
        this.defaultWarehouseDir = jdbcConfig.getDefaultWarehouseDir();
    }

    @Override
    public synchronized TrinoCatalog create(ConnectorIdentity identity)
    {
        // Reuse JdbcCatalog instance to avoid JDBC connection leaks
        if (icebergCatalog == null) {
            icebergCatalog = createJdbcCatalog();
        }
        return new TrinoJdbcCatalog(
                catalogName,
                typeManager,
                tableOperationsProvider,
                icebergCatalog,
                fileSystemFactory,
                isUniqueTableLocation,
                defaultWarehouseDir);
    }

    private JdbcCatalog createJdbcCatalog()
    {
        JdbcCatalog jdbcCatalog = new JdbcCatalog();
        jdbcCatalog.initialize(jdbcCatalogName, ImmutableMap.<String, String>builder()
                .put(URI, connectionUrl)
                .put(WAREHOUSE_LOCATION, defaultWarehouseDir)
                .buildOrThrow());
        return jdbcCatalog;
    }
}
