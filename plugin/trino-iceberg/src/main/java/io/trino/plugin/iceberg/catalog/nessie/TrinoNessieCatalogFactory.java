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
package io.trino.plugin.iceberg.catalog.nessie;

import com.google.inject.Inject;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.iceberg.IcebergConfig;
import io.trino.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.catalog.TrinoCatalogFactory;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.TypeManager;
import org.apache.iceberg.nessie.NessieIcebergClient;

import static java.util.Objects.requireNonNull;

public class TrinoNessieCatalogFactory
        implements TrinoCatalogFactory
{
    private final IcebergTableOperationsProvider tableOperationsProvider;
    private final String warehouseLocation;
    private final NessieIcebergClient nessieClient;
    private final boolean isUniqueTableLocation;
    private final CatalogName catalogName;
    private final TypeManager typeManager;
    private final TrinoFileSystemFactory fileSystemFactory;

    @Inject
    public TrinoNessieCatalogFactory(
            CatalogName catalogName,
            TypeManager typeManager,
            TrinoFileSystemFactory fileSystemFactory,
            IcebergTableOperationsProvider tableOperationsProvider,
            NessieIcebergClient nessieClient,
            IcebergNessieCatalogConfig icebergNessieCatalogConfig,
            IcebergConfig icebergConfig)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.tableOperationsProvider = requireNonNull(tableOperationsProvider, "tableOperationsProvider is null");
        this.nessieClient = requireNonNull(nessieClient, "nessieClient is null");
        this.warehouseLocation = icebergNessieCatalogConfig.getDefaultWarehouseDir();
        this.isUniqueTableLocation = icebergConfig.isUniqueTableLocation();
    }

    @Override
    public TrinoCatalog create(ConnectorIdentity identity)
    {
        return new TrinoNessieCatalog(catalogName, typeManager, fileSystemFactory, tableOperationsProvider, nessieClient, warehouseLocation, isUniqueTableLocation);
    }
}
