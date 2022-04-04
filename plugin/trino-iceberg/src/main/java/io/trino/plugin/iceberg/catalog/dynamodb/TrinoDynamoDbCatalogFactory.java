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
package io.trino.plugin.iceberg.catalog.dynamodb;

import io.trino.plugin.base.CatalogName;
import io.trino.plugin.hive.NodeVersion;
import io.trino.plugin.iceberg.IcebergConfig;
import io.trino.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.catalog.TrinoCatalogFactory;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.TypeManager;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

public class TrinoDynamoDbCatalogFactory
        implements TrinoCatalogFactory
{
    private final CatalogName catalogName;
    private final TypeManager typeManager;
    private final IcebergTableOperationsProvider tableOperationsProvider;
    private final DynamoDbClientFactory dynamoDbClientFactory;
    private final String trinoVersion;
    private final String defaultWarehouseDir;
    private final boolean isUniqueTableLocation;

    @Inject
    public TrinoDynamoDbCatalogFactory(
            CatalogName catalogName,
            TypeManager typeManager,
            IcebergTableOperationsProvider tableOperationsProvider,
            NodeVersion nodeVersion,
            DynamoDbClientFactory dynamoDbClientFactory,
            DynamoDbIcebergConfig dynamoConfig,
            IcebergConfig icebergConfig)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.tableOperationsProvider = requireNonNull(tableOperationsProvider, "tableOperationsProvider is null");
        this.dynamoDbClientFactory = requireNonNull(dynamoDbClientFactory, "dynamoDbClientFactory is null");
        this.trinoVersion = requireNonNull(nodeVersion, "nodeVersion is null").toString();
        this.defaultWarehouseDir = requireNonNull(dynamoConfig, "dynamoConfig is null").getDefaultWarehouseDir();
        this.isUniqueTableLocation = requireNonNull(icebergConfig, "icebergConfig is null").isUniqueTableLocation();
    }

    @Override
    public TrinoCatalog create(ConnectorIdentity identity)
    {
        return new TrinoDynamoDbCatalog(
                catalogName,
                typeManager,
                tableOperationsProvider,
                dynamoDbClientFactory,
                trinoVersion,
                isUniqueTableLocation,
                defaultWarehouseDir);
    }
}
