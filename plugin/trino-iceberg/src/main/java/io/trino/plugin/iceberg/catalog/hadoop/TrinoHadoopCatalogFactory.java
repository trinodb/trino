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
package io.trino.plugin.iceberg.catalog.hadoop;

import com.google.inject.Inject;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.hive.NodeVersion;
import io.trino.plugin.iceberg.IcebergConfig;
import io.trino.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.catalog.TrinoCatalogFactory;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.TypeManager;

import static java.util.Objects.requireNonNull;

public class TrinoHadoopCatalogFactory
        implements TrinoCatalogFactory
{
    private final IcebergConfig config;
    private final CatalogName catalogName;
    private final HdfsEnvironment hdfsEnvironment;
    private final TypeManager typeManager;
    private final IcebergTableOperationsProvider tableOperationsProvider;
    private final boolean isUniqueTableLocation;

    @Inject
    public TrinoHadoopCatalogFactory(
            IcebergConfig config,
            CatalogName catalogName,
            HdfsEnvironment hdfsEnvironment,
            TypeManager typeManager,
            IcebergTableOperationsProvider tableOperationsProvider,
            NodeVersion nodeVersion)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.tableOperationsProvider = requireNonNull(tableOperationsProvider, "tableOperationProvider is null");
        requireNonNull(config, "config is null");
        this.config = config;
        this.isUniqueTableLocation = config.isUniqueTableLocation();
    }

    @Override
    public TrinoCatalog create(ConnectorIdentity identity)
    {
        return new TrinoHadoopCatalog(
                catalogName,
                hdfsEnvironment,
                typeManager,
                tableOperationsProvider,
                isUniqueTableLocation,
                config);
    }
}
