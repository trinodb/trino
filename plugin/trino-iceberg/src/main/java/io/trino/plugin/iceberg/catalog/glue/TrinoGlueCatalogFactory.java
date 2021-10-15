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
package io.trino.plugin.iceberg.catalog.glue;

import com.amazonaws.services.glue.AWSGlueAsync;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.metastore.glue.GlueHiveMetastoreConfig;
import io.trino.plugin.hive.metastore.glue.GlueMetastoreStats;
import io.trino.plugin.iceberg.IcebergConfig;
import io.trino.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.catalog.TrinoCatalogFactory;

import javax.inject.Inject;

import java.util.Optional;

import static io.trino.plugin.hive.metastore.glue.GlueHiveMetastore.createAsyncGlueClient;
import static java.util.Objects.requireNonNull;

public class TrinoGlueCatalogFactory
        implements TrinoCatalogFactory
{
    private final HdfsEnvironment hdfsEnvironment;
    private final IcebergTableOperationsProvider tableOperationsProvider;
    private final String warehouse;
    private final boolean isUniqueTableLocation;
    private final AWSGlueAsync glueClient;
    private final String catalogId;
    private final GlueMetastoreStats stats = new GlueMetastoreStats();

    @Inject
    public TrinoGlueCatalogFactory(
            HdfsEnvironment hdfsEnvironment,
            IcebergTableOperationsProvider tableOperationsProvider,
            GlueHiveMetastoreConfig glueConfig,
            IcebergConfig icebergConfig)
    {
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.tableOperationsProvider = requireNonNull(tableOperationsProvider, "tableOperationsProvider is null");
        requireNonNull(glueConfig, "glueConfig is null");
        this.glueClient = createAsyncGlueClient(glueConfig, Optional.empty(), stats.newRequestMetricsCollector());
        this.catalogId = glueConfig.getCatalogId().orElse(null);
        requireNonNull(icebergConfig, "icebergConfig is null");
        this.warehouse = icebergConfig.getCatalogWarehouse();
        this.isUniqueTableLocation = icebergConfig.isUniqueTableLocation();
    }

    public TrinoCatalog create()
    {
        return new TrinoGlueCatalog(hdfsEnvironment, tableOperationsProvider, glueClient, stats, catalogId, warehouse, isUniqueTableLocation);
    }
}
