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
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.TypeManager;
import org.weakref.jmx.Flatten;
import org.weakref.jmx.Managed;

import javax.inject.Inject;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.hive.metastore.glue.GlueHiveMetastore.createAsyncGlueClient;
import static java.util.Objects.requireNonNull;

public class TrinoGlueCatalogFactory
        implements TrinoCatalogFactory
{
    private final HdfsEnvironment hdfsEnvironment;
    private final TypeManager typeManager;
    private final IcebergTableOperationsProvider tableOperationsProvider;
    private final Optional<String> defaultSchemaLocation;
    private final AWSGlueAsync glueClient;
    private final boolean isUniqueTableLocation;
    private final GlueMetastoreStats stats = new GlueMetastoreStats();

    @Inject
    public TrinoGlueCatalogFactory(
            HdfsEnvironment hdfsEnvironment,
            TypeManager typeManager,
            IcebergTableOperationsProvider tableOperationsProvider,
            GlueHiveMetastoreConfig glueConfig,
            IcebergConfig icebergConfig)
    {
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.tableOperationsProvider = requireNonNull(tableOperationsProvider, "tableOperationsProvider is null");
        requireNonNull(glueConfig, "glueConfig is null");
        checkArgument(glueConfig.getCatalogId().isEmpty(), "catalogId configuration is not supported");
        this.defaultSchemaLocation = glueConfig.getDefaultWarehouseDir();
        this.glueClient = createAsyncGlueClient(glueConfig, Optional.empty(), stats.newRequestMetricsCollector());
        requireNonNull(icebergConfig, "icebergConfig is null");
        this.isUniqueTableLocation = icebergConfig.isUniqueTableLocation();
    }

    @Managed
    @Flatten
    public GlueMetastoreStats getStats()
    {
        return stats;
    }

    @Override
    public TrinoCatalog create(ConnectorIdentity identity)
    {
        return new TrinoGlueCatalog(hdfsEnvironment, typeManager, tableOperationsProvider, glueClient, stats, defaultSchemaLocation, isUniqueTableLocation);
    }
}
