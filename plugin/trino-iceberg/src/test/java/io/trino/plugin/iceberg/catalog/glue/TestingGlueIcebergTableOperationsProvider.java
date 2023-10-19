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

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.glue.AWSGlueAsync;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.hive.metastore.glue.GlueHiveMetastoreConfig;
import io.trino.plugin.hive.metastore.glue.GlueMetastoreStats;
import io.trino.plugin.iceberg.catalog.IcebergTableOperations;
import io.trino.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.fileio.ForwardingFileIo;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.TypeManager;

import java.util.Optional;

import static io.trino.plugin.hive.metastore.glue.GlueClientUtil.createAsyncGlueClient;
import static java.util.Objects.requireNonNull;

public class TestingGlueIcebergTableOperationsProvider
        implements IcebergTableOperationsProvider
{
    private final TypeManager typeManager;
    private final boolean cacheTableMetadata;
    private final TrinoFileSystemFactory fileSystemFactory;
    private final AWSGlueAsync glueClient;
    private final GlueMetastoreStats stats;

    @Inject
    public TestingGlueIcebergTableOperationsProvider(
            TypeManager typeManager,
            IcebergGlueCatalogConfig catalogConfig,
            TrinoFileSystemFactory fileSystemFactory,
            GlueMetastoreStats stats,
            GlueHiveMetastoreConfig glueConfig,
            AWSCredentialsProvider credentialsProvider,
            AWSGlueAsyncAdapterProvider awsGlueAsyncAdapterProvider)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.cacheTableMetadata = catalogConfig.isCacheTableMetadata();
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.stats = requireNonNull(stats, "stats is null");
        requireNonNull(glueConfig, "glueConfig is null");
        requireNonNull(credentialsProvider, "credentialsProvider is null");
        requireNonNull(awsGlueAsyncAdapterProvider, "awsGlueAsyncAdapterProvider is null");
        this.glueClient = awsGlueAsyncAdapterProvider.createAWSGlueAsyncAdapter(
                createAsyncGlueClient(glueConfig, credentialsProvider, ImmutableSet.of(), stats.newRequestMetricsCollector()));
    }

    @Override
    public IcebergTableOperations createTableOperations(
            TrinoCatalog catalog,
            ConnectorSession session,
            String database,
            String table,
            Optional<String> owner,
            Optional<String> location)
    {
        return new GlueIcebergTableOperations(
                typeManager,
                cacheTableMetadata,
                glueClient,
                stats,
                ((TrinoGlueCatalog) catalog)::getTable,
                new ForwardingFileIo(fileSystemFactory.create(session)),
                session,
                database,
                table,
                owner,
                location);
    }
}
