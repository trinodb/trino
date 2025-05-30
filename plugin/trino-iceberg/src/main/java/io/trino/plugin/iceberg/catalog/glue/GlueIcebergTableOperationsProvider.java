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

import com.google.inject.Inject;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.hive.metastore.glue.GlueMetastoreStats;
import io.trino.plugin.iceberg.catalog.IcebergTableOperations;
import io.trino.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.fileio.ForwardingFileIo;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.TypeManager;
import software.amazon.awssdk.services.glue.GlueClient;

import java.util.Optional;

import static io.trino.plugin.iceberg.IcebergSessionProperties.isUseFileSizeFromMetadata;
import static java.util.Objects.requireNonNull;

public class GlueIcebergTableOperationsProvider
        implements IcebergTableOperationsProvider
{
    private final TypeManager typeManager;
    private final boolean cacheTableMetadata;
    private final TrinoFileSystemFactory fileSystemFactory;
    private final GlueClient glueClient;
    private final GlueMetastoreStats stats;

    @Inject
    public GlueIcebergTableOperationsProvider(
            TypeManager typeManager,
            IcebergGlueCatalogConfig catalogConfig,
            TrinoFileSystemFactory fileSystemFactory,
            GlueMetastoreStats stats,
            GlueClient glueClient)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.cacheTableMetadata = catalogConfig.isCacheTableMetadata();
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.stats = requireNonNull(stats, "stats is null");
        this.glueClient = requireNonNull(glueClient, "glueClient is null");
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
                // Share Glue Table cache between Catalog and TableOperations so that, when doing metadata queries (e.g. information_schema.columns)
                // the GetTableRequest is issued once per table.
                ((TrinoGlueCatalog) catalog)::getTable,
                new ForwardingFileIo(fileSystemFactory.create(session), isUseFileSizeFromMetadata(session)),
                session,
                database,
                table,
                owner,
                location);
    }
}
