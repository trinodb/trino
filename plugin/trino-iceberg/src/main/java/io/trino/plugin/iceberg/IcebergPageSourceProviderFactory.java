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

import com.google.common.cache.Cache;
import com.google.inject.Inject;
import io.trino.cache.EvictableCacheBuilder;
import io.trino.orc.OrcReaderOptions;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.hive.orc.OrcReaderConfig;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.iceberg.catalog.TrinoCatalogFactory;
import io.trino.plugin.iceberg.fileio.ForwardingFileIoFactory;
import io.trino.spi.connector.ConnectorPageSourceProviderFactory;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.TypeManager;

import static io.trino.plugin.iceberg.IcebergUtil.WORKER_CREDENTIAL_CACHE_TTL;
import static java.util.Objects.requireNonNull;

public class IcebergPageSourceProviderFactory
        implements ConnectorPageSourceProviderFactory
{
    private final IcebergFileSystemFactory fileSystemFactory;
    private final ForwardingFileIoFactory fileIoFactory;
    private final FileFormatDataSourceStats fileFormatDataSourceStats;
    private final OrcReaderOptions orcReaderOptions;
    private final ParquetReaderOptions parquetReaderOptions;
    private final TypeManager typeManager;
    private final TrinoCatalogFactory catalogFactory;
    // Shared across all providers created by this factory so that concurrent tasks on the
    // same worker do not all call the REST catalog when credentials need refreshing.
    // Size-bounded to cap memory even when many tables are being scanned simultaneously.
    private final Cache<SchemaTableName, IcebergTableCredentials> refreshedCredentialCache =
            EvictableCacheBuilder.newBuilder()
                    .maximumSize(1_000)
                    .expireAfterWrite(WORKER_CREDENTIAL_CACHE_TTL)
                    .shareNothingWhenDisabled()
                    .build();

    @Inject
    public IcebergPageSourceProviderFactory(
            IcebergFileSystemFactory fileSystemFactory,
            ForwardingFileIoFactory fileIoFactory,
            FileFormatDataSourceStats fileFormatDataSourceStats,
            OrcReaderConfig orcReaderConfig,
            ParquetReaderConfig parquetReaderConfig,
            TypeManager typeManager,
            TrinoCatalogFactory catalogFactory)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.fileIoFactory = requireNonNull(fileIoFactory, "fileIoFactory is null");
        this.fileFormatDataSourceStats = requireNonNull(fileFormatDataSourceStats, "fileFormatDataSourceStats is null");
        this.orcReaderOptions = orcReaderConfig.toOrcReaderOptions();
        this.parquetReaderOptions = parquetReaderConfig.toParquetReaderOptions();
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.catalogFactory = requireNonNull(catalogFactory, "catalogFactory is null");
    }

    @Override
    public IcebergPageSourceProvider createPageSourceProvider()
    {
        return new IcebergPageSourceProvider(fileSystemFactory, fileIoFactory, fileFormatDataSourceStats, orcReaderOptions, parquetReaderOptions, typeManager, catalogFactory, refreshedCredentialCache);
    }
}
