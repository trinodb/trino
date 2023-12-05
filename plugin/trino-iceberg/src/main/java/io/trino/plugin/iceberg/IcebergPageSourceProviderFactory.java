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

import com.google.inject.Inject;
import io.trino.orc.OrcReaderOptions;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.hive.orc.OrcReaderConfig;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorPageSourceProviderFactory;
import io.trino.spi.type.TypeManager;

import static java.util.Objects.requireNonNull;

public class IcebergPageSourceProviderFactory
        implements ConnectorPageSourceProviderFactory
{
    private final IcebergFileSystemFactory fileSystemFactory;
    private final FileFormatDataSourceStats fileFormatDataSourceStats;
    private final OrcReaderOptions orcReaderOptions;
    private final ParquetReaderOptions parquetReaderOptions;
    private final TypeManager typeManager;

    @Inject
    public IcebergPageSourceProviderFactory(
            IcebergFileSystemFactory fileSystemFactory,
            FileFormatDataSourceStats fileFormatDataSourceStats,
            OrcReaderConfig orcReaderConfig,
            ParquetReaderConfig parquetReaderConfig,
            TypeManager typeManager)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.fileFormatDataSourceStats = requireNonNull(fileFormatDataSourceStats, "fileFormatDataSourceStats is null");
        this.orcReaderOptions = orcReaderConfig.toOrcReaderOptions();
        this.parquetReaderOptions = parquetReaderConfig.toParquetReaderOptions();
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public ConnectorPageSourceProvider createPageSourceProvider()
    {
        return new IcebergPageSourceProvider(fileSystemFactory, fileFormatDataSourceStats, orcReaderOptions, parquetReaderOptions, typeManager);
    }
}
