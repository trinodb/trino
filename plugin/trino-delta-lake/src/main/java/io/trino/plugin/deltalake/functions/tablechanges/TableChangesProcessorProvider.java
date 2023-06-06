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
package io.trino.plugin.deltalake.functions.tablechanges;

import com.google.inject.Inject;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.plugin.deltalake.DeltaLakeConfig;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.function.table.TableFunctionProcessorProvider;
import io.trino.spi.function.table.TableFunctionSplitProcessor;
import org.joda.time.DateTimeZone;

import static java.util.Objects.requireNonNull;

public class TableChangesProcessorProvider
        implements TableFunctionProcessorProvider
{
    private final TrinoFileSystemFactory fileSystemFactory;
    private final DateTimeZone parquetDateTimeZone;
    private final int domainCompactionThreshold;
    private final FileFormatDataSourceStats fileFormatDataSourceStats;
    private final ParquetReaderOptions parquetReaderOptions;

    @Inject
    public TableChangesProcessorProvider(
            TrinoFileSystemFactory fileSystemFactory,
            DeltaLakeConfig deltaLakeConfig,
            FileFormatDataSourceStats fileFormatDataSourceStats,
            ParquetReaderConfig parquetReaderConfig)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.parquetDateTimeZone = deltaLakeConfig.getParquetDateTimeZone();
        this.domainCompactionThreshold = deltaLakeConfig.getDomainCompactionThreshold();
        this.fileFormatDataSourceStats = requireNonNull(fileFormatDataSourceStats, "fileFormatDataSourceStats is null");
        this.parquetReaderOptions = parquetReaderConfig.toParquetReaderOptions();
    }

    @Override
    public TableFunctionSplitProcessor getSplitProcessor(ConnectorSession session, ConnectorTableFunctionHandle handle, ConnectorSplit split)
    {
        return new TableChangesFunctionProcessor(
                session,
                fileSystemFactory,
                parquetDateTimeZone,
                domainCompactionThreshold,
                fileFormatDataSourceStats,
                parquetReaderOptions,
                (TableChangesTableFunctionHandle) handle,
                (TableChangesSplit) split);
    }
}
