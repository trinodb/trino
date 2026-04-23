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
package io.trino.plugin.hudi;

import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.hudi.file.HudiBaseFile;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableCredentials;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.EmptyPageSource;

import java.util.List;
import java.util.Optional;

public class HudiBenchmarkPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private static final Logger log = Logger.get(HudiPageSourceProvider.class);
    public static final java.util.concurrent.atomic.AtomicLong SLEEP_MS = new java.util.concurrent.atomic.AtomicLong(10);
    public static final java.util.concurrent.atomic.AtomicLong SPLITS_PROCESSED = new java.util.concurrent.atomic.AtomicLong(0);

    @Inject
    public HudiBenchmarkPageSourceProvider(
            TrinoFileSystemFactory fileSystemFactory,
            FileFormatDataSourceStats dataSourceStats,
            ParquetReaderConfig parquetReaderConfig)
    {
        // no op
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit connectorSplit,
            ConnectorTableHandle connectorTable,
            Optional<ConnectorTableCredentials> tableCredentials,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter)
    {
        HudiSplit hudiSplit = (HudiSplit) connectorSplit;
        Optional<HudiBaseFile> hudiBaseFileOpt = hudiSplit.getBaseFile();

        String dataFilePath = hudiBaseFileOpt.isPresent()
                ? hudiBaseFileOpt.get().getPath()
                : hudiSplit.getLogFiles().getFirst().getPath();
        // Filter out metadata table splits
        // TODO: Move this check into a higher calling stack, such that the split is not created at all
        SPLITS_PROCESSED.incrementAndGet();
        try {
            Thread.sleep(SLEEP_MS.get());
        }
        catch (Exception e) {
            log.error("Failed while waiting to read the file");
        }
        return new EmptyPageSource();
    }
}
