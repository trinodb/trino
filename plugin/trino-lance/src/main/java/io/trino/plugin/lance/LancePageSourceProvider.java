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
package io.trino.plugin.lance;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInputFile;
import io.trino.lance.file.LanceDataSource;
import io.trino.lance.file.LanceReader;
import io.trino.lance.file.TrinoLanceDataSource;
import io.trino.lance.file.v2.reader.Range;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.lance.metadata.Fragment;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.plugin.lance.LanceErrorCode.LANCE_BAD_DATA;
import static io.trino.plugin.lance.LanceErrorCode.LANCE_SPLIT_ERROR;
import static io.trino.plugin.lance.catalog.BaseTable.DATA_DIR;
import static io.trino.plugin.lance.catalog.BaseTable.LANCE_SUFFIX;
import static java.util.Objects.requireNonNull;

public class LancePageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final TrinoFileSystemFactory fileSystemFactory;
    private final FileFormatDataSourceStats stats;

    @Inject
    public LancePageSourceProvider(
            TrinoFileSystemFactory fileSystemFactory,
            FileFormatDataSourceStats stats)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.stats = requireNonNull(stats, "stats is null");
    }

    public ConnectorPageSource createFilePageSource(
            ConnectorSession session,
            Location path,
            List<ColumnHandle> columns,
            long start,
            long end)
    {
        if (!path.fileName().endsWith(LANCE_SUFFIX)) {
            throw new TrinoException(LANCE_BAD_DATA, "Unsupported file suffix: path=%s".formatted(path.fileName()));
        }
        LanceDataSource lanceDataSource;
        try {
            TrinoFileSystem fileSystem = fileSystemFactory.create(session);
            TrinoInputFile inputFile = fileSystem.newInputFile(path);
            lanceDataSource = new TrinoLanceDataSource(inputFile, stats);
            List<Integer> readColumnIds = columns.stream()
                    .map(LanceColumnHandle.class::cast)
                    .map(LanceColumnHandle::id)
                    .collect(toImmutableList());
            AggregatedMemoryContext memoryUsage = newSimpleAggregatedMemoryContext();
            LanceReader reader = new LanceReader(lanceDataSource, readColumnIds, Optional.of(ImmutableList.of(new Range(start, end))), memoryUsage);
            return new LancePageSource(reader, lanceDataSource, memoryUsage);
        }
        catch (IOException e) {
            throw new TrinoException(LANCE_SPLIT_ERROR, e);
        }
    }

    public ConnectorPageSource createFragmentPageSource(ConnectorSession session,
            LanceTableHandle table,
            Fragment fragment,
            List<ColumnHandle> columns,
            long start,
            long end)
    {
        // TODO: support multiple files in a fragment
        checkArgument(fragment.files().size() == 1, "only one file per fragment is supported");
        Fragment.DataFile dataFile = fragment.files().getFirst();
        return createFilePageSource(session, Location.of(Joiner.on("/").join(table.tablePath(), DATA_DIR, dataFile.path())), columns, start, end);
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit connectorSplit,
            ConnectorTableHandle table,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter)
    {
        checkArgument(table instanceof LanceTableHandle);
        checkArgument(connectorSplit instanceof LanceSplit);
        LanceTableHandle lanceTable = (LanceTableHandle) table;
        LanceSplit split = (LanceSplit) connectorSplit;
        return createFragmentPageSource(session, lanceTable, split.fragment(), columns, split.startRowPosition(), split.endRowPosition());
    }
}
