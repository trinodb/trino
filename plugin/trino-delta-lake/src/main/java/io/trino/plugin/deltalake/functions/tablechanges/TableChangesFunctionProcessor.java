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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInputFile;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.plugin.deltalake.DeltaLakeColumnHandle;
import io.trino.plugin.deltalake.DeltaLakeConfig;
import io.trino.plugin.deltalake.DeltaLakePageSource;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.hive.ReaderPageSource;
import io.trino.plugin.hive.parquet.ParquetPageSourceFactory;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.ptf.TableFunctionProcessState;
import io.trino.spi.ptf.TableFunctionProcessState.TableFunctionResult;
import io.trino.spi.ptf.TableFunctionSplitProcessor;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.deltalake.DeltaLakeCdfPageSink.CHANGE_TYPE_COLUMN_NAME;
import static io.trino.plugin.deltalake.DeltaLakeColumnType.REGULAR;
import static io.trino.plugin.deltalake.functions.tablechanges.FileSource.CDF_FILE;
import static io.trino.plugin.deltalake.functions.tablechanges.FileSource.DATA_FILE;
import static io.trino.spi.predicate.Utils.nativeValueToBlock;
import static io.trino.spi.ptf.TableFunctionProcessState.Type.FINISHED;
import static io.trino.spi.ptf.TableFunctionProcessState.Type.FORWARD;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class TableChangesFunctionProcessor
        implements TableFunctionSplitProcessor
{
    private final TrinoFileSystemFactory fileSystemFactory;
    private final DeltaLakeConfig deltaLakeConfig;
    private final List<DeltaLakeColumnHandle> dataFilesColumns;
    private final List<DeltaLakeColumnHandle> cdfFilesColumns;
    private DeltaLakePageSource deltaLakePageSource;

    public TableChangesFunctionProcessor(
            TrinoFileSystemFactory fileSystemFactory,
            DeltaLakeConfig deltaLakeConfig,
            TableChangesFunctionTableHandle handle)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.deltaLakeConfig = requireNonNull(deltaLakeConfig, "deltaLakeConfig is null");
        this.dataFilesColumns = requireNonNull(handle, "handle is null").getColumns();
        ImmutableList.Builder<DeltaLakeColumnHandle> cdfFilesColumns = ImmutableList.builder();
        this.cdfFilesColumns = cdfFilesColumns.addAll(handle.getColumns())
                .add(new DeltaLakeColumnHandle(
                        CHANGE_TYPE_COLUMN_NAME,
                        VARCHAR,
                        OptionalInt.empty(),
                        CHANGE_TYPE_COLUMN_NAME,
                        VARCHAR,
                        REGULAR))
                .build();
    }

    @Override
    public TableFunctionProcessState process(ConnectorSession session, ConnectorSplit connectorSplit)
    {
        if (connectorSplit instanceof DeltaLakeTableChangesSplit) {
            DeltaLakeTableChangesSplit split = (DeltaLakeTableChangesSplit) connectorSplit;
            if (split.getSource() == CDF_FILE) {
                return processCdfFile(session, split);
            }
            else {
                return processDataFile(session, split);
            }
        }
        return new TableFunctionProcessState(FINISHED, false, null, null);
    }

    private TableFunctionProcessState processCdfFile(ConnectorSession session, DeltaLakeTableChangesSplit split)
    {
        List<DeltaLakeColumnHandle> columns = cdfFilesColumns;
        if (deltaLakePageSource == null) {
            deltaLakePageSource = createDeltaLakePageSource(session, split, columns);
        }

        Page page = deltaLakePageSource.getNextPage();
        if (page != null) {
            int passThroughColumns = columns.size();
            Block[] resultBlock = new Block[passThroughColumns + CDF_FILE.getNumberOfAdditionalColumns()];
            for (int i = 0; i < columns.size(); i++) {
                resultBlock[i] = page.getBlock(i);
            }
            resultBlock[passThroughColumns] = RunLengthEncodedBlock.create(
                    nativeValueToBlock(INTEGER, split.getVersion()), page.getPositionCount());
            long commitTimestamp = Instant.ofEpochMilli(split.getCommitTimestamp()).toEpochMilli();
            resultBlock[passThroughColumns + 1] = RunLengthEncodedBlock.create(
                    nativeValueToBlock(TIMESTAMP_TZ_MILLIS, packDateTimeWithZone(commitTimestamp, UTC_KEY)), page.getPositionCount());
            return new TableFunctionProcessState(FORWARD, false, new TableFunctionResult(new Page(page.getPositionCount(), resultBlock)), null);
        }
        return new TableFunctionProcessState(FINISHED, false, null, null);
    }

    private TableFunctionProcessState processDataFile(ConnectorSession session, DeltaLakeTableChangesSplit split)
    {
        List<DeltaLakeColumnHandle> columns = dataFilesColumns;
        if (deltaLakePageSource == null) {
            deltaLakePageSource = createDeltaLakePageSource(session, split, columns);
        }

        Page page = deltaLakePageSource.getNextPage();
        if (page != null) {
            int passThroughColumns = columns.size();
            Block[] resultBlock = new Block[passThroughColumns + DATA_FILE.getNumberOfAdditionalColumns()];
            for (int i = 0; i < columns.size(); i++) {
                resultBlock[i] = page.getBlock(i);
            }
            resultBlock[passThroughColumns] = RunLengthEncodedBlock.create(
                    nativeValueToBlock(VARCHAR, utf8Slice("insert")), page.getPositionCount());
            resultBlock[passThroughColumns + 1] = RunLengthEncodedBlock.create(
                    nativeValueToBlock(INTEGER, split.getVersion()), page.getPositionCount());
            long commitTimestamp = Instant.ofEpochMilli(split.getCommitTimestamp()).toEpochMilli();
            resultBlock[passThroughColumns + 2] = RunLengthEncodedBlock.create(
                    nativeValueToBlock(TIMESTAMP_TZ_MILLIS, packDateTimeWithZone(commitTimestamp, UTC_KEY)), page.getPositionCount());
            return new TableFunctionProcessState(FORWARD, false, new TableFunctionResult(new Page(page.getPositionCount(), resultBlock)), null);
        }
        return new TableFunctionProcessState(FINISHED, false, null, null);
    }

    private DeltaLakePageSource createDeltaLakePageSource(ConnectorSession session, DeltaLakeTableChangesSplit split, List<DeltaLakeColumnHandle> columns)
    {
        TrinoFileSystem fileSystem = fileSystemFactory.create(session);
        TrinoInputFile inputFile = fileSystem.newInputFile(split.getPath());
        Map<String, Optional<String>> partitionKeys = split.getPartitionKeys();
        List<String> partitionValues = new ArrayList<>();
        for (DeltaLakeColumnHandle column : columns) {
            Optional<String> value = partitionKeys.get(column.getName());
            if (value != null) {
                partitionValues.add(value.orElse(null));
            }
        }
        ReaderPageSource pageSource = ParquetPageSourceFactory.createPageSource(
                inputFile,
                split.getStart(),
                split.getLength(),
                columns.stream().filter(column -> column.getColumnType() == REGULAR).map(DeltaLakeColumnHandle::toHiveColumnHandle).collect(toImmutableList()),
                TupleDomain.all(),
                true,
                deltaLakeConfig.getParquetDateTimeZone(),
                new FileFormatDataSourceStats(),
                new ParquetReaderOptions().withBloomFilter(false),
                Optional.empty(),
                deltaLakeConfig.getDomainCompactionThreshold());
        return new DeltaLakePageSource(
                columns,
                ImmutableSet.of(),
                partitionKeys,
                partitionValues,
                pageSource.get(),
                split.getPath(),
                split.getFileSize(),
                split.getFileModifiedTime());
    }
}
