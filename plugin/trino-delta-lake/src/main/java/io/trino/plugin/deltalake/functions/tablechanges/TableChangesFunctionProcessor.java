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
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInputFile;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.plugin.deltalake.DeltaLakeColumnHandle;
import io.trino.plugin.deltalake.DeltaLakePageSource;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.hive.ReaderPageSource;
import io.trino.plugin.hive.parquet.ParquetPageSourceFactory;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.table.TableFunctionProcessorState;
import io.trino.spi.function.table.TableFunctionSplitProcessor;
import io.trino.spi.predicate.TupleDomain;
import org.joda.time.DateTimeZone;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.deltalake.DeltaLakeCdfPageSink.CHANGE_TYPE_COLUMN_NAME;
import static io.trino.plugin.deltalake.DeltaLakeColumnType.REGULAR;
import static io.trino.plugin.deltalake.DeltaLakeSessionProperties.getParquetMaxReadBlockRowCount;
import static io.trino.plugin.deltalake.DeltaLakeSessionProperties.getParquetMaxReadBlockSize;
import static io.trino.plugin.deltalake.DeltaLakeSessionProperties.isParquetOptimizedNestedReaderEnabled;
import static io.trino.plugin.deltalake.DeltaLakeSessionProperties.isParquetOptimizedReaderEnabled;
import static io.trino.plugin.deltalake.DeltaLakeSessionProperties.isParquetUseColumnIndex;
import static io.trino.plugin.deltalake.functions.tablechanges.TableChangesFileType.CDF_FILE;
import static io.trino.spi.function.table.TableFunctionProcessorState.Finished.FINISHED;
import static io.trino.spi.predicate.Utils.nativeValueToBlock;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class TableChangesFunctionProcessor
        implements TableFunctionSplitProcessor
{
    private static final int NUMBER_OF_ADDITIONAL_COLUMNS_FOR_CDF_FILE = 2;
    private static final int NUMBER_OF_ADDITIONAL_COLUMNS_FOR_DATA_FILE = 3;

    private final ConnectorSession session;
    private final TrinoFileSystemFactory fileSystemFactory;
    private final DateTimeZone parquetDateTimeZone;
    private final int domainCompactionThreshold;
    private final FileFormatDataSourceStats fileFormatDataSourceStats;
    private final ParquetReaderOptions parquetReaderOptions;
    private final List<DeltaLakeColumnHandle> dataFilesColumns;
    private final List<DeltaLakeColumnHandle> cdfFilesColumns;

    private TableChangesFileType fileType;
    private DeltaLakePageSource deltaLakePageSource;
    private List<DeltaLakeColumnHandle> splitColumns;
    private Block currentVersionAsBlock;
    private Block currentVersionCommitTimestampAsBlock;

    public TableChangesFunctionProcessor(
            ConnectorSession session,
            TrinoFileSystemFactory fileSystemFactory,
            DateTimeZone parquetDateTimeZone,
            int domainCompactionThreshold,
            FileFormatDataSourceStats fileFormatDataSourceStats,
            ParquetReaderOptions parquetReaderOptions,
            TableChangesTableFunctionHandle handle,
            TableChangesSplit tableChangesSplit)
    {
        this.session = requireNonNull(session, "session is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.parquetDateTimeZone = requireNonNull(parquetDateTimeZone, "parquetDateTimeZone is null");
        this.domainCompactionThreshold = domainCompactionThreshold;
        this.fileFormatDataSourceStats = requireNonNull(fileFormatDataSourceStats, "fileFormatDataSourceStats is null");
        this.dataFilesColumns = handle.columns();
        this.cdfFilesColumns = ImmutableList.<DeltaLakeColumnHandle>builder().addAll(handle.columns())
                .add(new DeltaLakeColumnHandle(
                        CHANGE_TYPE_COLUMN_NAME,
                        VARCHAR,
                        OptionalInt.empty(),
                        CHANGE_TYPE_COLUMN_NAME,
                        VARCHAR,
                        REGULAR,
                        Optional.empty()))
                .build();
        this.parquetReaderOptions = requireNonNull(parquetReaderOptions, "parquetReaderOptions is null")
                .withMaxReadBlockSize(getParquetMaxReadBlockSize(session))
                .withMaxReadBlockRowCount(getParquetMaxReadBlockRowCount(session))
                .withUseColumnIndex(isParquetUseColumnIndex(session))
                .withBatchColumnReaders(isParquetOptimizedReaderEnabled(session))
                .withBatchNestedColumnReaders(isParquetOptimizedNestedReaderEnabled(session));
        fileType = tableChangesSplit.fileType();
        if (fileType == CDF_FILE) {
            splitColumns = cdfFilesColumns;
        }
        else {
            splitColumns = dataFilesColumns;
        }
        deltaLakePageSource = createDeltaLakePageSource(tableChangesSplit);
        currentVersionAsBlock = nativeValueToBlock(BIGINT, tableChangesSplit.currentVersion());
        currentVersionCommitTimestampAsBlock = nativeValueToBlock(TIMESTAMP_TZ_MILLIS, packDateTimeWithZone(tableChangesSplit.currentVersionCommitTimestamp(), UTC_KEY));
    }

    @Override
    public TableFunctionProcessorState process()
    {
        if (fileType == CDF_FILE) {
            return processCdfFile();
        }
        return processDataFile();
    }

    private TableFunctionProcessorState processCdfFile()
    {
        Page page = deltaLakePageSource.getNextPage();
        if (page != null) {
            int filePageColumns = page.getChannelCount();
            Block[] resultBlock = new Block[filePageColumns + NUMBER_OF_ADDITIONAL_COLUMNS_FOR_CDF_FILE];
            for (int i = 0; i < filePageColumns; i++) {
                resultBlock[i] = page.getBlock(i);
            }
            resultBlock[filePageColumns] = RunLengthEncodedBlock.create(
                    currentVersionAsBlock, page.getPositionCount());
            resultBlock[filePageColumns + 1] = RunLengthEncodedBlock.create(
                    currentVersionCommitTimestampAsBlock, page.getPositionCount());
            return TableFunctionProcessorState.Processed.produced(new Page(page.getPositionCount(), resultBlock));
        }
        return FINISHED;
    }

    private TableFunctionProcessorState processDataFile()
    {
        Page page = deltaLakePageSource.getNextPage();
        if (page != null) {
            int filePageColumns = page.getChannelCount();
            Block[] blocks = new Block[filePageColumns + NUMBER_OF_ADDITIONAL_COLUMNS_FOR_DATA_FILE];
            for (int i = 0; i < filePageColumns; i++) {
                blocks[i] = page.getBlock(i);
            }
            blocks[filePageColumns] = RunLengthEncodedBlock.create(
                    nativeValueToBlock(VARCHAR, utf8Slice("insert")), page.getPositionCount());
            blocks[filePageColumns + 1] = RunLengthEncodedBlock.create(
                    currentVersionAsBlock, page.getPositionCount());
            blocks[filePageColumns + 2] = RunLengthEncodedBlock.create(
                    currentVersionCommitTimestampAsBlock, page.getPositionCount());
            return TableFunctionProcessorState.Processed.produced(new Page(page.getPositionCount(), blocks));
        }
        return FINISHED;
    }

    private DeltaLakePageSource createDeltaLakePageSource(TableChangesSplit split)
    {
        TrinoFileSystem fileSystem = fileSystemFactory.create(session);
        TrinoInputFile inputFile = fileSystem.newInputFile(Location.of(split.path()), split.fileSize());
        Map<String, Optional<String>> partitionKeys = split.partitionKeys();

        ReaderPageSource pageSource = ParquetPageSourceFactory.createPageSource(
                inputFile,
                0,
                split.fileSize(),
                splitColumns.stream().filter(column -> column.getColumnType() == REGULAR).map(DeltaLakeColumnHandle::toHiveColumnHandle).collect(toImmutableList()),
                TupleDomain.all(), // TODO add predicate pushdown https://github.com/trinodb/trino/issues/16990
                true,
                parquetDateTimeZone,
                fileFormatDataSourceStats,
                parquetReaderOptions,
                Optional.empty(),
                domainCompactionThreshold);

        verify(pageSource.getReaderColumns().isEmpty(), "Unexpected reader columns: %s", pageSource.getReaderColumns().orElse(null));

        return new DeltaLakePageSource(
                splitColumns,
                ImmutableSet.of(),
                partitionKeys,
                Optional.empty(),
                pageSource.get(),
                Optional.empty(),
                split.path(),
                split.fileSize(),
                0L);
    }
}
