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
package io.trino.plugin.deltalake;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.operator.FlatHashStrategyCompiler;
import io.trino.operator.GroupByHashPageIndexerFactory;
import io.trino.operator.NullSafeHashCompiler;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.deltalake.transactionlog.ProtocolEntry;
import io.trino.plugin.hive.HiveTransactionHandle;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.hive.parquet.ParquetWriterConfig;
import io.trino.spi.NodeVersion;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.tpch.LineItem;
import io.trino.tpch.LineItemColumn;
import io.trino.tpch.LineItemGenerator;
import io.trino.tpch.TpchColumnType;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.trino.hdfs.HdfsTestUtils.HDFS_FILE_SYSTEM_FACTORY;
import static io.trino.plugin.deltalake.DeltaLakeColumnType.PARTITION_KEY;
import static io.trino.plugin.deltalake.DeltaLakeColumnType.REGULAR;
import static io.trino.plugin.deltalake.DeltaLakeMetadata.DEFAULT_READER_VERSION;
import static io.trino.plugin.deltalake.DeltaLakeMetadata.DEFAULT_WRITER_VERSION;
import static io.trino.plugin.deltalake.DeltaTestingConnectorSession.SESSION;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.ColumnMappingMode.NONE;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.serializeColumnType;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.serializeSchemaAsJson;
import static io.trino.plugin.deltalake.util.DeltaLakeWriteUtils.createDataFilePath;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.testing.TestingPageSinkId.TESTING_PAGE_SINK_ID;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.lang.Math.round;
import static java.time.temporal.ChronoUnit.MINUTES;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDeltaLakePageSink
{
    private static final int NUM_ROWS = 1000;
    private static final String SCHEMA_NAME = "test";
    private static final String TABLE_NAME = "test";

    @Test
    public void testPageSinkStats()
            throws Exception
    {
        Path tempDir = Files.createTempDirectory(null);
        try {
            DeltaLakeWriterStats stats = new DeltaLakeWriterStats();
            Path tablePath = tempDir.resolve("test_table");
            ConnectorPageSink pageSink = createPageSink(tablePath.toString(), stats, getColumnHandles());

            List<LineItemColumn> columns = ImmutableList.copyOf(LineItemColumn.values());
            List<Type> columnTypes = columns.stream()
                    .map(LineItemColumn::getType)
                    .map(TestDeltaLakePageSink::getTrinoType)
                    .collect(toList());

            PageBuilder pageBuilder = new PageBuilder(columnTypes);
            long rows = 0;
            for (LineItem lineItem : new LineItemGenerator(0.01, 1, 1)) {
                if (rows >= NUM_ROWS) {
                    break;
                }
                rows++;
                pageBuilder.declarePosition();
                for (int i = 0; i < columns.size(); i++) {
                    LineItemColumn column = columns.get(i);
                    BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(i);
                    writeToBlock(blockBuilder, column, lineItem);
                }
            }
            Page page = pageBuilder.build();
            pageSink.appendPage(page).get(10, TimeUnit.SECONDS);

            JsonCodec<DataFileInfo> dataFileInfoCodec = new JsonCodecFactory().jsonCodec(DataFileInfo.class);
            Collection<Slice> fragments = getFutureValue(pageSink.finish());
            List<DataFileInfo> dataFileInfos = fragments.stream()
                    .map(Slice::getInput)
                    .map(dataFileInfoCodec::fromJson)
                    .collect(toImmutableList());

            assertThat(dataFileInfos).hasSize(1);
            DataFileInfo dataFileInfo = dataFileInfos.get(0);

            List<Path> files = listDataFiles(tablePath);
            assertThat(files).hasSize(1);
            Path outputFile = files.get(0);
            String relativeOutputPath = tablePath.relativize(outputFile).toString();

            assertThat(round(stats.getInputPageSizeInBytes().getAllTime().getMax())).isEqualTo(page.getRetainedSizeInBytes());

            assertThat(dataFileInfo.statistics().getNumRecords()).isEqualTo(Optional.of(rows));
            assertThat(dataFileInfo.partitionValues()).isEqualTo(ImmutableList.of());
            assertThat(dataFileInfo.size()).isEqualTo(Files.size(outputFile));
            assertThat(dataFileInfo.path())
                    .isEqualTo(relativeOutputPath)
                    .isEqualTo(createDataFilePath(outputFile.getFileName().toString()));

            Instant now = Instant.now();
            assertThat(dataFileInfo.creationTime() < now.toEpochMilli()).isTrue();
            assertThat(dataFileInfo.creationTime() > now.minus(1, MINUTES).toEpochMilli()).isTrue();
        }
        finally {
            deleteRecursively(tempDir, ALLOW_INSECURE);
        }
    }

    @Test
    public void testPartitionedPageSinkUsesBinaryPath()
            throws Exception
    {
        Path tempDir = Files.createTempDirectory(null);
        try {
            Path tablePath = tempDir.resolve("test_partitioned_table");
            List<DeltaLakeColumnHandle> columns = ImmutableList.of(
                    new DeltaLakeColumnHandle("value", BIGINT, OptionalInt.empty(), "value", BIGINT, REGULAR, Optional.empty()),
                    new DeltaLakeColumnHandle("part", INTEGER, OptionalInt.empty(), "part", INTEGER, PARTITION_KEY, Optional.empty()));
            ConnectorPageSink pageSink = createPageSink(tablePath.toString(), new DeltaLakeWriterStats(), columns);

            PageBuilder pageBuilder = new PageBuilder(ImmutableList.of(BIGINT, INTEGER));
            pageBuilder.declarePosition();
            BIGINT.writeLong(pageBuilder.getBlockBuilder(0), 42);
            INTEGER.writeLong(pageBuilder.getBlockBuilder(1), 7);
            pageSink.appendPage(pageBuilder.build()).get(10, TimeUnit.SECONDS);

            DataFileInfo dataFileInfo = getOnlyDataFileInfo(pageSink);
            Path outputFile = getOnlyElement(listDataFiles(tablePath));
            String relativeOutputPath = tablePath.relativize(outputFile).toString();

            assertThat(dataFileInfo.partitionValues()).containsExactly("7");
            assertThat(dataFileInfo.path())
                    .isEqualTo(relativeOutputPath)
                    .isEqualTo(createDataFilePath(outputFile.getFileName().toString()))
                    .doesNotContain("part=");
        }
        finally {
            deleteRecursively(tempDir, ALLOW_INSECURE);
        }
    }

    @Test
    public void testBinaryPathCanBeDisabled()
            throws Exception
    {
        Path tempDir = Files.createTempDirectory(null);
        try {
            Path tablePath = tempDir.resolve("test_partitioned_table");
            List<DeltaLakeColumnHandle> columns = ImmutableList.of(
                    new DeltaLakeColumnHandle("value", BIGINT, OptionalInt.empty(), "value", BIGINT, REGULAR, Optional.empty()),
                    new DeltaLakeColumnHandle("part", INTEGER, OptionalInt.empty(), "part", INTEGER, PARTITION_KEY, Optional.empty()));
            ConnectorPageSink pageSink = createPageSink(tablePath.toString(), new DeltaLakeWriterStats(), columns, false);

            PageBuilder pageBuilder = new PageBuilder(ImmutableList.of(BIGINT, INTEGER));
            pageBuilder.declarePosition();
            BIGINT.writeLong(pageBuilder.getBlockBuilder(0), 42);
            INTEGER.writeLong(pageBuilder.getBlockBuilder(1), 7);
            pageSink.appendPage(pageBuilder.build()).get(10, TimeUnit.SECONDS);

            DataFileInfo dataFileInfo = getOnlyDataFileInfo(pageSink);
            Path outputFile = getOnlyElement(listDataFiles(tablePath));
            String relativeOutputPath = tablePath.relativize(outputFile).toString();

            assertThat(dataFileInfo.partitionValues()).containsExactly("7");
            assertThat(dataFileInfo.path())
                    .isEqualTo(relativeOutputPath)
                    .isEqualTo("part=7/" + outputFile.getFileName());
        }
        finally {
            deleteRecursively(tempDir, ALLOW_INSECURE);
        }
    }

    private void writeToBlock(BlockBuilder blockBuilder, LineItemColumn column, LineItem lineItem)
    {
        switch (column.getType().getBase()) {
            case IDENTIFIER -> BIGINT.writeLong(blockBuilder, column.getIdentifier(lineItem));
            case INTEGER -> INTEGER.writeLong(blockBuilder, column.getInteger(lineItem));
            case DATE -> DATE.writeLong(blockBuilder, column.getDate(lineItem));
            case DOUBLE -> DOUBLE.writeDouble(blockBuilder, column.getDouble(lineItem));
            case VARCHAR -> createUnboundedVarcharType().writeSlice(blockBuilder, Slices.utf8Slice(column.getString(lineItem)));
            default -> throw new IllegalArgumentException("Unsupported type " + column.getType());
        }
    }

    private static ConnectorPageSink createPageSink(String outputPath, DeltaLakeWriterStats stats, List<DeltaLakeColumnHandle> columns)
    {
        return createPageSink(outputPath, stats, columns, true);
    }

    private static ConnectorPageSink createPageSink(String outputPath, DeltaLakeWriterStats stats, List<DeltaLakeColumnHandle> columns, boolean objectStoreLayoutEnabled)
    {
        HiveTransactionHandle transaction = new HiveTransactionHandle(false);
        DeltaLakeConfig deltaLakeConfig = new DeltaLakeConfig()
                .setObjectStoreLayoutEnabled(objectStoreLayoutEnabled);
        DeltaLakeTable.Builder deltaTable = DeltaLakeTable.builder();
        for (DeltaLakeColumnHandle column : columns) {
            deltaTable.addColumn(column.columnName(), serializeColumnType(NONE, new AtomicInteger(), column.type()), true, Optional.empty(), ImmutableMap.of());
        }
        String schemaString = serializeSchemaAsJson(deltaTable.build());
        DeltaLakeOutputTableHandle tableHandle = new DeltaLakeOutputTableHandle(
                SCHEMA_NAME,
                TABLE_NAME,
                columns,
                outputPath,
                Optional.of(deltaLakeConfig.getDefaultCheckpointWritingInterval()),
                true,
                Optional.empty(),
                Optional.of(false),
                false,
                objectStoreLayoutEnabled,
                schemaString,
                NONE,
                OptionalInt.empty(),
                false,
                Optional.of(getColumnHandles()),
                OptionalLong.empty(),
                new ProtocolEntry(DEFAULT_READER_VERSION, DEFAULT_WRITER_VERSION, Optional.empty(), Optional.empty()));

        DeltaLakePageSinkProvider provider = new DeltaLakePageSinkProvider(
                new GroupByHashPageIndexerFactory(new FlatHashStrategyCompiler(new TypeOperators(), new NullSafeHashCompiler(new TypeOperators()))),
                new DefaultDeltaLakeFileSystemFactory(HDFS_FILE_SYSTEM_FACTORY, new NoOpTableCredentialsProvider()),
                jsonCodec(DataFileInfo.class),
                jsonCodec(DeltaLakeMergeResult.class),
                stats,
                new FileFormatDataSourceStats(),
                deltaLakeConfig,
                new ParquetReaderConfig(),
                new ParquetWriterConfig(),
                TESTING_TYPE_MANAGER,
                new NodeVersion("test-version"));

        return provider.createPageSink(transaction, SESSION, tableHandle, Optional.empty(), TESTING_PAGE_SINK_ID);
    }

    private static DataFileInfo getOnlyDataFileInfo(ConnectorPageSink pageSink)
    {
        JsonCodec<DataFileInfo> dataFileInfoCodec = new JsonCodecFactory().jsonCodec(DataFileInfo.class);
        return getOnlyElement(getFutureValue(pageSink.finish()).stream()
                .map(Slice::getInput)
                .map(dataFileInfoCodec::fromJson)
                .collect(toImmutableList()));
    }

    private static List<Path> listDataFiles(Path tablePath)
            throws IOException
    {
        try (Stream<Path> files = Files.walk(tablePath)) {
            return files
                    .filter(Files::isRegularFile)
                    .filter(path -> !path.getFileName().toString().endsWith(".crc"))
                    .collect(toImmutableList());
        }
    }

    private static List<DeltaLakeColumnHandle> getColumnHandles()
    {
        ImmutableList.Builder<DeltaLakeColumnHandle> handles = ImmutableList.builder();
        LineItemColumn[] columns = LineItemColumn.values();
        for (LineItemColumn column : columns) {
            handles.add(new DeltaLakeColumnHandle(
                    column.getColumnName(),
                    getTrinoType(column.getType()),
                    OptionalInt.empty(),
                    column.getColumnName(),
                    getTrinoType(column.getType()),
                    REGULAR,
                    Optional.empty()));
        }
        return handles.build();
    }

    private static Type getTrinoType(TpchColumnType type)
    {
        return switch (type.getBase()) {
            case IDENTIFIER -> BIGINT;
            case INTEGER -> INTEGER;
            case DATE -> DATE;
            case DOUBLE -> DOUBLE;
            case VARCHAR -> createUnboundedVarcharType();
        };
    }
}
