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
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.filesystem.hdfs.HdfsFileSystemFactory;
import io.trino.operator.GroupByHashPageIndexerFactory;
import io.trino.plugin.deltalake.transactionlog.ProtocolEntry;
import io.trino.plugin.hive.HiveTransactionHandle;
import io.trino.plugin.hive.NodeVersion;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.type.TestingTypeManager;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.gen.JoinCompiler;
import io.trino.tpch.LineItem;
import io.trino.tpch.LineItemColumn;
import io.trino.tpch.LineItemGenerator;
import io.trino.tpch.TpchColumnType;
import io.trino.type.BlockTypeOperators;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.Test;

import java.io.File;
import java.nio.file.Files;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.trino.plugin.deltalake.DeltaLakeColumnType.REGULAR;
import static io.trino.plugin.deltalake.DeltaLakeMetadata.DEFAULT_READER_VERSION;
import static io.trino.plugin.deltalake.DeltaLakeMetadata.DEFAULT_WRITER_VERSION;
import static io.trino.plugin.deltalake.DeltaTestingConnectorSession.SESSION;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.testing.TestingPageSinkId.TESTING_PAGE_SINK_ID;
import static java.lang.Math.round;
import static java.time.temporal.ChronoUnit.MINUTES;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestDeltaLakePageSink
{
    private static final int NUM_ROWS = 1000;
    private static final String SCHEMA_NAME = "test";
    private static final String TABLE_NAME = "test";

    @Test
    public void testPageSinkStats()
            throws Exception
    {
        File tempDir = Files.createTempDirectory(null).toFile();
        try {
            DeltaLakeWriterStats stats = new DeltaLakeWriterStats();
            String tablePath = tempDir.getAbsolutePath() + "/test_table";
            ConnectorPageSink pageSink = createPageSink(new Path(tablePath), stats);

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
                    .map(Slice::getBytes)
                    .map(dataFileInfoCodec::fromJson)
                    .collect(toImmutableList());

            assertEquals(dataFileInfos.size(), 1);
            DataFileInfo dataFileInfo = dataFileInfos.get(0);

            List<File> files = ImmutableList.copyOf(new File(tablePath).listFiles((dir, name) -> !name.endsWith(".crc")));
            assertEquals(files.size(), 1);
            File outputFile = files.get(0);

            assertEquals(round(stats.getInputPageSizeInBytes().getAllTime().getMax()), page.getRetainedSizeInBytes());

            assertEquals(dataFileInfo.getStatistics().getNumRecords(), Optional.of(rows));
            assertEquals(dataFileInfo.getPartitionValues(), ImmutableList.of());
            assertEquals(dataFileInfo.getSize(), outputFile.length());
            assertEquals(dataFileInfo.getPath(), outputFile.getName());

            Instant now = Instant.now();
            assertTrue(dataFileInfo.getCreationTime() < now.toEpochMilli());
            assertTrue(dataFileInfo.getCreationTime() > now.minus(1, MINUTES).toEpochMilli());
        }
        finally {
            deleteRecursively(tempDir.toPath(), ALLOW_INSECURE);
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

    private static ConnectorPageSink createPageSink(Path outputPath, DeltaLakeWriterStats stats)
    {
        HiveTransactionHandle transaction = new HiveTransactionHandle(false);
        DeltaLakeConfig deltaLakeConfig = new DeltaLakeConfig();
        DeltaLakeOutputTableHandle tableHandle = new DeltaLakeOutputTableHandle(
                SCHEMA_NAME,
                TABLE_NAME,
                getColumnHandles(),
                outputPath.toString(),
                Optional.of(deltaLakeConfig.getDefaultCheckpointWritingInterval()),
                true,
                Optional.empty(),
                Optional.of(false),
                new ProtocolEntry(DEFAULT_READER_VERSION, DEFAULT_WRITER_VERSION));

        DeltaLakePageSinkProvider provider = new DeltaLakePageSinkProvider(
                new GroupByHashPageIndexerFactory(new JoinCompiler(new TypeOperators()), new BlockTypeOperators()),
                new HdfsFileSystemFactory(HDFS_ENVIRONMENT),
                JsonCodec.jsonCodec(DataFileInfo.class),
                JsonCodec.jsonCodec(DeltaLakeMergeResult.class),
                stats,
                deltaLakeConfig,
                new TestingTypeManager(),
                new NodeVersion("test-version"));

        return provider.createPageSink(transaction, SESSION, tableHandle, TESTING_PAGE_SINK_ID);
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
                    REGULAR));
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
