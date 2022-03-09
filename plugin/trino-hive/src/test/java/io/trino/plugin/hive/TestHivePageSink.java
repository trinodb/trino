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
package io.trino.plugin.hive;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slices;
import io.trino.operator.GroupByHashPageIndexerFactory;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.HiveMetastoreFactory;
import io.trino.plugin.hive.metastore.HivePageSinkMetadata;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.SplitWeight;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.gen.JoinCompiler;
import io.trino.testing.MaterializedResult;
import io.trino.testing.TestingNodeManager;
import io.trino.tpch.LineItem;
import io.trino.tpch.LineItemColumn;
import io.trino.tpch.LineItemGenerator;
import io.trino.tpch.TpchColumnType;
import io.trino.tpch.TpchColumnTypes;
import io.trino.type.BlockTypeOperators;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.testing.Assertions.assertGreaterThan;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.trino.plugin.hive.HiveColumnHandle.createBaseColumn;
import static io.trino.plugin.hive.HiveCompressionCodec.NONE;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.trino.plugin.hive.HiveTestUtils.PAGE_SORTER;
import static io.trino.plugin.hive.HiveTestUtils.getDefaultHiveFileWriterFactories;
import static io.trino.plugin.hive.HiveTestUtils.getDefaultHivePageSourceFactories;
import static io.trino.plugin.hive.HiveTestUtils.getDefaultHiveRecordCursorProviders;
import static io.trino.plugin.hive.HiveTestUtils.getHiveSession;
import static io.trino.plugin.hive.HiveTestUtils.getHiveSessionProperties;
import static io.trino.plugin.hive.HiveType.HIVE_DATE;
import static io.trino.plugin.hive.HiveType.HIVE_DOUBLE;
import static io.trino.plugin.hive.HiveType.HIVE_INT;
import static io.trino.plugin.hive.HiveType.HIVE_LONG;
import static io.trino.plugin.hive.HiveType.HIVE_STRING;
import static io.trino.plugin.hive.LocationHandle.WriteMode.DIRECT_TO_TARGET_NEW_DIRECTORY;
import static io.trino.plugin.hive.acid.AcidTransaction.NO_ACID_TRANSACTION;
import static io.trino.plugin.hive.metastore.file.FileHiveMetastore.createTestingFileHiveMetastore;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.testing.assertions.Assert.assertEquals;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.lang.Math.round;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.FILE_INPUT_FORMAT;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_LIB;
import static org.testng.Assert.assertTrue;

public class TestHivePageSink
{
    private static final int NUM_ROWS = 1000;
    private static final String SCHEMA_NAME = "test";
    private static final String TABLE_NAME = "test";

    @Test
    public void testAllFormats()
            throws Exception
    {
        HiveConfig config = new HiveConfig();
        File tempDir = Files.createTempDir();
        try {
            HiveMetastore metastore = createTestingFileHiveMetastore(new File(tempDir, "metastore"));
            for (HiveStorageFormat format : HiveStorageFormat.values()) {
                if (format == HiveStorageFormat.CSV) {
                    // CSV supports only unbounded VARCHAR type, which is not provided by lineitem
                    continue;
                }
                config.setHiveStorageFormat(format);
                config.setHiveCompressionCodec(NONE);
                long uncompressedLength = writeTestFile(config, metastore, makeFileName(tempDir, config));
                assertGreaterThan(uncompressedLength, 0L);

                for (HiveCompressionCodec codec : HiveCompressionCodec.values()) {
                    if (codec == NONE) {
                        continue;
                    }
                    config.setHiveCompressionCodec(codec);
                    long length = writeTestFile(config, metastore, makeFileName(tempDir, config));
                    assertTrue(uncompressedLength > length, format("%s with %s compressed to %s which is not less than %s", format, codec, length, uncompressedLength));
                }
            }
        }
        finally {
            deleteRecursively(tempDir.toPath(), ALLOW_INSECURE);
        }
    }

    private static String makeFileName(File tempDir, HiveConfig config)
    {
        return tempDir.getAbsolutePath() + "/" + config.getHiveStorageFormat().name() + "." + config.getHiveCompressionCodec().name();
    }

    private static long writeTestFile(HiveConfig config, HiveMetastore metastore, String outputPath)
    {
        HiveTransactionHandle transaction = new HiveTransactionHandle(false);
        HiveWriterStats stats = new HiveWriterStats();
        ConnectorPageSink pageSink = createPageSink(transaction, config, metastore, new Path("file:///" + outputPath), stats);
        List<LineItemColumn> columns = getTestColumns();
        List<Type> columnTypes = columns.stream()
                .map(LineItemColumn::getType)
                .map(TestHivePageSink::getHiveType)
                .map(hiveType -> hiveType.getType(TESTING_TYPE_MANAGER))
                .collect(toList());

        PageBuilder pageBuilder = new PageBuilder(columnTypes);
        int rows = 0;
        for (LineItem lineItem : new LineItemGenerator(0.01, 1, 1)) {
            rows++;
            if (rows >= NUM_ROWS) {
                break;
            }
            pageBuilder.declarePosition();
            for (int i = 0; i < columns.size(); i++) {
                LineItemColumn column = columns.get(i);
                BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(i);
                switch (column.getType().getBase()) {
                    case IDENTIFIER:
                        BIGINT.writeLong(blockBuilder, column.getIdentifier(lineItem));
                        break;
                    case INTEGER:
                        INTEGER.writeLong(blockBuilder, column.getInteger(lineItem));
                        break;
                    case DATE:
                        DATE.writeLong(blockBuilder, column.getDate(lineItem));
                        break;
                    case DOUBLE:
                        DOUBLE.writeDouble(blockBuilder, column.getDouble(lineItem));
                        break;
                    case VARCHAR:
                        createUnboundedVarcharType().writeSlice(blockBuilder, Slices.utf8Slice(column.getString(lineItem)));
                        break;
                    default:
                        throw new IllegalArgumentException("Unsupported type " + column.getType());
                }
            }
        }
        Page page = pageBuilder.build();
        pageSink.appendPage(page);
        getFutureValue(pageSink.finish());

        File outputDir = new File(outputPath);
        List<File> files = ImmutableList.copyOf(outputDir.listFiles((dir, name) -> !name.endsWith(".crc")));
        File outputFile = getOnlyElement(files);
        long length = outputFile.length();

        ConnectorPageSource pageSource = createPageSource(transaction, config, outputFile);

        List<Page> pages = new ArrayList<>();
        while (!pageSource.isFinished()) {
            Page nextPage = pageSource.getNextPage();
            if (nextPage != null) {
                pages.add(nextPage.getLoadedPage());
            }
        }
        MaterializedResult expectedResults = toMaterializedResult(getHiveSession(config), columnTypes, ImmutableList.of(page));
        MaterializedResult results = toMaterializedResult(getHiveSession(config), columnTypes, pages);
        assertEquals(results, expectedResults);
        assertEquals(round(stats.getInputPageSizeInBytes().getAllTime().getMax()), page.getRetainedSizeInBytes());
        return length;
    }

    public static MaterializedResult toMaterializedResult(ConnectorSession session, List<Type> types, List<Page> pages)
    {
        // materialize pages
        MaterializedResult.Builder resultBuilder = MaterializedResult.resultBuilder(session, types);
        for (Page outputPage : pages) {
            resultBuilder.page(outputPage);
        }
        return resultBuilder.build();
    }

    private static ConnectorPageSource createPageSource(HiveTransactionHandle transaction, HiveConfig config, File outputFile)
    {
        Properties splitProperties = new Properties();
        splitProperties.setProperty(FILE_INPUT_FORMAT, config.getHiveStorageFormat().getInputFormat());
        splitProperties.setProperty(SERIALIZATION_LIB, config.getHiveStorageFormat().getSerde());
        splitProperties.setProperty("columns", Joiner.on(',').join(getColumnHandles().stream().map(HiveColumnHandle::getName).collect(toImmutableList())));
        splitProperties.setProperty("columns.types", Joiner.on(',').join(getColumnHandles().stream().map(HiveColumnHandle::getHiveType).map(hiveType -> hiveType.getHiveTypeName().toString()).collect(toImmutableList())));
        HiveSplit split = new HiveSplit(
                SCHEMA_NAME,
                TABLE_NAME,
                "",
                "file:///" + outputFile.getAbsolutePath(),
                0,
                outputFile.length(),
                outputFile.length(),
                outputFile.lastModified(),
                splitProperties,
                ImmutableList.of(),
                ImmutableList.of(),
                OptionalInt.empty(),
                0,
                false,
                TableToPartitionMapping.empty(),
                Optional.empty(),
                Optional.empty(),
                false,
                Optional.empty(),
                0,
                SplitWeight.standard());
        ConnectorTableHandle table = new HiveTableHandle(SCHEMA_NAME, TABLE_NAME, ImmutableMap.of(), ImmutableList.of(), ImmutableList.of(), Optional.empty());
        HivePageSourceProvider provider = new HivePageSourceProvider(
                TESTING_TYPE_MANAGER,
                HDFS_ENVIRONMENT,
                config,
                getDefaultHivePageSourceFactories(HDFS_ENVIRONMENT, config),
                getDefaultHiveRecordCursorProviders(config, HDFS_ENVIRONMENT),
                new GenericHiveRecordCursorProvider(HDFS_ENVIRONMENT, config),
                Optional.empty());
        return provider.createPageSource(transaction, getHiveSession(config), split, table, ImmutableList.copyOf(getColumnHandles()), DynamicFilter.EMPTY);
    }

    private static ConnectorPageSink createPageSink(HiveTransactionHandle transaction, HiveConfig config, HiveMetastore metastore, Path outputPath, HiveWriterStats stats)
    {
        LocationHandle locationHandle = new LocationHandle(outputPath, outputPath, false, DIRECT_TO_TARGET_NEW_DIRECTORY);
        HiveOutputTableHandle handle = new HiveOutputTableHandle(
                SCHEMA_NAME,
                TABLE_NAME,
                getColumnHandles(),
                new HivePageSinkMetadata(new SchemaTableName(SCHEMA_NAME, TABLE_NAME), metastore.getTable(SCHEMA_NAME, TABLE_NAME), ImmutableMap.of()),
                locationHandle,
                config.getHiveStorageFormat(),
                config.getHiveStorageFormat(),
                ImmutableList.of(),
                Optional.empty(),
                "test",
                ImmutableMap.of(),
                NO_ACID_TRANSACTION,
                false,
                false,
                Optional.empty());
        JsonCodec<PartitionUpdate> partitionUpdateCodec = JsonCodec.jsonCodec(PartitionUpdate.class);
        TypeOperators typeOperators = new TypeOperators();
        BlockTypeOperators blockTypeOperators = new BlockTypeOperators(typeOperators);
        HivePageSinkProvider provider = new HivePageSinkProvider(
                getDefaultHiveFileWriterFactories(config, HDFS_ENVIRONMENT),
                HDFS_ENVIRONMENT,
                PAGE_SORTER,
                HiveMetastoreFactory.ofInstance(metastore),
                new GroupByHashPageIndexerFactory(new JoinCompiler(typeOperators), blockTypeOperators),
                TESTING_TYPE_MANAGER,
                config,
                new HiveLocationService(HDFS_ENVIRONMENT),
                partitionUpdateCodec,
                new TestingNodeManager("fake-environment"),
                new HiveEventClient(),
                getHiveSessionProperties(config),
                stats);
        return provider.createPageSink(transaction, getHiveSession(config), handle);
    }

    private static List<HiveColumnHandle> getColumnHandles()
    {
        ImmutableList.Builder<HiveColumnHandle> handles = ImmutableList.builder();
        List<LineItemColumn> columns = getTestColumns();
        for (int i = 0; i < columns.size(); i++) {
            LineItemColumn column = columns.get(i);
            HiveType hiveType = getHiveType(column.getType());
            handles.add(createBaseColumn(column.getColumnName(), i, hiveType, hiveType.getType(TESTING_TYPE_MANAGER), REGULAR, Optional.empty()));
        }
        return handles.build();
    }

    private static List<LineItemColumn> getTestColumns()
    {
        return Stream.of(LineItemColumn.values())
                // Not all the formats support DATE
                .filter(column -> !column.getType().equals(TpchColumnTypes.DATE))
                .collect(toList());
    }

    private static HiveType getHiveType(TpchColumnType type)
    {
        switch (type.getBase()) {
            case IDENTIFIER:
                return HIVE_LONG;
            case INTEGER:
                return HIVE_INT;
            case DATE:
                return HIVE_DATE;
            case DOUBLE:
                return HIVE_DOUBLE;
            case VARCHAR:
                return HIVE_STRING;
        }
        throw new UnsupportedOperationException();
    }
}
