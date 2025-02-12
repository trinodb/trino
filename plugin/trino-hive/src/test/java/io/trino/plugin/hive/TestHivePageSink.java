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
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.trino.filesystem.FileEntry;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.memory.MemoryFileSystemFactory;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.HiveMetastoreFactory;
import io.trino.operator.FlatHashStrategyCompiler;
import io.trino.operator.GroupByHashPageIndexerFactory;
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
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.testing.MaterializedResult;
import io.trino.tpch.LineItem;
import io.trino.tpch.LineItemColumn;
import io.trino.tpch.LineItemGenerator;
import io.trino.tpch.TpchColumnType;
import io.trino.tpch.TpchColumnTypes;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.hive.thrift.metastore.hive_metastoreConstants.FILE_INPUT_FORMAT;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.PARTITION_KEY;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.trino.plugin.hive.HiveColumnHandle.createBaseColumn;
import static io.trino.plugin.hive.HiveCompressionOption.LZ4;
import static io.trino.plugin.hive.HiveCompressionOption.NONE;
import static io.trino.plugin.hive.HiveStorageFormat.AVRO;
import static io.trino.plugin.hive.HiveStorageFormat.PARQUET;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_FILE_SYSTEM_FACTORY;
import static io.trino.plugin.hive.HiveTestUtils.PAGE_SORTER;
import static io.trino.plugin.hive.HiveTestUtils.getDefaultHiveFileWriterFactories;
import static io.trino.plugin.hive.HiveTestUtils.getDefaultHivePageSourceFactories;
import static io.trino.plugin.hive.HiveTestUtils.getHiveSession;
import static io.trino.plugin.hive.LocationHandle.WriteMode.DIRECT_TO_TARGET_NEW_DIRECTORY;
import static io.trino.plugin.hive.acid.AcidTransaction.NO_ACID_TRANSACTION;
import static io.trino.plugin.hive.metastore.file.TestingFileHiveMetastore.createTestingFileHiveMetastore;
import static io.trino.plugin.hive.util.HiveTypeTranslator.toHiveType;
import static io.trino.plugin.hive.util.SerdeConstants.LIST_COLUMNS;
import static io.trino.plugin.hive.util.SerdeConstants.LIST_COLUMN_TYPES;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.testing.TestingPageSinkId.TESTING_PAGE_SINK_ID;
import static io.trino.tpch.LineItemColumn.SHIP_MODE;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.lang.Math.round;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestHivePageSink
{
    private static final int NUM_ROWS = 1000;
    private static final String SCHEMA_NAME = "test";
    private static final String TABLE_NAME = "test";

    @Test
    void testAllFormats()
            throws Exception
    {
        HiveConfig config = new HiveConfig();
        SortingFileWriterConfig sortingFileWriterConfig = new SortingFileWriterConfig();

        TrinoFileSystemFactory fileSystemFactory = new MemoryFileSystemFactory();
        HiveMetastore metastore = createTestingFileHiveMetastore(fileSystemFactory, Location.of("memory:///metastore"));
        for (HiveStorageFormat format : HiveStorageFormat.values()) {
            if (format == HiveStorageFormat.CSV) {
                // CSV supports only the unbounded VARCHAR type, which is not provided by lineitem
                continue;
            }
            if (format == HiveStorageFormat.REGEX) {
                // REGEX format is readonly
                continue;
            }
            config.setHiveStorageFormat(format);
            config.setHiveCompressionCodec(NONE);
            long uncompressedLength = writeTestFile(fileSystemFactory, config, sortingFileWriterConfig, metastore, makeFileName(config));
            assertThat(uncompressedLength).isGreaterThan(0L);

            for (HiveCompressionOption codec : HiveCompressionOption.values()) {
                if (codec == NONE) {
                    continue;
                }
                config.setHiveCompressionCodec(codec);

                // TODO (https://github.com/trinodb/trino/issues/9142) LZ4 is not supported with native Parquet writer
                if (!isSupportedCodec(format, codec)) {
                    assertThatThrownBy(() -> writeTestFile(fileSystemFactory, config, sortingFileWriterConfig, metastore, makeFileName(config)))
                            .hasMessage("Compression codec " + codec + " not supported for " + format.humanName());
                    continue;
                }

                long length = writeTestFile(fileSystemFactory, config, sortingFileWriterConfig, metastore, makeFileName(config));
                assertThat(uncompressedLength > length)
                        .describedAs(format("%s with %s compressed to %s which is not less than %s", format, codec, length, uncompressedLength))
                        .isTrue();
            }
        }
    }

    @Test
    public void testCloseIdleWritersWhenIdleWriterMinFileSizeLimitIsReached()
            throws IOException
    {
        testCloseIdleWriters(DataSize.of(1, BYTE), 2, 1);
    }

    @Test
    public void testCloseIdleWritersWhenIdleWriterMinFileSizeLimitIsNotReached()
            throws IOException
    {
        testCloseIdleWriters(DataSize.of(100, MEGABYTE), 1, 1);
    }

    private void testCloseIdleWriters(DataSize idleWritersMinFileSize, int expectedTruckFiles, int expectedShipFiles)
            throws IOException
    {
        HiveConfig config = new HiveConfig()
                .setIdleWriterMinFileSize(idleWritersMinFileSize)
                .setHiveStorageFormat(PARQUET)
                .setHiveCompressionCodec(NONE);
        SortingFileWriterConfig sortingFileWriterConfig = new SortingFileWriterConfig();

        TrinoFileSystemFactory fileSystemFactory = new MemoryFileSystemFactory();
        HiveMetastore metastore = createTestingFileHiveMetastore(fileSystemFactory, Location.of("memory:///metastore"));

        HiveTransactionHandle transaction = new HiveTransactionHandle(false);
        HiveWriterStats stats = new HiveWriterStats();
        List<HiveColumnHandle> columnHandles = getPartitionedColumnHandles(SHIP_MODE.getColumnName());
        Location location = makeFileName(config);
        ConnectorPageSink pageSink = createPageSink(fileSystemFactory, transaction, config, sortingFileWriterConfig, metastore, location, stats, columnHandles);
        Page truckPage = createPage(lineItem -> lineItem.shipMode().equals("TRUCK"));
        Page shipPage = createPage(lineItem -> lineItem.shipMode().equals("SHIP"));

        pageSink.appendPage(truckPage);
        pageSink.appendPage(shipPage);
        // This call will mark the truck and ship partition as idle.
        pageSink.closeIdleWriters();

        // This call will mark the ship partition as non-idle.
        pageSink.appendPage(shipPage);
        // This call will close the truck partition if idleWritersMinFileSize limit is reached since
        // it is still idle.
        pageSink.closeIdleWriters();

        pageSink.appendPage(truckPage);
        pageSink.appendPage(shipPage);

        getFutureValue(pageSink.finish());
        FileIterator fileIterator = fileSystemFactory.create(ConnectorIdentity.ofUser("test")).listFiles(location);

        int truckFileCount = 0;
        int shipFileCount = 0;
        while (fileIterator.hasNext()) {
            FileEntry file = fileIterator.next();
            if (file.location().toString().contains("TRUCK")) {
                truckFileCount++;
            }
            else if (file.location().toString().contains("SHIP")) {
                shipFileCount++;
            }
        }
        assertThat(truckFileCount).isEqualTo(expectedTruckFiles);
        assertThat(shipFileCount).isEqualTo(expectedShipFiles);
    }

    private static boolean isSupportedCodec(HiveStorageFormat storageFormat, HiveCompressionOption compressionOption)
    {
        if ((storageFormat == AVRO || storageFormat == PARQUET) && compressionOption == LZ4) {
            return false;
        }
        return true;
    }

    private static Location makeFileName(HiveConfig config)
    {
        return Location.of("memory:///" + config.getHiveStorageFormat().name() + "." + config.getHiveCompressionCodec().name());
    }

    private static long writeTestFile(TrinoFileSystemFactory fileSystemFactory, HiveConfig config, SortingFileWriterConfig sortingFileWriterConfig, HiveMetastore metastore, Location location)
            throws IOException
    {
        HiveTransactionHandle transaction = new HiveTransactionHandle(false);
        HiveWriterStats stats = new HiveWriterStats();
        ConnectorPageSink pageSink = createPageSink(fileSystemFactory, transaction, config, sortingFileWriterConfig, metastore, location, stats, getColumnHandles());
        List<LineItemColumn> columns = getTestColumns();
        List<Type> columnTypes = columns.stream()
                .map(LineItemColumn::getType)
                .map(TestHivePageSink::getType)
                .map(hiveType -> TESTING_TYPE_MANAGER.getType(hiveType.getTypeSignature()))
                .collect(toList());
        Page page = createPage(lineItem -> true);
        pageSink.appendPage(page);
        getFutureValue(pageSink.finish());

        FileIterator fileIterator = fileSystemFactory.create(ConnectorIdentity.ofUser("test")).listFiles(location);
        FileEntry fileEntry = fileIterator.next();
        assertThat(fileIterator.hasNext()).isFalse();

        List<Page> pages = new ArrayList<>();
        try (ConnectorPageSource pageSource = createPageSource(fileSystemFactory, transaction, config, fileEntry.location())) {
            while (!pageSource.isFinished()) {
                Page nextPage = pageSource.getNextPage();
                if (nextPage != null) {
                    pages.add(nextPage.getLoadedPage());
                }
            }
        }

        MaterializedResult expectedResults = toMaterializedResult(getHiveSession(config), columnTypes, ImmutableList.of(page));
        MaterializedResult results = toMaterializedResult(getHiveSession(config), columnTypes, pages);
        assertThat(results).containsExactlyElementsOf(expectedResults);
        assertThat(round(stats.getInputPageSizeInBytes().getAllTime().getMax())).isEqualTo(page.getRetainedSizeInBytes());
        return fileEntry.length();
    }

    private static Page createPage(Function<LineItem, Boolean> filter)
    {
        List<LineItemColumn> columns = getTestColumns();
        List<Type> columnTypes = columns.stream()
                .map(LineItemColumn::getType)
                .map(TestHivePageSink::getType)
                .map(hiveType -> TESTING_TYPE_MANAGER.getType(hiveType.getTypeSignature()))
                .collect(toList());
        PageBuilder pageBuilder = new PageBuilder(columnTypes);
        int rows = 0;
        for (LineItem lineItem : new LineItemGenerator(0.01, 1, 1)) {
            if (!filter.apply(lineItem)) {
                continue;
            }
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
        return pageBuilder.build();
    }

    static MaterializedResult toMaterializedResult(ConnectorSession session, List<Type> types, List<Page> pages)
    {
        // materialize pages
        MaterializedResult.Builder resultBuilder = MaterializedResult.resultBuilder(session, types);
        for (Page outputPage : pages) {
            resultBuilder.page(outputPage);
        }
        return resultBuilder.build();
    }

    private static ConnectorPageSource createPageSource(TrinoFileSystemFactory fileSystemFactory, HiveTransactionHandle transaction, HiveConfig config, Location location)
            throws IOException
    {
        long length = fileSystemFactory.create(ConnectorIdentity.ofUser("test")).newInputFile(location).length();
        Map<String, String> splitProperties = ImmutableMap.<String, String>builder()
                .put(FILE_INPUT_FORMAT, config.getHiveStorageFormat().getInputFormat())
                .put(LIST_COLUMNS, Joiner.on(',').join(getColumnHandles().stream().map(HiveColumnHandle::getName).collect(toImmutableList())))
                .put(LIST_COLUMN_TYPES, Joiner.on(',').join(getColumnHandles().stream().map(HiveColumnHandle::getHiveType).map(hiveType -> hiveType.getHiveTypeName().toString()).collect(toImmutableList())))
                .buildOrThrow();
        HiveSplit split = new HiveSplit(
                "",
                location.toString(),
                0,
                length,
                length,
                0,
                new Schema(config.getHiveStorageFormat().getSerde(), false, splitProperties),
                ImmutableList.of(),
                ImmutableList.of(),
                OptionalInt.empty(),
                OptionalInt.empty(),
                false,
                ImmutableMap.of(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                SplitWeight.standard());
        ConnectorTableHandle table = new HiveTableHandle(SCHEMA_NAME, TABLE_NAME, ImmutableMap.of(), ImmutableList.of(), ImmutableList.of(), Optional.empty());
        HivePageSourceProvider provider = new HivePageSourceProvider(
                TESTING_TYPE_MANAGER,
                config,
                getDefaultHivePageSourceFactories(fileSystemFactory, config));
        return provider.createPageSource(transaction, getHiveSession(config), split, table, ImmutableList.copyOf(getColumnHandles()), DynamicFilter.EMPTY);
    }

    private static ConnectorPageSink createPageSink(
            TrinoFileSystemFactory fileSystemFactory,
            HiveTransactionHandle transaction,
            HiveConfig config,
            SortingFileWriterConfig sortingFileWriterConfig,
            HiveMetastore metastore,
            Location location,
            HiveWriterStats stats,
            List<HiveColumnHandle> columnHandles)
    {
        LocationHandle locationHandle = new LocationHandle(location, location, DIRECT_TO_TARGET_NEW_DIRECTORY);
        HiveOutputTableHandle handle = new HiveOutputTableHandle(
                SCHEMA_NAME,
                TABLE_NAME,
                columnHandles,
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
                false);
        JsonCodec<PartitionUpdate> partitionUpdateCodec = JsonCodec.jsonCodec(PartitionUpdate.class);
        HivePageSinkProvider provider = new HivePageSinkProvider(
                getDefaultHiveFileWriterFactories(config, fileSystemFactory),
                HDFS_FILE_SYSTEM_FACTORY,
                PAGE_SORTER,
                HiveMetastoreFactory.ofInstance(metastore),
                new GroupByHashPageIndexerFactory(new FlatHashStrategyCompiler(new TypeOperators())),
                TESTING_TYPE_MANAGER,
                config,
                sortingFileWriterConfig,
                new HiveLocationService(HDFS_FILE_SYSTEM_FACTORY, config),
                partitionUpdateCodec,
                stats);
        return provider.createPageSink(transaction, getHiveSession(config), handle, TESTING_PAGE_SINK_ID);
    }

    private static List<HiveColumnHandle> getColumnHandles()
    {
        ImmutableList.Builder<HiveColumnHandle> handles = ImmutableList.builder();
        List<LineItemColumn> columns = getTestColumns();
        for (int i = 0; i < columns.size(); i++) {
            LineItemColumn column = columns.get(i);
            Type type = getType(column.getType());
            handles.add(createBaseColumn(column.getColumnName(), i, toHiveType(type), type, REGULAR, Optional.empty()));
        }
        return handles.build();
    }

    private static List<HiveColumnHandle> getPartitionedColumnHandles(String partitionColumn)
    {
        ImmutableList.Builder<HiveColumnHandle> handles = ImmutableList.builder();
        List<LineItemColumn> columns = getTestColumns();
        for (int i = 0; i < columns.size(); i++) {
            LineItemColumn column = columns.get(i);
            Type type = getType(column.getType());
            if (column.getColumnName().equals(partitionColumn)) {
                handles.add(createBaseColumn(column.getColumnName(), i, toHiveType(type), type, PARTITION_KEY, Optional.empty()));
            }
            else {
                handles.add(createBaseColumn(column.getColumnName(), i, toHiveType(type), type, REGULAR, Optional.empty()));
            }
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

    private static Type getType(TpchColumnType type)
    {
        return switch (type.getBase()) {
            case IDENTIFIER -> BIGINT;
            case INTEGER -> INTEGER;
            case DATE -> DATE;
            case DOUBLE -> DOUBLE;
            case VARCHAR -> VARCHAR;
        };
    }
}
