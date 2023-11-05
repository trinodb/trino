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
package io.trino.plugin.hive.orc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.memory.MemoryFileSystemFactory;
import io.trino.orc.OrcReaderOptions;
import io.trino.orc.OrcWriterOptions;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.hive.FileWriter;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HiveColumnProjectionInfo;
import io.trino.plugin.hive.HiveCompressionCodec;
import io.trino.plugin.hive.HiveConfig;
import io.trino.plugin.hive.HivePageSourceProvider;
import io.trino.plugin.hive.HiveType;
import io.trino.plugin.hive.NodeVersion;
import io.trino.plugin.hive.TableToPartitionMapping;
import io.trino.plugin.hive.WriterKind;
import io.trino.plugin.hive.metastore.StorageFormat;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.RowBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.RowType;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.stream.Collectors;

import static io.trino.hive.thrift.metastore.hive_metastoreConstants.FILE_INPUT_FORMAT;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.trino.plugin.hive.HiveColumnHandle.createBaseColumn;
import static io.trino.plugin.hive.HivePageSourceProvider.ColumnMapping.buildColumnMappings;
import static io.trino.plugin.hive.HiveStorageFormat.ORC;
import static io.trino.plugin.hive.HiveTestUtils.getHiveSession;
import static io.trino.plugin.hive.acid.AcidTransaction.NO_ACID_TRANSACTION;
import static io.trino.plugin.hive.util.SerdeConstants.SERIALIZATION_LIB;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RowType.field;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.util.stream.Collectors.toList;
import static org.joda.time.DateTimeZone.UTC;
import static org.testng.Assert.assertEquals;

class TestOrcPredicates
{
    private static final int NUM_ROWS = 50000;
    private static final FileFormatDataSourceStats STATS = new FileFormatDataSourceStats();

    private static final HiveColumnHandle INTEGER_COLUMN = createBaseColumn("column_primitive_integer", 0, HiveType.HIVE_INT, INTEGER, REGULAR, Optional.empty());
    private static final HiveColumnHandle STRUCT_COLUMN = createBaseColumn(
            "column1_struct",
            1,
            HiveType.toHiveType(RowType.rowType(field("field0", BIGINT), field("field1", BIGINT))),
            RowType.rowType(field("field0", BIGINT), field("field1", BIGINT)),
            REGULAR,
            Optional.empty());
    private static final HiveColumnHandle BIGINT_COLUMN = createBaseColumn("column_primitive_bigint", 2, HiveType.HIVE_LONG, BIGINT, REGULAR, Optional.empty());
    private static final List<HiveColumnHandle> COLUMNS = ImmutableList.of(INTEGER_COLUMN, STRUCT_COLUMN, BIGINT_COLUMN);

    private static final HiveColumnHandle STRUCT_FIELD1_COLUMN = new HiveColumnHandle(
            STRUCT_COLUMN.getBaseColumnName(),
            STRUCT_COLUMN.getBaseHiveColumnIndex(),
            STRUCT_COLUMN.getBaseHiveType(),
            STRUCT_COLUMN.getBaseType(),
            Optional.of(new HiveColumnProjectionInfo(
                    ImmutableList.of(1),
                    ImmutableList.of("field1"),
                    HiveType.HIVE_LONG,
                    BIGINT)),
            STRUCT_COLUMN.getColumnType(),
            STRUCT_COLUMN.getComment());
    private static final List<HiveColumnHandle> PROJECTED_COLUMNS = ImmutableList.of(BIGINT_COLUMN, STRUCT_FIELD1_COLUMN);

    @Test
    void testOrcPredicates()
            throws Exception
    {
        testOrcPredicates(getHiveSession(new HiveConfig(), new OrcReaderConfig().setUseColumnNames(true)));
        testOrcPredicates(getHiveSession(new HiveConfig(), new OrcReaderConfig()));
    }

    private static void testOrcPredicates(ConnectorSession session)
            throws Exception
    {
        TrinoFileSystemFactory fileSystemFactory = new MemoryFileSystemFactory();
        Location location = Location.of("memory:///test");
        writeTestFile(session, fileSystemFactory, location);

        // Verify predicates on base column
        // All rows returned for a satisfying predicate
        assertFilteredRows(fileSystemFactory, location, TupleDomain.withColumnDomains(ImmutableMap.of(BIGINT_COLUMN, Domain.singleValue(BIGINT, 6L))), COLUMNS, session, NUM_ROWS);
        // No rows returned for a mismatched predicate
        assertFilteredRows(fileSystemFactory, location, TupleDomain.withColumnDomains(ImmutableMap.of(BIGINT_COLUMN, Domain.singleValue(BIGINT, 1L))), COLUMNS, session, 0);

        // Verify predicates on projected column
        // All rows returned for a satisfying predicate
        assertFilteredRows(fileSystemFactory, location, TupleDomain.withColumnDomains(ImmutableMap.of(STRUCT_FIELD1_COLUMN, Domain.singleValue(BIGINT, 5L))), PROJECTED_COLUMNS, session, NUM_ROWS);
        // No rows returned for a mismatched predicate
        assertFilteredRows(fileSystemFactory, location, TupleDomain.withColumnDomains(ImmutableMap.of(STRUCT_FIELD1_COLUMN, Domain.singleValue(BIGINT, 6L))), PROJECTED_COLUMNS, session, 0);
    }

    private static void assertFilteredRows(
            TrinoFileSystemFactory fileSystemFactory,
            Location location,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            List<HiveColumnHandle> columnsToRead,
            ConnectorSession session,
            int expectedRows)
            throws IOException
    {
        try (ConnectorPageSource pageSource = createPageSource(fileSystemFactory, location, effectivePredicate, columnsToRead, session)) {
            int filteredRows = 0;
            while (!pageSource.isFinished()) {
                Page page = pageSource.getNextPage();
                if (page != null) {
                    filteredRows += page.getPositionCount();
                }
            }
            assertEquals(filteredRows, expectedRows);
        }
    }

    private static ConnectorPageSource createPageSource(
            TrinoFileSystemFactory fileSystemFactory,
            Location location,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            List<HiveColumnHandle> columns,
            ConnectorSession session)
            throws IOException
    {
        OrcPageSourceFactory readerFactory = new OrcPageSourceFactory(new OrcReaderOptions(), fileSystemFactory, STATS, UTC);

        long length = fileSystemFactory.create(session).newInputFile(location).length();
        List<HivePageSourceProvider.ColumnMapping> columnMappings = buildColumnMappings(
                "",
                ImmutableList.of(),
                columns,
                ImmutableList.of(),
                TableToPartitionMapping.empty(),
                location.toString(),
                OptionalInt.empty(),
                length,
                Instant.now().toEpochMilli());

        return HivePageSourceProvider.createHivePageSource(
                        ImmutableSet.of(readerFactory),
                        session,
                        location,
                        OptionalInt.empty(),
                        0,
                        length,
                        length,
                        getTableProperties(),
                        effectivePredicate,
                        TESTING_TYPE_MANAGER,
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        false,
                        NO_ACID_TRANSACTION,
                        columnMappings)
                .orElseThrow();
    }

    private static void writeTestFile(ConnectorSession session, TrinoFileSystemFactory fileSystemFactory, Location location)
    {
        FileWriter fileWriter = new OrcFileWriterFactory(TESTING_TYPE_MANAGER, new NodeVersion("test"), STATS, new OrcWriterOptions(), fileSystemFactory)
                .createFileWriter(
                        location,
                        COLUMNS.stream().map(HiveColumnHandle::getName).collect(toList()),
                        StorageFormat.fromHiveStorageFormat(ORC),
                        HiveCompressionCodec.NONE,
                        getTableProperties(),
                        session,
                        OptionalInt.empty(),
                        NO_ACID_TRANSACTION,
                        false,
                        WriterKind.INSERT)
                .orElseThrow();

        fileWriter.appendRows(new Page(
                RunLengthEncodedBlock.create(new IntArrayBlock(1, Optional.empty(), new int[] {3}), NUM_ROWS),
                RunLengthEncodedBlock.create(
                        RowBlock.fromFieldBlocks(1, new Block[] {
                                new LongArrayBlock(1, Optional.empty(), new long[] {4}),
                                new LongArrayBlock(1, Optional.empty(), new long[] {5})}),
                        NUM_ROWS),
                RunLengthEncodedBlock.create(new LongArrayBlock(1, Optional.empty(), new long[] {6}), NUM_ROWS)));

        fileWriter.commit();
    }

    private static Properties getTableProperties()
    {
        Properties tableProperties = new Properties();
        tableProperties.setProperty(FILE_INPUT_FORMAT, ORC.getInputFormat());
        tableProperties.setProperty(SERIALIZATION_LIB, ORC.getSerde());
        tableProperties.setProperty(
                "columns",
                COLUMNS.stream()
                        .map(HiveColumnHandle::getName)
                        .collect(Collectors.joining(",")));
        tableProperties.setProperty(
                "columns.types",
                COLUMNS.stream()
                        .map(HiveColumnHandle::getHiveType)
                        .map(HiveType::toString)
                        .collect(Collectors.joining(",")));
        return tableProperties;
    }
}
