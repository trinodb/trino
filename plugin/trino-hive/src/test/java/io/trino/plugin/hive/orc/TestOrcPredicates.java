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
import io.trino.orc.OrcReaderOptions;
import io.trino.orc.OrcWriterOptions;
import io.trino.plugin.hive.AbstractTestHiveFileFormats;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HiveCompressionCodec;
import io.trino.plugin.hive.HiveConfig;
import io.trino.plugin.hive.HivePageSourceProvider;
import io.trino.plugin.hive.HivePartitionKey;
import io.trino.plugin.hive.NodeVersion;
import io.trino.plugin.hive.TableToPartitionMapping;
import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import org.apache.hadoop.mapred.FileSplit;
import org.testng.annotations.Test;

import java.io.File;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.hadoop.ConfigurationInstantiator.newEmptyConfiguration;
import static io.trino.plugin.hive.HivePageSourceProvider.ColumnMapping.buildColumnMappings;
import static io.trino.plugin.hive.HiveStorageFormat.ORC;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_FILE_SYSTEM_FACTORY;
import static io.trino.plugin.hive.HiveTestUtils.getHiveSession;
import static io.trino.plugin.hive.acid.AcidTransaction.NO_ACID_TRANSACTION;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.StructuralTestUtil.rowBlockOf;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.FILE_INPUT_FORMAT;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_LIB;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardStructObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaIntObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaLongObjectInspector;
import static org.joda.time.DateTimeZone.UTC;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestOrcPredicates
        extends AbstractTestHiveFileFormats
{
    private static final int NUM_ROWS = 50000;
    private static final FileFormatDataSourceStats STATS = new FileFormatDataSourceStats();

    // Prepare test columns
    private static final TestColumn columnPrimitiveInteger = new TestColumn("column_primitive_integer", javaIntObjectInspector, 3, 3);
    private static final TestColumn columnStruct = new TestColumn(
            "column1_struct",
            getStandardStructObjectInspector(ImmutableList.of("field0", "field1"), ImmutableList.of(javaLongObjectInspector, javaLongObjectInspector)),
            new Long[] {4L, 5L},
            rowBlockOf(ImmutableList.of(BIGINT, BIGINT), 4L, 5L));
    private static final TestColumn columnPrimitiveBigInt = new TestColumn("column_primitive_bigint", javaLongObjectInspector, 6L, 6L);

    @Test
    public void testOrcPredicates()
            throws Exception
    {
        testOrcPredicates(getHiveSession(new HiveConfig(), new OrcReaderConfig().setUseColumnNames(true)));
        testOrcPredicates(getHiveSession(new HiveConfig(), new OrcReaderConfig()));
    }

    private void testOrcPredicates(ConnectorSession session)
            throws Exception
    {
        List<TestColumn> columnsToWrite = ImmutableList.of(columnPrimitiveInteger, columnStruct, columnPrimitiveBigInt);

        File file = File.createTempFile("test", "orc_predicate");
        file.delete();
        try {
            // Write data
            OrcFileWriterFactory writerFactory = new OrcFileWriterFactory(TESTING_TYPE_MANAGER, new NodeVersion("test"), STATS, new OrcWriterOptions(), HDFS_FILE_SYSTEM_FACTORY);
            FileSplit split = createTestFileTrino(file.getAbsolutePath(), ORC, HiveCompressionCodec.NONE, columnsToWrite, session, NUM_ROWS, writerFactory);

            TupleDomain<TestColumn> testingPredicate;

            // Verify predicates on base column
            List<TestColumn> columnsToRead = columnsToWrite;
            // All rows returned for a satisfying predicate
            testingPredicate = TupleDomain.withColumnDomains(ImmutableMap.of(columnPrimitiveBigInt, Domain.singleValue(BIGINT, 6L)));
            assertFilteredRows(testingPredicate, columnsToRead, session, split, NUM_ROWS);
            // No rows returned for a mismatched predicate
            testingPredicate = TupleDomain.withColumnDomains(ImmutableMap.of(columnPrimitiveBigInt, Domain.singleValue(BIGINT, 1L)));
            assertFilteredRows(testingPredicate, columnsToRead, session, split, 0);

            // Verify predicates on projected column
            TestColumn projectedColumn = new TestColumn(
                    columnStruct.getBaseName(),
                    columnStruct.getBaseObjectInspector(),
                    ImmutableList.of("field1"),
                    ImmutableList.of(1),
                    javaLongObjectInspector,
                    5L,
                    5L,
                    false);

            columnsToRead = ImmutableList.of(columnPrimitiveBigInt, projectedColumn);
            // All rows returned for a satisfying predicate
            testingPredicate = TupleDomain.withColumnDomains(ImmutableMap.of(projectedColumn, Domain.singleValue(BIGINT, 5L)));
            assertFilteredRows(testingPredicate, columnsToRead, session, split, NUM_ROWS);
            // No rows returned for a mismatched predicate
            testingPredicate = TupleDomain.withColumnDomains(ImmutableMap.of(projectedColumn, Domain.singleValue(BIGINT, 6L)));
            assertFilteredRows(testingPredicate, columnsToRead, session, split, 0);
        }
        finally {
            file.delete();
        }
    }

    private void assertFilteredRows(
            TupleDomain<TestColumn> effectivePredicate,
            List<TestColumn> columnsToRead,
            ConnectorSession session,
            FileSplit split,
            int expectedRows)
    {
        ConnectorPageSource pageSource = createPageSource(effectivePredicate, columnsToRead, session, split);

        int filteredRows = 0;
        while (!pageSource.isFinished()) {
            Page page = pageSource.getNextPage();
            if (page != null) {
                filteredRows += page.getPositionCount();
            }
        }

        assertEquals(filteredRows, expectedRows);
    }

    private ConnectorPageSource createPageSource(
            TupleDomain<TestColumn> effectivePredicate,
            List<TestColumn> columnsToRead,
            ConnectorSession session,
            FileSplit split)
    {
        OrcPageSourceFactory readerFactory = new OrcPageSourceFactory(new OrcReaderOptions(), HDFS_FILE_SYSTEM_FACTORY, STATS, UTC);

        Properties splitProperties = new Properties();
        splitProperties.setProperty(FILE_INPUT_FORMAT, ORC.getInputFormat());
        splitProperties.setProperty(SERIALIZATION_LIB, ORC.getSerde());

        // Use full columns in split properties
        ImmutableList.Builder<String> splitPropertiesColumnNames = ImmutableList.builder();
        ImmutableList.Builder<String> splitPropertiesColumnTypes = ImmutableList.builder();
        Set<String> baseColumnNames = new HashSet<>();
        for (TestColumn columnToRead : columnsToRead) {
            String name = columnToRead.getBaseName();
            if (!baseColumnNames.contains(name) && !columnToRead.isPartitionKey()) {
                baseColumnNames.add(name);
                splitPropertiesColumnNames.add(name);
                splitPropertiesColumnTypes.add(columnToRead.getBaseObjectInspector().getTypeName());
            }
        }

        splitProperties.setProperty("columns", splitPropertiesColumnNames.build().stream().collect(Collectors.joining(",")));
        splitProperties.setProperty("columns.types", splitPropertiesColumnTypes.build().stream().collect(Collectors.joining(",")));

        List<HivePartitionKey> partitionKeys = columnsToRead.stream()
                .filter(TestColumn::isPartitionKey)
                .map(input -> new HivePartitionKey(input.getName(), (String) input.getWriteValue()))
                .collect(toList());

        String partitionName = String.join("/", partitionKeys.stream()
                .map(partitionKey -> format("%s=%s", partitionKey.getName(), partitionKey.getValue()))
                .collect(toImmutableList()));

        List<HiveColumnHandle> columnHandles = getColumnHandles(columnsToRead);

        TupleDomain<HiveColumnHandle> predicate = effectivePredicate.transformKeys(testColumn -> {
            Optional<HiveColumnHandle> handle = columnHandles.stream()
                    .filter(column -> testColumn.getName().equals(column.getName()))
                    .findFirst();

            checkState(handle.isPresent(), "Predicate on invalid column");
            return handle.get();
        });

        List<HivePageSourceProvider.ColumnMapping> columnMappings = buildColumnMappings(
                partitionName,
                partitionKeys,
                columnHandles,
                ImmutableList.of(),
                TableToPartitionMapping.empty(),
                split.getPath().toString(),
                OptionalInt.empty(),
                split.getLength(),
                Instant.now().toEpochMilli());

        Optional<ConnectorPageSource> pageSource = HivePageSourceProvider.createHivePageSource(
                ImmutableSet.of(readerFactory),
                ImmutableSet.of(),
                newEmptyConfiguration(),
                session,
                Location.of(split.getPath().toString()),
                OptionalInt.empty(),
                split.getStart(),
                split.getLength(),
                split.getLength(),
                splitProperties,
                predicate,
                columnHandles,
                TESTING_TYPE_MANAGER,
                Optional.empty(),
                Optional.empty(),
                false,
                Optional.empty(),
                false,
                NO_ACID_TRANSACTION,
                columnMappings);

        assertTrue(pageSource.isPresent());
        return pageSource.get();
    }
}
