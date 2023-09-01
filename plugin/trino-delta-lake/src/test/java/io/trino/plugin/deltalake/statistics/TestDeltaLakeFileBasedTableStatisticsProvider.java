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
package io.trino.plugin.deltalake.statistics;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import io.airlift.json.JsonCodecFactory;
import io.trino.plugin.deltalake.DeltaLakeColumnHandle;
import io.trino.plugin.deltalake.DeltaLakeConfig;
import io.trino.plugin.deltalake.DeltaLakeTableHandle;
import io.trino.plugin.deltalake.transactionlog.MetadataEntry;
import io.trino.plugin.deltalake.transactionlog.ProtocolEntry;
import io.trino.plugin.deltalake.transactionlog.TableSnapshot;
import io.trino.plugin.deltalake.transactionlog.TransactionLogAccess;
import io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointSchemaManager;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.TypeManager;
import io.trino.testing.TestingConnectorContext;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.time.LocalDate;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;

import static io.trino.plugin.deltalake.DeltaLakeColumnType.REGULAR;
import static io.trino.plugin.deltalake.DeltaTestingConnectorSession.SESSION;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_FILE_SYSTEM_FACTORY;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.POSITIVE_INFINITY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;

public class TestDeltaLakeFileBasedTableStatisticsProvider
{
    private static final ColumnHandle COLUMN_HANDLE = new DeltaLakeColumnHandle("val", DoubleType.DOUBLE, OptionalInt.empty(), "val", DoubleType.DOUBLE, REGULAR, Optional.empty());

    private TransactionLogAccess transactionLogAccess;
    private CachingExtendedStatisticsAccess statistics;
    private DeltaLakeTableStatisticsProvider tableStatisticsProvider;

    @BeforeClass
    public void setupMetastore()
    {
        TestingConnectorContext context = new TestingConnectorContext();
        TypeManager typeManager = context.getTypeManager();
        CheckpointSchemaManager checkpointSchemaManager = new CheckpointSchemaManager(typeManager);

        FileFormatDataSourceStats fileFormatDataSourceStats = new FileFormatDataSourceStats();

        transactionLogAccess = new TransactionLogAccess(
                typeManager,
                checkpointSchemaManager,
                new DeltaLakeConfig(),
                fileFormatDataSourceStats,
                HDFS_FILE_SYSTEM_FACTORY,
                new ParquetReaderConfig());

        statistics = new CachingExtendedStatisticsAccess(new MetaDirStatisticsAccess(HDFS_FILE_SYSTEM_FACTORY, new JsonCodecFactory().jsonCodec(ExtendedStatistics.class)));
        tableStatisticsProvider = new FileBasedTableStatisticsProvider(
                typeManager,
                transactionLogAccess,
                statistics);
    }

    private DeltaLakeTableHandle registerTable(String tableName)
    {
        return registerTable(tableName, tableName);
    }

    private DeltaLakeTableHandle registerTable(String tableName, String directoryName)
    {
        String tableLocation = Resources.getResource("statistics/" + directoryName).toExternalForm();
        SchemaTableName schemaTableName = new SchemaTableName("db_name", tableName);
        TableSnapshot tableSnapshot;
        try {
            tableSnapshot = transactionLogAccess.loadSnapshot(schemaTableName, tableLocation, SESSION);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        MetadataEntry metadataEntry = transactionLogAccess.getMetadataEntry(tableSnapshot, SESSION);
        return new DeltaLakeTableHandle(
                schemaTableName.getSchemaName(),
                schemaTableName.getTableName(),
                false,
                tableLocation,
                metadataEntry,
                new ProtocolEntry(1, 2, Optional.empty(), Optional.empty()),
                TupleDomain.all(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                0);
    }

    @Test
    public void testStatisticsNaN()
    {
        DeltaLakeTableHandle tableHandle = registerTable("nan");
        TableStatistics stats = getTableStatistics(SESSION, tableHandle);
        assertEquals(stats.getRowCount(), Estimate.of(1));
        assertEquals(stats.getColumnStatistics().size(), 1);

        ColumnStatistics columnStatistics = stats.getColumnStatistics().get(COLUMN_HANDLE);
        assertEquals(columnStatistics.getRange(), Optional.empty());
    }

    @Test
    public void testStatisticsInf()
    {
        DeltaLakeTableHandle tableHandle = registerTable("positive_infinity");
        TableStatistics stats = getTableStatistics(SESSION, tableHandle);
        ColumnStatistics columnStatistics = stats.getColumnStatistics().get(COLUMN_HANDLE);
        assertEquals(columnStatistics.getRange().get().getMin(), POSITIVE_INFINITY);
        assertEquals(columnStatistics.getRange().get().getMax(), POSITIVE_INFINITY);
    }

    @Test
    public void testStatisticsNegInf()
    {
        DeltaLakeTableHandle tableHandle = registerTable("negative_infinity");
        TableStatistics stats = getTableStatistics(SESSION, tableHandle);
        ColumnStatistics columnStatistics = stats.getColumnStatistics().get(COLUMN_HANDLE);
        assertEquals(columnStatistics.getRange().get().getMin(), NEGATIVE_INFINITY);
        assertEquals(columnStatistics.getRange().get().getMax(), NEGATIVE_INFINITY);
    }

    @Test
    public void testStatisticsNegZero()
    {
        DeltaLakeTableHandle tableHandle = registerTable("negative_zero");
        TableStatistics stats = getTableStatistics(SESSION, tableHandle);
        ColumnStatistics columnStatistics = stats.getColumnStatistics().get(COLUMN_HANDLE);
        assertEquals(columnStatistics.getRange().get().getMin(), -0.0d);
        assertEquals(columnStatistics.getRange().get().getMax(), -0.0d);
    }

    @Test
    public void testStatisticsInfinityAndNaN()
    {
        // Stats with NaN values cannot be used
        DeltaLakeTableHandle tableHandle = registerTable("infinity_nan");
        TableStatistics stats = getTableStatistics(SESSION, tableHandle);
        ColumnStatistics columnStatistics = stats.getColumnStatistics().get(COLUMN_HANDLE);
        assertEquals(columnStatistics.getRange().get().getMin(), POSITIVE_INFINITY);
        assertEquals(columnStatistics.getRange().get().getMax(), POSITIVE_INFINITY);
    }

    @Test
    public void testStatisticsNegativeInfinityAndNaN()
    {
        // Stats with NaN values cannot be used
        DeltaLakeTableHandle tableHandle = registerTable("negative_infinity_nan");
        TableStatistics stats = getTableStatistics(SESSION, tableHandle);
        ColumnStatistics columnStatistics = stats.getColumnStatistics().get(COLUMN_HANDLE);
        assertEquals(columnStatistics.getRange().get().getMin(), NEGATIVE_INFINITY);
        assertEquals(columnStatistics.getRange().get().getMax(), POSITIVE_INFINITY);
    }

    @Test
    public void testStatisticsZeroAndNaN()
    {
        // Stats with NaN values cannot be used
        DeltaLakeTableHandle tableHandle = registerTable("zero_nan");
        TableStatistics stats = getTableStatistics(SESSION, tableHandle);
        ColumnStatistics columnStatistics = stats.getColumnStatistics().get(COLUMN_HANDLE);
        assertEquals(columnStatistics.getRange().get().getMin(), 0.0);
        assertEquals(columnStatistics.getRange().get().getMax(), POSITIVE_INFINITY);
    }

    @Test
    public void testStatisticsZeroAndInfinity()
    {
        DeltaLakeTableHandle tableHandle = registerTable("zero_infinity");
        TableStatistics stats = getTableStatistics(SESSION, tableHandle);
        ColumnStatistics columnStatistics = stats.getColumnStatistics().get(COLUMN_HANDLE);
        assertEquals(columnStatistics.getRange().get().getMin(), 0.0);
        assertEquals(columnStatistics.getRange().get().getMax(), POSITIVE_INFINITY);
    }

    @Test
    public void testStatisticsZeroAndNegativeInfinity()
    {
        DeltaLakeTableHandle tableHandle = registerTable("zero_negative_infinity");
        TableStatistics stats = getTableStatistics(SESSION, tableHandle);
        ColumnStatistics columnStatistics = stats.getColumnStatistics().get(COLUMN_HANDLE);
        assertEquals(columnStatistics.getRange().get().getMin(), NEGATIVE_INFINITY);
        assertEquals(columnStatistics.getRange().get().getMax(), 0.0);
    }

    @Test
    public void testStatisticsNaNWithMultipleFiles()
    {
        // Stats with NaN values cannot be used. This transaction combines a file with NaN min/max values with one with 0.0 min/max values
        DeltaLakeTableHandle tableHandle = registerTable("nan_multi_file");
        TableStatistics stats = getTableStatistics(SESSION, tableHandle);
        ColumnStatistics columnStatistics = stats.getColumnStatistics().get(COLUMN_HANDLE);
        assertEquals(columnStatistics.getRange(), Optional.empty());
    }

    @Test
    public void testStatisticsMultipleFiles()
    {
        DeltaLakeTableHandle tableHandle = registerTable("basic_multi_file");
        TableStatistics stats = getTableStatistics(SESSION, tableHandle);
        ColumnStatistics columnStatistics = stats.getColumnStatistics().get(COLUMN_HANDLE);
        assertEquals(columnStatistics.getRange().get().getMin(), -42.0);
        assertEquals(columnStatistics.getRange().get().getMax(), 42.0);

        DeltaLakeTableHandle tableHandleWithUnenforcedConstraint = new DeltaLakeTableHandle(
                tableHandle.getSchemaName(),
                tableHandle.getTableName(),
                tableHandle.isManaged(),
                tableHandle.getLocation(),
                tableHandle.getMetadataEntry(),
                tableHandle.getProtocolEntry(),
                TupleDomain.all(),
                TupleDomain.withColumnDomains(ImmutableMap.of((DeltaLakeColumnHandle) COLUMN_HANDLE, Domain.singleValue(DOUBLE, 42.0))),
                tableHandle.getWriteType(),
                tableHandle.getProjectedColumns(),
                tableHandle.getUpdatedColumns(),
                tableHandle.getUpdateRowIdColumns(),
                tableHandle.getAnalyzeHandle(),
                0);
        stats = getTableStatistics(SESSION, tableHandleWithUnenforcedConstraint);
        columnStatistics = stats.getColumnStatistics().get(COLUMN_HANDLE);
        assertEquals(columnStatistics.getRange().get().getMin(), 0.0);
        assertEquals(columnStatistics.getRange().get().getMax(), 42.0);
    }

    @Test
    public void testStatisticsNoRecords()
    {
        DeltaLakeTableHandle tableHandle = registerTable("zero_record_count", "basic_multi_file");
        DeltaLakeTableHandle tableHandleWithNoneEnforcedConstraint = new DeltaLakeTableHandle(
                tableHandle.getSchemaName(),
                tableHandle.getTableName(),
                tableHandle.isManaged(),
                tableHandle.getLocation(),
                tableHandle.getMetadataEntry(),
                tableHandle.getProtocolEntry(),
                TupleDomain.none(),
                TupleDomain.all(),
                tableHandle.getWriteType(),
                tableHandle.getProjectedColumns(),
                tableHandle.getUpdatedColumns(),
                tableHandle.getUpdateRowIdColumns(),
                tableHandle.getAnalyzeHandle(),
                0);
        DeltaLakeTableHandle tableHandleWithNoneUnenforcedConstraint = new DeltaLakeTableHandle(
                tableHandle.getSchemaName(),
                tableHandle.getTableName(),
                tableHandle.isManaged(),
                tableHandle.getLocation(),
                tableHandle.getMetadataEntry(),
                tableHandle.getProtocolEntry(),
                TupleDomain.all(),
                TupleDomain.none(),
                tableHandle.getWriteType(),
                tableHandle.getProjectedColumns(),
                tableHandle.getUpdatedColumns(),
                tableHandle.getUpdateRowIdColumns(),
                tableHandle.getAnalyzeHandle(),
                0);
        // If either the table handle's constraint or the provided Constraint are none, it will cause a 0 record count to be reported
        assertEmptyStats(getTableStatistics(SESSION, tableHandleWithNoneEnforcedConstraint));
        assertEmptyStats(getTableStatistics(SESSION, tableHandleWithNoneUnenforcedConstraint));
    }

    private void assertEmptyStats(TableStatistics tableStatistics)
    {
        assertEquals(tableStatistics.getRowCount(), Estimate.of(0));
        ColumnStatistics columnStatistics = tableStatistics.getColumnStatistics().get(COLUMN_HANDLE);
        assertEquals(columnStatistics.getNullsFraction(), Estimate.of(0));
        assertEquals(columnStatistics.getDistinctValuesCount(), Estimate.of(0));
    }

    @Test
    public void testStatisticsParquetParsedStatistics()
    {
        // The transaction log for this table was created so that the checkpoints only write struct statistics, not json statistics
        DeltaLakeTableHandle tableHandle = registerTable("parquet_struct_statistics");
        TableStatistics stats = getTableStatistics(SESSION, tableHandle);
        assertEquals(stats.getRowCount(), Estimate.of(9));

        Map<ColumnHandle, ColumnStatistics> statisticsMap = stats.getColumnStatistics();
        ColumnStatistics columnStats = statisticsMap.get(new DeltaLakeColumnHandle("dec_short", DecimalType.createDecimalType(5, 1), OptionalInt.empty(), "dec_short", DecimalType.createDecimalType(5, 1), REGULAR, Optional.empty()));
        assertEquals(columnStats.getNullsFraction(), Estimate.zero());
        assertEquals(columnStats.getRange().get().getMin(), -10.1);
        assertEquals(columnStats.getRange().get().getMax(), 10.1);

        columnStats = statisticsMap.get(new DeltaLakeColumnHandle("dec_long", DecimalType.createDecimalType(25, 3), OptionalInt.empty(), "dec_long", DecimalType.createDecimalType(25, 3), REGULAR, Optional.empty()));
        assertEquals(columnStats.getNullsFraction(), Estimate.zero());
        assertEquals(columnStats.getRange().get().getMin(), -999999999999.123);
        assertEquals(columnStats.getRange().get().getMax(), 999999999999.123);

        columnStats = statisticsMap.get(new DeltaLakeColumnHandle("l", BIGINT, OptionalInt.empty(), "l", BIGINT, REGULAR, Optional.empty()));
        assertEquals(columnStats.getNullsFraction(), Estimate.zero());
        assertEquals(columnStats.getRange().get().getMin(), -10000000.0);
        assertEquals(columnStats.getRange().get().getMax(), 10000000.0);

        columnStats = statisticsMap.get(new DeltaLakeColumnHandle("in", INTEGER, OptionalInt.empty(), "in", INTEGER, REGULAR, Optional.empty()));
        assertEquals(columnStats.getNullsFraction(), Estimate.zero());
        assertEquals(columnStats.getRange().get().getMin(), -20000000.0);
        assertEquals(columnStats.getRange().get().getMax(), 20000000.0);

        columnStats = statisticsMap.get(new DeltaLakeColumnHandle("sh", SMALLINT, OptionalInt.empty(), "sh", SMALLINT, REGULAR, Optional.empty()));
        assertEquals(columnStats.getNullsFraction(), Estimate.zero());
        assertEquals(columnStats.getRange().get().getMin(), -123.0);
        assertEquals(columnStats.getRange().get().getMax(), 123.0);

        columnStats = statisticsMap.get(new DeltaLakeColumnHandle("byt", TINYINT, OptionalInt.empty(), "byt", TINYINT, REGULAR, Optional.empty()));
        assertEquals(columnStats.getNullsFraction(), Estimate.zero());
        assertEquals(columnStats.getRange().get().getMin(), -42.0);
        assertEquals(columnStats.getRange().get().getMax(), 42.0);

        columnStats = statisticsMap.get(new DeltaLakeColumnHandle("fl", REAL, OptionalInt.empty(), "fl", REAL, REGULAR, Optional.empty()));
        assertEquals(columnStats.getNullsFraction(), Estimate.zero());
        assertEquals((float) columnStats.getRange().get().getMin(), -0.123f);
        assertEquals((float) columnStats.getRange().get().getMax(), 0.123f);

        columnStats = statisticsMap.get(new DeltaLakeColumnHandle("dou", DOUBLE, OptionalInt.empty(), "dou", DOUBLE, REGULAR, Optional.empty()));
        assertEquals(columnStats.getNullsFraction(), Estimate.zero());
        assertEquals(columnStats.getRange().get().getMin(), -0.321);
        assertEquals(columnStats.getRange().get().getMax(), 0.321);

        columnStats = statisticsMap.get(new DeltaLakeColumnHandle("dat", DATE, OptionalInt.empty(), "dat", DATE, REGULAR, Optional.empty()));
        assertEquals(columnStats.getNullsFraction(), Estimate.zero());
        assertEquals(columnStats.getRange().get().getMin(), (double) LocalDate.parse("1900-01-01").toEpochDay());
        assertEquals(columnStats.getRange().get().getMax(), (double) LocalDate.parse("5000-01-01").toEpochDay());
    }

    @Test
    public void testStatisticsParquetParsedStatisticsNaNValues()
    {
        // The transaction log for this table was created so that the checkpoints only write struct statistics, not json statistics
        // The table has a REAL and DOUBLE columns each with 9 values, one of them being NaN
        DeltaLakeTableHandle tableHandle = registerTable("parquet_struct_statistics_nan");
        TableStatistics stats = getTableStatistics(SESSION, tableHandle);
        assertEquals(stats.getRowCount(), Estimate.of(9));

        Map<ColumnHandle, ColumnStatistics> statisticsMap = stats.getColumnStatistics();
        ColumnStatistics columnStats = statisticsMap.get(new DeltaLakeColumnHandle("fl", REAL, OptionalInt.empty(), "fl", REAL, REGULAR, Optional.empty()));
        assertEquals(columnStats.getNullsFraction(), Estimate.zero());
        assertThat(columnStats.getRange()).isEmpty();

        columnStats = statisticsMap.get(new DeltaLakeColumnHandle("dou", DOUBLE, OptionalInt.empty(), "dou", DOUBLE, REGULAR, Optional.empty()));
        assertEquals(columnStats.getNullsFraction(), Estimate.zero());
        assertThat(columnStats.getRange()).isEmpty();
    }

    @Test
    public void testStatisticsParquetParsedStatisticsNullCount()
    {
        // The transaction log for this table was created so that the checkpoints only write struct statistics, not json statistics
        // The table has one INTEGER column 'i' where 3 of the 9 values are null
        DeltaLakeTableHandle tableHandle = registerTable("parquet_struct_statistics_null_count");
        TableStatistics stats = getTableStatistics(SESSION, tableHandle);
        assertEquals(stats.getRowCount(), Estimate.of(9));

        Map<ColumnHandle, ColumnStatistics> statisticsMap = stats.getColumnStatistics();
        ColumnStatistics columnStats = statisticsMap.get(new DeltaLakeColumnHandle("i", INTEGER, OptionalInt.empty(), "i", INTEGER, REGULAR, Optional.empty()));
        assertEquals(columnStats.getNullsFraction(), Estimate.of(3.0 / 9.0));
    }

    @Test
    public void testExtendedStatisticsWithoutDataSize()
    {
        // Read extended_stats.json that was generated before supporting data_size
        Optional<ExtendedStatistics> extendedStatistics = readExtendedStatisticsFromTableResource("statistics/extended_stats_without_data_size");
        assertThat(extendedStatistics).isNotEmpty();
        Map<String, DeltaLakeColumnStatistics> columnStatistics = extendedStatistics.get().getColumnStatistics();
        assertThat(columnStatistics).hasSize(3);
    }

    @Test
    public void testExtendedStatisticsWithDataSize()
    {
        // Read extended_stats.json that was generated after supporting data_size
        Optional<ExtendedStatistics> extendedStatistics = readExtendedStatisticsFromTableResource("statistics/extended_stats_with_data_size");
        assertThat(extendedStatistics).isNotEmpty();
        Map<String, DeltaLakeColumnStatistics> columnStatistics = extendedStatistics.get().getColumnStatistics();
        assertThat(columnStatistics).hasSize(3);
        assertEquals(columnStatistics.get("regionkey").getTotalSizeInBytes(), OptionalLong.empty());
        assertEquals(columnStatistics.get("name").getTotalSizeInBytes(), OptionalLong.of(34));
        assertEquals(columnStatistics.get("comment").getTotalSizeInBytes(), OptionalLong.of(330));
    }

    @Test
    public void testMergeExtendedStatisticsWithoutAndWithDataSize()
    {
        // Merge two extended stats files. The first file doesn't have totalSizeInBytes field and the second file has totalSizeInBytes field
        Optional<ExtendedStatistics> statisticsWithoutDataSize = readExtendedStatisticsFromTableResource("statistics/extended_stats_without_data_size");
        Optional<ExtendedStatistics> statisticsWithDataSize = readExtendedStatisticsFromTableResource("statistics/extended_stats_with_data_size");
        assertThat(statisticsWithoutDataSize).isNotEmpty();
        assertThat(statisticsWithDataSize).isNotEmpty();

        Map<String, DeltaLakeColumnStatistics> columnStatisticsWithoutDataSize = statisticsWithoutDataSize.get().getColumnStatistics();
        Map<String, DeltaLakeColumnStatistics> columnStatisticsWithDataSize = statisticsWithDataSize.get().getColumnStatistics();

        DeltaLakeColumnStatistics mergedRegionKey = columnStatisticsWithoutDataSize.get("regionkey").update(columnStatisticsWithDataSize.get("regionkey"));
        assertEquals(mergedRegionKey.getTotalSizeInBytes(), OptionalLong.empty());
        assertEquals(mergedRegionKey.getNdvSummary().cardinality(), 5);

        DeltaLakeColumnStatistics mergedName = columnStatisticsWithoutDataSize.get("name").update(columnStatisticsWithDataSize.get("name"));
        assertEquals(mergedName.getTotalSizeInBytes(), OptionalLong.empty());
        assertEquals(mergedName.getNdvSummary().cardinality(), 5);

        DeltaLakeColumnStatistics mergedComment = columnStatisticsWithoutDataSize.get("comment").update(columnStatisticsWithDataSize.get("comment"));
        assertEquals(mergedComment.getTotalSizeInBytes(), OptionalLong.empty());
        assertEquals(mergedComment.getNdvSummary().cardinality(), 5);
    }

    private TableStatistics getTableStatistics(ConnectorSession session, DeltaLakeTableHandle tableHandle)
    {
        TableSnapshot tableSnapshot;
        try {
            tableSnapshot = transactionLogAccess.loadSnapshot(tableHandle.getSchemaTableName(), tableHandle.getLocation(), SESSION);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        return tableStatisticsProvider.getTableStatistics(session, tableHandle, tableSnapshot);
    }

    private Optional<ExtendedStatistics> readExtendedStatisticsFromTableResource(String tableLocationResourceName)
    {
        SchemaTableName name = new SchemaTableName("some_ignored_schema", "some_ignored_name");
        String tableLocation = Resources.getResource(tableLocationResourceName).toExternalForm();
        return statistics.readExtendedStatistics(SESSION, name, tableLocation);
    }
}
