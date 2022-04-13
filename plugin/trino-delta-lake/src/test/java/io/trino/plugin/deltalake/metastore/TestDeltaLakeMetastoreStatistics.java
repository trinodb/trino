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
package io.trino.plugin.deltalake.metastore;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import com.google.common.io.Resources;
import io.airlift.json.JsonCodecFactory;
import io.trino.plugin.deltalake.DeltaLakeColumnHandle;
import io.trino.plugin.deltalake.DeltaLakeConfig;
import io.trino.plugin.deltalake.DeltaLakeSessionProperties;
import io.trino.plugin.deltalake.DeltaLakeTableHandle;
import io.trino.plugin.deltalake.statistics.CachingExtendedStatisticsAccess;
import io.trino.plugin.deltalake.statistics.ExtendedStatistics;
import io.trino.plugin.deltalake.statistics.MetaDirStatisticsAccess;
import io.trino.plugin.deltalake.transactionlog.MetadataEntry;
import io.trino.plugin.deltalake.transactionlog.TransactionLogAccess;
import io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointSchemaManager;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.hive.HdfsConfig;
import io.trino.plugin.hive.HdfsConfiguration;
import io.trino.plugin.hive.HdfsConfigurationInitializer;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.HiveHdfsConfiguration;
import io.trino.plugin.hive.HiveType;
import io.trino.plugin.hive.NodeVersion;
import io.trino.plugin.hive.authentication.NoHdfsAuthentication;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.MetastoreConfig;
import io.trino.plugin.hive.metastore.PrincipalPrivileges;
import io.trino.plugin.hive.metastore.Storage;
import io.trino.plugin.hive.metastore.StorageFormat;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.hive.metastore.file.FileHiveMetastore;
import io.trino.plugin.hive.metastore.file.FileHiveMetastoreConfig;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.hive.parquet.ParquetWriterConfig;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.Constraint;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.PrincipalType;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.TypeManager;
import io.trino.testing.TestingConnectorContext;
import io.trino.testing.TestingConnectorSession;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.time.LocalDate;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static io.trino.plugin.deltalake.DeltaLakeColumnType.REGULAR;
import static io.trino.plugin.deltalake.DeltaLakeMetadata.PATH_PROPERTY;
import static io.trino.plugin.deltalake.metastore.HiveMetastoreBackedDeltaLakeMetastore.TABLE_PROVIDER_PROPERTY;
import static io.trino.plugin.deltalake.metastore.HiveMetastoreBackedDeltaLakeMetastore.TABLE_PROVIDER_VALUE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.testing.assertions.Assert.assertEquals;
import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.POSITIVE_INFINITY;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDeltaLakeMetastoreStatistics
{
    private static final ColumnHandle COLUMN_HANDLE = new DeltaLakeColumnHandle("val", DoubleType.DOUBLE, REGULAR);
    private static final ConnectorSession SESSION = TestingConnectorSession.builder()
            .setPropertyMetadata(new DeltaLakeSessionProperties(new DeltaLakeConfig(), new ParquetReaderConfig(), new ParquetWriterConfig()).getSessionProperties())
            .build();

    private DeltaLakeMetastore deltaLakeMetastore;
    private HiveMetastore hiveMetastore;

    @BeforeClass
    public void setupMetastore()
    {
        TestingConnectorContext context = new TestingConnectorContext();
        TypeManager typeManager = context.getTypeManager();
        CheckpointSchemaManager checkpointSchemaManager = new CheckpointSchemaManager(typeManager);

        HdfsConfig hdfsConfig = new HdfsConfig();
        HdfsConfiguration hdfsConfiguration = new HiveHdfsConfiguration(new HdfsConfigurationInitializer(hdfsConfig), ImmutableSet.of());
        HdfsEnvironment hdfsEnvironment = new HdfsEnvironment(hdfsConfiguration, hdfsConfig, new NoHdfsAuthentication());
        FileFormatDataSourceStats fileFormatDataSourceStats = new FileFormatDataSourceStats();

        TransactionLogAccess transactionLogAccess = new TransactionLogAccess(
                typeManager,
                checkpointSchemaManager,
                new DeltaLakeConfig(),
                fileFormatDataSourceStats,
                hdfsEnvironment,
                new ParquetReaderConfig());

        File tmpDir = Files.createTempDir();
        File metastoreDir = new File(tmpDir, "metastore");
        hiveMetastore = new FileHiveMetastore(
                new NodeVersion("test_version"),
                hdfsEnvironment,
                new MetastoreConfig(),
                new FileHiveMetastoreConfig()
                        .setCatalogDirectory(metastoreDir.toURI().toString())
                        .setMetastoreUser("test"));

        hiveMetastore.createDatabase(new Database("db_name", Optional.empty(), Optional.of("test"), Optional.of(PrincipalType.USER), Optional.empty(), ImmutableMap.of()));

        CachingExtendedStatisticsAccess statistics = new CachingExtendedStatisticsAccess(new MetaDirStatisticsAccess(hdfsEnvironment, new JsonCodecFactory().jsonCodec(ExtendedStatistics.class)));
        deltaLakeMetastore = new HiveMetastoreBackedDeltaLakeMetastore(
                hiveMetastore,
                transactionLogAccess,
                typeManager,
                statistics);
    }

    private DeltaLakeTableHandle registerTable(String tableName)
    {
        return registerTable(tableName, tableName);
    }

    private DeltaLakeTableHandle registerTable(String tableName, String directoryName)
    {
        String tableLocation = Resources.getResource("statistics/" + directoryName).toExternalForm();

        Storage tableStorage = new Storage(
                StorageFormat.create("serde", "input", "output"), Optional.of(tableLocation), Optional.empty(), true, ImmutableMap.of(PATH_PROPERTY, tableLocation));

        hiveMetastore.createTable(
                new Table(
                        "db_name",
                        tableName,
                        Optional.of("test"),
                        "EXTERNAL_TABLE",
                        tableStorage,
                        ImmutableList.of(new Column("val", HiveType.HIVE_DOUBLE, Optional.empty())),
                        ImmutableList.of(),
                        ImmutableMap.of(TABLE_PROVIDER_PROPERTY, TABLE_PROVIDER_VALUE),
                        Optional.empty(),
                        Optional.empty(),
                        OptionalLong.empty()),
                PrincipalPrivileges.fromHivePrivilegeInfos(ImmutableSet.of()));

        return new DeltaLakeTableHandle(
                "db_name",
                tableName,
                "location",
                Optional.of(new MetadataEntry("id", "test", "description", null, "", ImmutableList.of(), ImmutableMap.of(), 0)),
                TupleDomain.all(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                0,
                false);
    }

    @Test
    public void testStatisticsNaN()
    {
        DeltaLakeTableHandle tableHandle = registerTable("nan");
        TableStatistics stats = deltaLakeMetastore.getTableStatistics(SESSION, tableHandle, Constraint.alwaysTrue());
        assertEquals(stats.getRowCount(), Estimate.of(1));
        assertEquals(stats.getColumnStatistics().size(), 1);

        ColumnStatistics columnStatistics = stats.getColumnStatistics().get(COLUMN_HANDLE);
        assertEquals(columnStatistics.getRange(), Optional.empty());
    }

    @Test
    public void testStatisticsInf()
    {
        DeltaLakeTableHandle tableHandle = registerTable("positive_infinity");
        TableStatistics stats = deltaLakeMetastore.getTableStatistics(SESSION, tableHandle, Constraint.alwaysTrue());
        ColumnStatistics columnStatistics = stats.getColumnStatistics().get(COLUMN_HANDLE);
        assertEquals(columnStatistics.getRange().get().getMin(), POSITIVE_INFINITY);
        assertEquals(columnStatistics.getRange().get().getMax(), POSITIVE_INFINITY);
    }

    @Test
    public void testStatisticsNegInf()
    {
        DeltaLakeTableHandle tableHandle = registerTable("negative_infinity");
        TableStatistics stats = deltaLakeMetastore.getTableStatistics(SESSION, tableHandle, Constraint.alwaysTrue());
        ColumnStatistics columnStatistics = stats.getColumnStatistics().get(COLUMN_HANDLE);
        assertEquals(columnStatistics.getRange().get().getMin(), NEGATIVE_INFINITY);
        assertEquals(columnStatistics.getRange().get().getMax(), NEGATIVE_INFINITY);
    }

    @Test
    public void testStatisticsNegZero()
    {
        DeltaLakeTableHandle tableHandle = registerTable("negative_zero");
        TableStatistics stats = deltaLakeMetastore.getTableStatistics(SESSION, tableHandle, Constraint.alwaysTrue());
        ColumnStatistics columnStatistics = stats.getColumnStatistics().get(COLUMN_HANDLE);
        assertEquals(columnStatistics.getRange().get().getMin(), -0.0d);
        assertEquals(columnStatistics.getRange().get().getMax(), -0.0d);
    }

    @Test
    public void testStatisticsInfinityAndNaN()
    {
        // Stats with NaN values cannot be used
        DeltaLakeTableHandle tableHandle = registerTable("infinity_nan");
        TableStatistics stats = deltaLakeMetastore.getTableStatistics(SESSION, tableHandle, Constraint.alwaysTrue());
        ColumnStatistics columnStatistics = stats.getColumnStatistics().get(COLUMN_HANDLE);
        assertEquals(columnStatistics.getRange().get().getMin(), POSITIVE_INFINITY);
        assertEquals(columnStatistics.getRange().get().getMax(), POSITIVE_INFINITY);
    }

    @Test
    public void testStatisticsNegativeInfinityAndNaN()
    {
        // Stats with NaN values cannot be used
        DeltaLakeTableHandle tableHandle = registerTable("negative_infinity_nan");
        TableStatistics stats = deltaLakeMetastore.getTableStatistics(SESSION, tableHandle, Constraint.alwaysTrue());
        ColumnStatistics columnStatistics = stats.getColumnStatistics().get(COLUMN_HANDLE);
        assertEquals(columnStatistics.getRange().get().getMin(), NEGATIVE_INFINITY);
        assertEquals(columnStatistics.getRange().get().getMax(), POSITIVE_INFINITY);
    }

    @Test
    public void testStatisticsZeroAndNaN()
    {
        // Stats with NaN values cannot be used
        DeltaLakeTableHandle tableHandle = registerTable("zero_nan");
        TableStatistics stats = deltaLakeMetastore.getTableStatistics(SESSION, tableHandle, Constraint.alwaysTrue());
        ColumnStatistics columnStatistics = stats.getColumnStatistics().get(COLUMN_HANDLE);
        assertEquals(columnStatistics.getRange().get().getMin(), 0.0);
        assertEquals(columnStatistics.getRange().get().getMax(), POSITIVE_INFINITY);
    }

    @Test
    public void testStatisticsZeroAndInfinity()
    {
        DeltaLakeTableHandle tableHandle = registerTable("zero_infinity");
        TableStatistics stats = deltaLakeMetastore.getTableStatistics(SESSION, tableHandle, Constraint.alwaysTrue());
        ColumnStatistics columnStatistics = stats.getColumnStatistics().get(COLUMN_HANDLE);
        assertEquals(columnStatistics.getRange().get().getMin(), 0.0);
        assertEquals(columnStatistics.getRange().get().getMax(), POSITIVE_INFINITY);
    }

    @Test
    public void testStatisticsZeroAndNegativeInfinity()
    {
        DeltaLakeTableHandle tableHandle = registerTable("zero_negative_infinity");
        TableStatistics stats = deltaLakeMetastore.getTableStatistics(SESSION, tableHandle, Constraint.alwaysTrue());
        ColumnStatistics columnStatistics = stats.getColumnStatistics().get(COLUMN_HANDLE);
        assertEquals(columnStatistics.getRange().get().getMin(), NEGATIVE_INFINITY);
        assertEquals(columnStatistics.getRange().get().getMax(), 0.0);
    }

    @Test
    public void testStatisticsNaNWithMultipleFiles()
    {
        // Stats with NaN values cannot be used. This transaction combines a file with NaN min/max values with one with 0.0 min/max values
        DeltaLakeTableHandle tableHandle = registerTable("nan_multi_file");
        TableStatistics stats = deltaLakeMetastore.getTableStatistics(SESSION, tableHandle, Constraint.alwaysTrue());
        ColumnStatistics columnStatistics = stats.getColumnStatistics().get(COLUMN_HANDLE);
        assertEquals(columnStatistics.getRange(), Optional.empty());
    }

    @Test
    public void testStatisticsMultipleFiles()
    {
        DeltaLakeTableHandle tableHandle = registerTable("basic_multi_file");
        TableStatistics stats = deltaLakeMetastore.getTableStatistics(SESSION, tableHandle, Constraint.alwaysTrue());
        ColumnStatistics columnStatistics = stats.getColumnStatistics().get(COLUMN_HANDLE);
        assertEquals(columnStatistics.getRange().get().getMin(), -42.0);
        assertEquals(columnStatistics.getRange().get().getMax(), 42.0);

        DeltaLakeTableHandle tableHandleWithUnenforcedConstraint = new DeltaLakeTableHandle(
                tableHandle.getSchemaName(),
                tableHandle.getTableName(),
                tableHandle.getLocation(),
                Optional.of(tableHandle.getMetadataEntry()),
                TupleDomain.all(),
                TupleDomain.withColumnDomains(ImmutableMap.of((DeltaLakeColumnHandle) COLUMN_HANDLE, Domain.singleValue(DOUBLE, 42.0))),
                tableHandle.getWriteType(),
                tableHandle.getProjectedColumns(),
                tableHandle.getUpdatedColumns(),
                tableHandle.getUpdateRowIdColumns(),
                tableHandle.getAnalyzeHandle(),
                0,
                false);
        stats = deltaLakeMetastore.getTableStatistics(SESSION, tableHandleWithUnenforcedConstraint, Constraint.alwaysTrue());
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
                tableHandle.getLocation(),
                Optional.of(tableHandle.getMetadataEntry()),
                TupleDomain.none(),
                TupleDomain.all(),
                tableHandle.getWriteType(),
                tableHandle.getProjectedColumns(),
                tableHandle.getUpdatedColumns(),
                tableHandle.getUpdateRowIdColumns(),
                tableHandle.getAnalyzeHandle(),
                0,
                false);
        DeltaLakeTableHandle tableHandleWithNoneUnenforcedConstraint = new DeltaLakeTableHandle(
                tableHandle.getSchemaName(),
                tableHandle.getTableName(),
                tableHandle.getLocation(),
                Optional.of(tableHandle.getMetadataEntry()),
                TupleDomain.all(),
                TupleDomain.none(),
                tableHandle.getWriteType(),
                tableHandle.getProjectedColumns(),
                tableHandle.getUpdatedColumns(),
                tableHandle.getUpdateRowIdColumns(),
                tableHandle.getAnalyzeHandle(),
                0,
                false);
        // If either the table handle's constraint or the provided Constraint are none, it will cause a 0 record count to be reported
        assertEmptyStats(deltaLakeMetastore.getTableStatistics(SESSION, tableHandleWithNoneEnforcedConstraint, Constraint.alwaysTrue()));
        assertEmptyStats(deltaLakeMetastore.getTableStatistics(SESSION, tableHandleWithNoneUnenforcedConstraint, Constraint.alwaysTrue()));
        assertEmptyStats(deltaLakeMetastore.getTableStatistics(SESSION, tableHandle, Constraint.alwaysFalse()));
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
        TableStatistics stats = deltaLakeMetastore.getTableStatistics(SESSION, tableHandle, Constraint.alwaysTrue());
        assertEquals(stats.getRowCount(), Estimate.of(9));

        Map<ColumnHandle, ColumnStatistics> statisticsMap = stats.getColumnStatistics();
        ColumnStatistics columnStats = statisticsMap.get(new DeltaLakeColumnHandle("dec_short", DecimalType.createDecimalType(5, 1), REGULAR));
        assertEquals(columnStats.getNullsFraction(), Estimate.zero());
        assertEquals(columnStats.getRange().get().getMin(), -10.1);
        assertEquals(columnStats.getRange().get().getMax(), 10.1);

        columnStats = statisticsMap.get(new DeltaLakeColumnHandle("dec_long", DecimalType.createDecimalType(25, 3), REGULAR));
        assertEquals(columnStats.getNullsFraction(), Estimate.zero());
        assertEquals(columnStats.getRange().get().getMin(), -999999999999.123);
        assertEquals(columnStats.getRange().get().getMax(), 999999999999.123);

        columnStats = statisticsMap.get(new DeltaLakeColumnHandle("l", BIGINT, REGULAR));
        assertEquals(columnStats.getNullsFraction(), Estimate.zero());
        assertEquals(columnStats.getRange().get().getMin(), -10000000.0);
        assertEquals(columnStats.getRange().get().getMax(), 10000000.0);

        columnStats = statisticsMap.get(new DeltaLakeColumnHandle("in", INTEGER, REGULAR));
        assertEquals(columnStats.getNullsFraction(), Estimate.zero());
        assertEquals(columnStats.getRange().get().getMin(), -20000000.0);
        assertEquals(columnStats.getRange().get().getMax(), 20000000.0);

        columnStats = statisticsMap.get(new DeltaLakeColumnHandle("sh", SMALLINT, REGULAR));
        assertEquals(columnStats.getNullsFraction(), Estimate.zero());
        assertEquals(columnStats.getRange().get().getMin(), -123.0);
        assertEquals(columnStats.getRange().get().getMax(), 123.0);

        columnStats = statisticsMap.get(new DeltaLakeColumnHandle("byt", TINYINT, REGULAR));
        assertEquals(columnStats.getNullsFraction(), Estimate.zero());
        assertEquals(columnStats.getRange().get().getMin(), -42.0);
        assertEquals(columnStats.getRange().get().getMax(), 42.0);

        columnStats = statisticsMap.get(new DeltaLakeColumnHandle("fl", REAL, REGULAR));
        assertEquals(columnStats.getNullsFraction(), Estimate.zero());
        assertEquals((float) columnStats.getRange().get().getMin(), -0.123f);
        assertEquals((float) columnStats.getRange().get().getMax(), 0.123f);

        columnStats = statisticsMap.get(new DeltaLakeColumnHandle("dou", DOUBLE, REGULAR));
        assertEquals(columnStats.getNullsFraction(), Estimate.zero());
        assertEquals(columnStats.getRange().get().getMin(), -0.321);
        assertEquals(columnStats.getRange().get().getMax(), 0.321);

        columnStats = statisticsMap.get(new DeltaLakeColumnHandle("dat", DATE, REGULAR));
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
        TableStatistics stats = deltaLakeMetastore.getTableStatistics(SESSION, tableHandle, Constraint.alwaysTrue());
        assertEquals(stats.getRowCount(), Estimate.of(9));

        Map<ColumnHandle, ColumnStatistics> statisticsMap = stats.getColumnStatistics();
        ColumnStatistics columnStats = statisticsMap.get(new DeltaLakeColumnHandle("fl", REAL, REGULAR));
        assertEquals(columnStats.getNullsFraction(), Estimate.zero());
        assertThat(columnStats.getRange()).isEmpty();

        columnStats = statisticsMap.get(new DeltaLakeColumnHandle("dou", DOUBLE, REGULAR));
        assertEquals(columnStats.getNullsFraction(), Estimate.zero());
        assertThat(columnStats.getRange()).isEmpty();
    }

    @Test
    public void testStatisticsParquetParsedStatisticsNullCount()
    {
        // The transaction log for this table was created so that the checkpoints only write struct statistics, not json statistics
        // The table has one INTEGER column 'i' where 3 of the 9 values are null
        DeltaLakeTableHandle tableHandle = registerTable("parquet_struct_statistics_null_count");
        TableStatistics stats = deltaLakeMetastore.getTableStatistics(SESSION, tableHandle, Constraint.alwaysTrue());
        assertEquals(stats.getRowCount(), Estimate.of(9));

        Map<ColumnHandle, ColumnStatistics> statisticsMap = stats.getColumnStatistics();
        ColumnStatistics columnStats = statisticsMap.get(new DeltaLakeColumnHandle("i", INTEGER, REGULAR));
        assertEquals(columnStats.getNullsFraction(), Estimate.of(3.0 / 9.0));
    }
}
