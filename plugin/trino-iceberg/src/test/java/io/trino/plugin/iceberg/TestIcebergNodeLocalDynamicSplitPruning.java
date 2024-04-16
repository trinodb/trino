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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.testing.TempFile;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.filesystem.hdfs.HdfsFileSystemFactory;
import io.trino.filesystem.local.LocalInputFile;
import io.trino.filesystem.local.LocalOutputFile;
import io.trino.metadata.TableHandle;
import io.trino.orc.OrcWriteValidation;
import io.trino.orc.OrcWriter;
import io.trino.orc.OrcWriterOptions;
import io.trino.orc.OrcWriterStats;
import io.trino.orc.OutputStreamOrcDataSink;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.hive.HiveTransactionHandle;
import io.trino.plugin.hive.orc.OrcReaderConfig;
import io.trino.plugin.hive.orc.OrcWriterConfig;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.hive.parquet.ParquetWriterConfig;
import io.trino.plugin.iceberg.catalog.rest.DefaultIcebergFileSystemFactory;
import io.trino.spi.Page;
import io.trino.spi.SplitWeight;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.SqlDecimal;
import io.trino.spi.type.Type;
import io.trino.testing.TestingConnectorSession;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static io.trino.orc.metadata.CompressionKind.NONE;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_FILE_SYSTEM_STATS;
import static io.trino.plugin.iceberg.ColumnIdentity.TypeCategory.PRIMITIVE;
import static io.trino.plugin.iceberg.IcebergFileFormat.ORC;
import static io.trino.plugin.iceberg.util.OrcTypeConverter.toOrcType;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.Decimals.writeShortDecimal;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.TestingHandles.TEST_CATALOG_HANDLE;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.assertj.core.api.Assertions.assertThat;

public class TestIcebergNodeLocalDynamicSplitPruning
{
    private static final OrcReaderConfig ORC_READER_CONFIG = new OrcReaderConfig();
    private static final OrcWriterConfig ORC_WRITER_CONFIG = new OrcWriterConfig();
    private static final ParquetReaderConfig PARQUET_READER_CONFIG = new ParquetReaderConfig();
    private static final ParquetWriterConfig PARQUET_WRITER_CONFIG = new ParquetWriterConfig();

    @Test
    public void testDynamicSplitPruningOnUnpartitionedTable()
            throws IOException
    {
        String tableName = "unpartitioned_table";
        String keyColumnName = "a_integer";
        ColumnIdentity keyColumnIdentity = new ColumnIdentity(1, keyColumnName, PRIMITIVE, ImmutableList.of());
        IcebergColumnHandle keyColumnHandle = new IcebergColumnHandle(keyColumnIdentity, INTEGER, ImmutableList.of(), INTEGER, true, Optional.empty());
        int keyColumnValue = 42;
        String dataColumnName = "a_varchar";
        ColumnIdentity dataColumnIdentity = new ColumnIdentity(2, dataColumnName, PRIMITIVE, ImmutableList.of());
        IcebergColumnHandle dataColumnHandle = new IcebergColumnHandle(dataColumnIdentity, VARCHAR, ImmutableList.of(), VARCHAR, true, Optional.empty());
        String dataColumnValue = "hello world";
        Schema tableSchema = new Schema(
                optional(keyColumnIdentity.getId(), keyColumnName, Types.IntegerType.get()),
                optional(dataColumnIdentity.getId(), dataColumnName, Types.StringType.get()));

        IcebergConfig icebergConfig = new IcebergConfig();
        HiveTransactionHandle transaction = new HiveTransactionHandle(false);
        try (TempFile file = new TempFile()) {
            Files.delete(file.path());

            TrinoOutputFile outputFile = new LocalOutputFile(file.file());
            TrinoInputFile inputFile = new LocalInputFile(file.file());
            List<String> columnNames = ImmutableList.of(keyColumnName, dataColumnName);
            List<Type> types = ImmutableList.of(INTEGER, VARCHAR);

            try (OrcWriter writer = new OrcWriter(
                    OutputStreamOrcDataSink.create(outputFile),
                    columnNames,
                    types,
                    toOrcType(tableSchema),
                    NONE,
                    new OrcWriterOptions(),
                    ImmutableMap.of(),
                    true,
                    OrcWriteValidation.OrcWriteValidationMode.BOTH,
                    new OrcWriterStats())) {
                BlockBuilder keyBuilder = INTEGER.createBlockBuilder(null, 1);
                INTEGER.writeLong(keyBuilder, keyColumnValue);
                BlockBuilder dataBuilder = VARCHAR.createBlockBuilder(null, 1);
                VARCHAR.writeString(dataBuilder, dataColumnValue);
                writer.write(new Page(keyBuilder.build(), dataBuilder.build()));
            }

            // Pruning due to IcebergTableHandle#unenforcedPredicate
            IcebergSplit split = new IcebergSplit(
                    inputFile.toString(),
                    0,
                    inputFile.length(),
                    inputFile.length(),
                    -1, // invalid; normally known
                    ORC,
                    PartitionSpecParser.toJson(PartitionSpec.unpartitioned()),
                    PartitionData.toJson(new PartitionData(new Object[] {})),
                    ImmutableList.of(),
                    SplitWeight.standard(),
                    TupleDomain.all(),
                    ImmutableMap.of());

            String tablePath = inputFile.location().fileName();
            TableHandle tableHandle = new TableHandle(
                    TEST_CATALOG_HANDLE,
                    new IcebergTableHandle(
                            CatalogHandle.fromId("iceberg:NORMAL:v12345"),
                            "test_schema",
                            tableName,
                            TableType.DATA,
                            Optional.empty(),
                            SchemaParser.toJson(tableSchema),
                            Optional.of(PartitionSpecParser.toJson(PartitionSpec.unpartitioned())),
                            2,
                            TupleDomain.withColumnDomains(ImmutableMap.of(keyColumnHandle, Domain.singleValue(INTEGER, (long) keyColumnValue))),
                            TupleDomain.all(),
                            OptionalLong.empty(),
                            ImmutableSet.of(keyColumnHandle),
                            Optional.empty(),
                            tablePath,
                            ImmutableMap.of(),
                            false,
                            Optional.empty(),
                            ImmutableSet.of(),
                            Optional.of(false)),
                    transaction);

            TupleDomain<ColumnHandle> splitPruningPredicate = TupleDomain.withColumnDomains(
                    ImmutableMap.of(
                            keyColumnHandle,
                            Domain.singleValue(INTEGER, 1L)));
            try (ConnectorPageSource emptyPageSource = createTestingPageSource(transaction, icebergConfig, split, tableHandle, ImmutableList.of(keyColumnHandle, dataColumnHandle), getDynamicFilter(splitPruningPredicate))) {
                assertThat(emptyPageSource.getNextPage()).isNull();
            }

            TupleDomain<ColumnHandle> nonSelectivePredicate = TupleDomain.withColumnDomains(
                    ImmutableMap.of(
                            keyColumnHandle,
                            Domain.singleValue(INTEGER, (long) keyColumnValue)));
            try (ConnectorPageSource nonEmptyPageSource = createTestingPageSource(transaction, icebergConfig, split, tableHandle, ImmutableList.of(keyColumnHandle, dataColumnHandle), getDynamicFilter(nonSelectivePredicate))) {
                Page page = nonEmptyPageSource.getNextPage();
                assertThat(page).isNotNull();
                assertThat(page.getPositionCount()).isEqualTo(1);
                assertThat(INTEGER.getInt(page.getBlock(0), 0)).isEqualTo(keyColumnValue);
                assertThat(VARCHAR.getSlice(page.getBlock(1), 0).toStringUtf8()).isEqualTo(dataColumnValue);
            }

            // Pruning due to IcebergSplit#fileStatisticsDomain
            split = new IcebergSplit(
                    inputFile.toString(),
                    0,
                    inputFile.length(),
                    inputFile.length(),
                    -1, // invalid; normally known
                    ORC,
                    PartitionSpecParser.toJson(PartitionSpec.unpartitioned()),
                    PartitionData.toJson(new PartitionData(new Object[] {})),
                    ImmutableList.of(),
                    SplitWeight.standard(),
                    TupleDomain.withColumnDomains(ImmutableMap.of(keyColumnHandle, Domain.singleValue(INTEGER, (long) keyColumnValue))),
                    ImmutableMap.of());

            tableHandle = new TableHandle(
                    TEST_CATALOG_HANDLE,
                    new IcebergTableHandle(
                            CatalogHandle.fromId("iceberg:NORMAL:v12345"),
                            "test_schema",
                            tableName,
                            TableType.DATA,
                            Optional.empty(),
                            SchemaParser.toJson(tableSchema),
                            Optional.of(PartitionSpecParser.toJson(PartitionSpec.unpartitioned())),
                            2,
                            TupleDomain.all(),
                            TupleDomain.all(),
                            OptionalLong.empty(),
                            ImmutableSet.of(keyColumnHandle),
                            Optional.empty(),
                            tablePath,
                            ImmutableMap.of(),
                            false,
                            Optional.empty(),
                            ImmutableSet.of(),
                            Optional.of(false)),
                    transaction);

            try (ConnectorPageSource emptyPageSource = createTestingPageSource(transaction, icebergConfig, split, tableHandle, ImmutableList.of(keyColumnHandle, dataColumnHandle), getDynamicFilter(splitPruningPredicate))) {
                assertThat(emptyPageSource.getNextPage()).isNull();
            }

            try (ConnectorPageSource nonEmptyPageSource = createTestingPageSource(transaction, icebergConfig, split, tableHandle, ImmutableList.of(keyColumnHandle, dataColumnHandle), getDynamicFilter(nonSelectivePredicate))) {
                Page page = nonEmptyPageSource.getNextPage();
                assertThat(page).isNotNull();
                assertThat(page.getPositionCount()).isEqualTo(1);
                assertThat(INTEGER.getInt(page.getBlock(0), 0)).isEqualTo(keyColumnValue);
                assertThat(VARCHAR.getSlice(page.getBlock(1), 0).toStringUtf8()).isEqualTo(dataColumnValue);
            }
        }
    }

    @Test
    public void testDynamicSplitPruningWithExplicitPartitionFilter()
            throws IOException
    {
        String tableName = "sales_table";
        String dateColumnName = "date";
        ColumnIdentity dateColumnIdentity = new ColumnIdentity(1, dateColumnName, PRIMITIVE, ImmutableList.of());
        IcebergColumnHandle dateColumnHandle = new IcebergColumnHandle(dateColumnIdentity, DATE, ImmutableList.of(), DATE, true, Optional.empty());
        long dateColumnValue = LocalDate.of(2023, 1, 10).toEpochDay();
        String receiptColumnName = "receipt";
        ColumnIdentity receiptColumnIdentity = new ColumnIdentity(2, receiptColumnName, PRIMITIVE, ImmutableList.of());
        IcebergColumnHandle receiptColumnHandle = new IcebergColumnHandle(receiptColumnIdentity, VARCHAR, ImmutableList.of(), VARCHAR, true, Optional.empty());
        String receiptColumnValue = "#12345";
        String amountColumnName = "amount";
        ColumnIdentity amountColumnIdentity = new ColumnIdentity(3, amountColumnName, PRIMITIVE, ImmutableList.of());
        DecimalType amountColumnType = DecimalType.createDecimalType(10, 2);
        IcebergColumnHandle amountColumnHandle = new IcebergColumnHandle(amountColumnIdentity, amountColumnType, ImmutableList.of(), amountColumnType, true, Optional.empty());
        BigDecimal amountColumnValue = new BigDecimal("1234567.65");
        Schema tableSchema = new Schema(
                optional(dateColumnIdentity.getId(), dateColumnName, Types.DateType.get()),
                optional(receiptColumnIdentity.getId(), receiptColumnName, Types.StringType.get()),
                optional(amountColumnIdentity.getId(), amountColumnName, Types.DecimalType.of(10, 2)));
        PartitionSpec partitionSpec = PartitionSpec.builderFor(tableSchema)
                .identity(dateColumnName)
                .build();

        IcebergConfig icebergConfig = new IcebergConfig();
        HiveTransactionHandle transaction = new HiveTransactionHandle(false);
        try (TempFile file = new TempFile()) {
            Files.delete(file.path());

            TrinoOutputFile outputFile = new LocalOutputFile(file.file());
            TrinoInputFile inputFile = new LocalInputFile(file.file());
            List<String> columnNames = ImmutableList.of(dateColumnName, receiptColumnName, amountColumnName);
            List<Type> types = ImmutableList.of(DATE, VARCHAR, amountColumnType);

            try (OrcWriter writer = new OrcWriter(
                    OutputStreamOrcDataSink.create(outputFile),
                    columnNames,
                    types,
                    toOrcType(tableSchema),
                    NONE,
                    new OrcWriterOptions(),
                    ImmutableMap.of(),
                    true,
                    OrcWriteValidation.OrcWriteValidationMode.BOTH,
                    new OrcWriterStats())) {
                BlockBuilder dateBuilder = DATE.createBlockBuilder(null, 1);
                DATE.writeLong(dateBuilder, dateColumnValue);
                BlockBuilder receiptBuilder = VARCHAR.createBlockBuilder(null, 1);
                VARCHAR.writeString(receiptBuilder, receiptColumnValue);
                BlockBuilder amountBuilder = amountColumnType.createBlockBuilder(null, 1);
                writeShortDecimal(amountBuilder, amountColumnValue.unscaledValue().longValueExact());
                writer.write(new Page(dateBuilder.build(), receiptBuilder.build(), amountBuilder.build()));
            }

            IcebergSplit split = new IcebergSplit(
                    inputFile.toString(),
                    0,
                    inputFile.length(),
                    inputFile.length(),
                    -1, // invalid; normally known
                    ORC,
                    PartitionSpecParser.toJson(partitionSpec),
                    PartitionData.toJson(new PartitionData(new Object[] {dateColumnValue})),
                    ImmutableList.of(),
                    SplitWeight.standard(),
                    TupleDomain.all(),
                    ImmutableMap.of());

            String tablePath = inputFile.location().fileName();
            TableHandle tableHandle = new TableHandle(
                    TEST_CATALOG_HANDLE,
                    new IcebergTableHandle(
                            CatalogHandle.fromId("iceberg:NORMAL:v12345"),
                            "test_schema",
                            tableName,
                            TableType.DATA,
                            Optional.empty(),
                            SchemaParser.toJson(tableSchema),
                            Optional.of(PartitionSpecParser.toJson(partitionSpec)),
                            2,
                            TupleDomain.all(),
                            TupleDomain.all(),
                            OptionalLong.empty(),
                            ImmutableSet.of(dateColumnHandle),
                            Optional.empty(),
                            tablePath,
                            ImmutableMap.of(),
                            false,
                            Optional.empty(),
                            ImmutableSet.of(),
                            Optional.of(false)),
                    transaction);

            // Simulate situations where the dynamic filter (e.g.: while performing a JOIN with another table) reduces considerably
            // the amount of data to be processed from the current table

            TupleDomain<ColumnHandle> differentDatePredicate = TupleDomain.withColumnDomains(
                    ImmutableMap.of(
                            dateColumnHandle,
                            Domain.singleValue(DATE, LocalDate.of(2023, 2, 2).toEpochDay())));
            TupleDomain<ColumnHandle> nonOverlappingDatePredicate = TupleDomain.withColumnDomains(
                    ImmutableMap.of(
                            dateColumnHandle,
                            Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(DATE, LocalDate.of(2023, 2, 2).toEpochDay())), true)));
            for (TupleDomain<ColumnHandle> partitionPredicate : List.of(differentDatePredicate, nonOverlappingDatePredicate)) {
                try (ConnectorPageSource emptyPageSource = createTestingPageSource(
                        transaction,
                        icebergConfig,
                        split,
                        tableHandle,
                        ImmutableList.of(dateColumnHandle, receiptColumnHandle, amountColumnHandle),
                        getDynamicFilter(partitionPredicate))) {
                    assertThat(emptyPageSource.getNextPage()).isNull();
                }
            }

            TupleDomain<ColumnHandle> sameDatePredicate = TupleDomain.withColumnDomains(
                    ImmutableMap.of(
                            dateColumnHandle,
                            Domain.singleValue(DATE, dateColumnValue)));
            TupleDomain<ColumnHandle> overlappingDatePredicate = TupleDomain.withColumnDomains(
                    ImmutableMap.of(
                            dateColumnHandle,
                            Domain.create(ValueSet.ofRanges(Range.range(DATE, LocalDate.of(2023, 1, 1).toEpochDay(), true, LocalDate.of(2023, 2, 1).toEpochDay(), false)), true)));
            for (TupleDomain<ColumnHandle> partitionPredicate : List.of(sameDatePredicate, overlappingDatePredicate)) {
                try (ConnectorPageSource nonEmptyPageSource = createTestingPageSource(
                        transaction,
                        icebergConfig,
                        split,
                        tableHandle,
                        ImmutableList.of(dateColumnHandle, receiptColumnHandle, amountColumnHandle),
                        getDynamicFilter(partitionPredicate))) {
                    Page page = nonEmptyPageSource.getNextPage();
                    assertThat(page).isNotNull();
                    assertThat(page.getPositionCount()).isEqualTo(1);
                    assertThat(INTEGER.getInt(page.getBlock(0), 0)).isEqualTo(dateColumnValue);
                    assertThat(VARCHAR.getSlice(page.getBlock(1), 0).toStringUtf8()).isEqualTo(receiptColumnValue);
                    assertThat(((SqlDecimal) amountColumnType.getObjectValue(null, page.getBlock(2), 0)).toBigDecimal()).isEqualTo(amountColumnValue);
                }
            }
        }
    }

    @Test
    public void testDynamicSplitPruningWithExplicitPartitionFilterPartitionEvolution()
            throws IOException
    {
        String tableName = "sales_table";
        String yearColumnName = "year";
        ColumnIdentity yearColumnIdentity = new ColumnIdentity(1, yearColumnName, PRIMITIVE, ImmutableList.of());
        IcebergColumnHandle yearColumnHandle = new IcebergColumnHandle(yearColumnIdentity, INTEGER, ImmutableList.of(), INTEGER, true, Optional.empty());
        long yearColumnValue = 2023L;
        String monthColumnName = "month";
        ColumnIdentity monthColumnIdentity = new ColumnIdentity(2, monthColumnName, PRIMITIVE, ImmutableList.of());
        IcebergColumnHandle monthColumnHandle = new IcebergColumnHandle(monthColumnIdentity, INTEGER, ImmutableList.of(), INTEGER, true, Optional.empty());
        long monthColumnValue = 1L;
        String receiptColumnName = "receipt";
        ColumnIdentity receiptColumnIdentity = new ColumnIdentity(3, receiptColumnName, PRIMITIVE, ImmutableList.of());
        IcebergColumnHandle receiptColumnHandle = new IcebergColumnHandle(receiptColumnIdentity, VARCHAR, ImmutableList.of(), VARCHAR, true, Optional.empty());
        String receiptColumnValue = "#12345";
        String amountColumnName = "amount";
        ColumnIdentity amountColumnIdentity = new ColumnIdentity(4, amountColumnName, PRIMITIVE, ImmutableList.of());
        DecimalType amountColumnType = DecimalType.createDecimalType(10, 2);
        IcebergColumnHandle amountColumnHandle = new IcebergColumnHandle(amountColumnIdentity, amountColumnType, ImmutableList.of(), amountColumnType, true, Optional.empty());
        BigDecimal amountColumnValue = new BigDecimal("1234567.65");
        Schema tableSchema = new Schema(
                optional(yearColumnIdentity.getId(), yearColumnName, Types.IntegerType.get()),
                optional(monthColumnIdentity.getId(), monthColumnName, Types.IntegerType.get()),
                optional(receiptColumnIdentity.getId(), receiptColumnName, Types.StringType.get()),
                optional(amountColumnIdentity.getId(), amountColumnName, Types.DecimalType.of(10, 2)));
        PartitionSpec partitionSpec = PartitionSpec.builderFor(tableSchema)
                .identity(yearColumnName)
                .build();
        IcebergConfig icebergConfig = new IcebergConfig();
        HiveTransactionHandle transaction = new HiveTransactionHandle(false);
        try (TempFile file = new TempFile()) {
            Files.delete(file.path());

            TrinoOutputFile outputFile = new LocalOutputFile(file.file());
            TrinoInputFile inputFile = new LocalInputFile(file.file());
            List<String> columnNames = ImmutableList.of(yearColumnName, monthColumnName, receiptColumnName, amountColumnName);
            List<Type> types = ImmutableList.of(INTEGER, INTEGER, VARCHAR, amountColumnType);

            try (OrcWriter writer = new OrcWriter(
                    OutputStreamOrcDataSink.create(outputFile),
                    columnNames,
                    types,
                    toOrcType(tableSchema),
                    NONE,
                    new OrcWriterOptions(),
                    ImmutableMap.of(),
                    true,
                    OrcWriteValidation.OrcWriteValidationMode.BOTH,
                    new OrcWriterStats())) {
                BlockBuilder yearBuilder = INTEGER.createBlockBuilder(null, 1);
                INTEGER.writeLong(yearBuilder, yearColumnValue);
                BlockBuilder monthBuilder = INTEGER.createBlockBuilder(null, 1);
                INTEGER.writeLong(monthBuilder, monthColumnValue);
                BlockBuilder receiptBuilder = VARCHAR.createBlockBuilder(null, 1);
                VARCHAR.writeString(receiptBuilder, receiptColumnValue);
                BlockBuilder amountBuilder = amountColumnType.createBlockBuilder(null, 1);
                writeShortDecimal(amountBuilder, amountColumnValue.unscaledValue().longValueExact());
                writer.write(new Page(yearBuilder.build(), monthBuilder.build(), receiptBuilder.build(), amountBuilder.build()));
            }

            IcebergSplit split = new IcebergSplit(
                    inputFile.toString(),
                    0,
                    inputFile.length(),
                    inputFile.length(),
                    -1, // invalid; normally known
                    ORC,
                    PartitionSpecParser.toJson(partitionSpec),
                    PartitionData.toJson(new PartitionData(new Object[] {yearColumnValue})),
                    ImmutableList.of(),
                    SplitWeight.standard(),
                    TupleDomain.all(),
                    ImmutableMap.of());

            String tablePath = inputFile.location().fileName();
            // Simulate the situation where `month` column is added at a later phase as partitioning column
            // in addition to the `year` column, which leads to use it as unenforced predicate in the table handle
            // after applying the filter
            TableHandle tableHandle = new TableHandle(
                    TEST_CATALOG_HANDLE,
                    new IcebergTableHandle(
                            CatalogHandle.fromId("iceberg:NORMAL:v12345"),
                            "test_schema",
                            tableName,
                            TableType.DATA,
                            Optional.empty(),
                            SchemaParser.toJson(tableSchema),
                            Optional.of(PartitionSpecParser.toJson(partitionSpec)),
                            2,
                            TupleDomain.withColumnDomains(
                                    ImmutableMap.of(
                                            yearColumnHandle,
                                            Domain.create(ValueSet.ofRanges(Range.range(INTEGER, 2023L, true, 2024L, true)), true))),
                            TupleDomain.withColumnDomains(
                                    ImmutableMap.of(
                                            monthColumnHandle,
                                            Domain.create(ValueSet.ofRanges(Range.range(INTEGER, 1L, true, 12L, true)), true))),
                            OptionalLong.empty(),
                            ImmutableSet.of(yearColumnHandle, monthColumnHandle, receiptColumnHandle, amountColumnHandle),
                            Optional.empty(),
                            tablePath,
                            ImmutableMap.of(),
                            false,
                            Optional.empty(),
                            ImmutableSet.of(),
                            Optional.of(false)),
                    transaction);

            // Simulate situations where the dynamic filter (e.g.: while performing a JOIN with another table) reduces considerably
            // the amount of data to be processed from the current table
            TupleDomain<ColumnHandle> differentYearPredicate = TupleDomain.withColumnDomains(
                    ImmutableMap.of(
                            yearColumnHandle,
                            Domain.singleValue(INTEGER, 2024L)));
            TupleDomain<ColumnHandle> sameYearAndDifferentMonthPredicate = TupleDomain.withColumnDomains(
                    ImmutableMap.of(
                            yearColumnHandle,
                            Domain.singleValue(INTEGER, 2023L),
                            monthColumnHandle,
                            Domain.singleValue(INTEGER, 2L)));
            for (TupleDomain<ColumnHandle> partitionPredicate : List.of(differentYearPredicate, sameYearAndDifferentMonthPredicate)) {
                try (ConnectorPageSource emptyPageSource = createTestingPageSource(
                        transaction,
                        icebergConfig,
                        split,
                        tableHandle,
                        ImmutableList.of(yearColumnHandle, monthColumnHandle, receiptColumnHandle, amountColumnHandle),
                        getDynamicFilter(partitionPredicate))) {
                    assertThat(emptyPageSource.getNextPage()).isNull();
                }
            }

            TupleDomain<ColumnHandle> sameYearPredicate = TupleDomain.withColumnDomains(
                    ImmutableMap.of(
                            yearColumnHandle,
                            Domain.singleValue(INTEGER, 2023L)));
            TupleDomain<ColumnHandle> sameYearAndMonthPredicate = TupleDomain.withColumnDomains(
                    ImmutableMap.of(
                            yearColumnHandle,
                            Domain.singleValue(INTEGER, 2023L),
                            monthColumnHandle,
                            Domain.singleValue(INTEGER, 1L)));
            for (TupleDomain<ColumnHandle> partitionPredicate : List.of(sameYearPredicate, sameYearAndMonthPredicate)) {
                try (ConnectorPageSource nonEmptyPageSource = createTestingPageSource(
                        transaction,
                        icebergConfig,
                        split,
                        tableHandle,
                        ImmutableList.of(yearColumnHandle, monthColumnHandle, receiptColumnHandle, amountColumnHandle),
                        getDynamicFilter(partitionPredicate))) {
                    Page page = nonEmptyPageSource.getNextPage();
                    assertThat(page).isNotNull();
                    assertThat(page.getPositionCount()).isEqualTo(1);
                    assertThat(INTEGER.getInt(page.getBlock(0), 0)).isEqualTo(2023L);
                    assertThat(INTEGER.getInt(page.getBlock(1), 0)).isEqualTo(1L);
                    assertThat(VARCHAR.getSlice(page.getBlock(2), 0).toStringUtf8()).isEqualTo(receiptColumnValue);
                    assertThat(((SqlDecimal) amountColumnType.getObjectValue(null, page.getBlock(3), 0)).toBigDecimal()).isEqualTo(amountColumnValue);
                }
            }
        }
    }

    private static ConnectorPageSource createTestingPageSource(
            HiveTransactionHandle transaction,
            IcebergConfig icebergConfig,
            IcebergSplit split,
            TableHandle tableHandle,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter)
    {
        FileFormatDataSourceStats stats = new FileFormatDataSourceStats();
        IcebergPageSourceProvider provider = new IcebergPageSourceProvider(
                new DefaultIcebergFileSystemFactory(new HdfsFileSystemFactory(HDFS_ENVIRONMENT, HDFS_FILE_SYSTEM_STATS)),
                stats,
                ORC_READER_CONFIG,
                PARQUET_READER_CONFIG,
                TESTING_TYPE_MANAGER);

        return provider.createPageSource(
                transaction,
                getSession(icebergConfig),
                split,
                tableHandle.connectorHandle(),
                columns,
                dynamicFilter);
    }

    private static TestingConnectorSession getSession(IcebergConfig icebergConfig)
    {
        return TestingConnectorSession.builder()
                .setPropertyMetadata(new IcebergSessionProperties(icebergConfig, ORC_READER_CONFIG, ORC_WRITER_CONFIG, PARQUET_READER_CONFIG, PARQUET_WRITER_CONFIG).getSessionProperties())
                .build();
    }

    private static DynamicFilter getDynamicFilter(TupleDomain<ColumnHandle> tupleDomain)
    {
        return new DynamicFilter()
        {
            @Override
            public Set<ColumnHandle> getColumnsCovered()
            {
                return tupleDomain.getDomains().map(Map::keySet)
                        .orElseGet(ImmutableSet::of);
            }

            @Override
            public CompletableFuture<?> isBlocked()
            {
                return completedFuture(null);
            }

            @Override
            public boolean isComplete()
            {
                return true;
            }

            @Override
            public boolean isAwaitable()
            {
                return false;
            }

            @Override
            public TupleDomain<ColumnHandle> getCurrentPredicate()
            {
                return tupleDomain;
            }
        };
    }
}
