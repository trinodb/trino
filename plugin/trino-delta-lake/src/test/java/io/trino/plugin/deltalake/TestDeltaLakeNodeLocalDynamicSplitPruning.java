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
import com.google.common.collect.ImmutableSet;
import io.airlift.testing.TempFile;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.filesystem.hdfs.HdfsFileSystemFactory;
import io.trino.filesystem.local.LocalInputFile;
import io.trino.filesystem.local.LocalOutputFile;
import io.trino.metadata.TableHandle;
import io.trino.parquet.writer.ParquetSchemaConverter;
import io.trino.parquet.writer.ParquetWriter;
import io.trino.parquet.writer.ParquetWriterOptions;
import io.trino.plugin.deltalake.transactionlog.MetadataEntry;
import io.trino.plugin.deltalake.transactionlog.ProtocolEntry;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.hive.HiveTransactionHandle;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.hive.parquet.ParquetWriterConfig;
import io.trino.spi.Page;
import io.trino.spi.SplitWeight;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.SqlDecimal;
import io.trino.testing.TestingConnectorSession;
import org.apache.parquet.format.CompressionCodec;
import org.joda.time.DateTimeZone;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static io.trino.plugin.deltalake.DeltaLakeColumnType.PARTITION_KEY;
import static io.trino.plugin.deltalake.DeltaLakeColumnType.REGULAR;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_FILE_SYSTEM_STATS;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.Decimals.writeShortDecimal;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.TestingHandles.TEST_CATALOG_HANDLE;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDeltaLakeNodeLocalDynamicSplitPruning
{
    private static final ParquetReaderConfig PARQUET_READER_CONFIG = new ParquetReaderConfig();
    private static final ParquetWriterConfig PARQUET_WRITER_CONFIG = new ParquetWriterConfig();

    @Test
    public void testDynamicSplitPruningOnUnpartitionedTable()
            throws IOException
    {
        String keyColumnName = "a_integer";
        DeltaLakeColumnHandle keyColumnHandle = new DeltaLakeColumnHandle(keyColumnName, INTEGER, OptionalInt.empty(), keyColumnName, INTEGER, REGULAR, Optional.empty());
        int keyColumnValue = 42;
        String dataColumnName = "a_varchar";
        String dataColumnValue = "hello world";
        DeltaLakeColumnHandle dataColumnHandle = new DeltaLakeColumnHandle(dataColumnName, VARCHAR, OptionalInt.empty(), dataColumnName, VARCHAR, REGULAR, Optional.empty());
        ParquetSchemaConverter schemaConverter = new ParquetSchemaConverter(
                ImmutableList.of(INTEGER, VARCHAR),
                ImmutableList.of(keyColumnName, dataColumnName),
                false,
                false);

        DeltaLakeConfig deltaLakeConfig = new DeltaLakeConfig();
        HiveTransactionHandle transaction = new HiveTransactionHandle(false);
        try (TempFile file = new TempFile()) {
            Files.delete(file.path());

            TrinoOutputFile outputFile = new LocalOutputFile(file.file());
            TrinoInputFile inputFile = new LocalInputFile(file.file());

            try (ParquetWriter writer = createParquetWriter(outputFile, schemaConverter)) {
                BlockBuilder keyBuilder = INTEGER.createBlockBuilder(null, 1);
                INTEGER.writeLong(keyBuilder, keyColumnValue);
                BlockBuilder dataBuilder = VARCHAR.createBlockBuilder(null, 1);
                VARCHAR.writeString(dataBuilder, dataColumnValue);
                writer.write(new Page(keyBuilder.build(), dataBuilder.build()));
            }

            DeltaLakeSplit split = new DeltaLakeSplit(
                    inputFile.location().toString(),
                    0,
                    inputFile.length(),
                    inputFile.length(),
                    Optional.empty(),
                    0,
                    Optional.empty(),
                    SplitWeight.standard(),
                    TupleDomain.all(),
                    ImmutableMap.of());

            MetadataEntry metadataEntry = new MetadataEntry(
                    "id",
                    "name",
                    "description",
                    new MetadataEntry.Format("provider", ImmutableMap.of()),
                    "{\"type\":\"struct\",\"fields\":[{\"name\":\"a_integer\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"a_varchar\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}",
                    ImmutableList.of(),
                    ImmutableMap.of(),
                    0);
            TableHandle tableHandle = new TableHandle(
                    TEST_CATALOG_HANDLE,
                    new DeltaLakeTableHandle(
                            "test_schema_name",
                            "unpartitioned_table",
                            true,
                            "test_location",
                            metadataEntry,
                            new ProtocolEntry(1, 2, Optional.empty(), Optional.empty()),
                            TupleDomain.all(),
                            TupleDomain.all(),
                            Optional.empty(),
                            Optional.of(Set.of(keyColumnHandle, dataColumnHandle)),
                            Optional.empty(),
                            Optional.empty(),
                            Optional.empty(),
                            0,
                            false),
                    transaction);

            TupleDomain<ColumnHandle> splitPruningPredicate = TupleDomain.withColumnDomains(
                    ImmutableMap.of(
                            keyColumnHandle,
                            Domain.singleValue(INTEGER, 1L)));
            try (ConnectorPageSource emptyPageSource = createTestingPageSource(transaction, deltaLakeConfig, split, tableHandle, ImmutableList.of(keyColumnHandle, dataColumnHandle), getDynamicFilter(splitPruningPredicate))) {
                assertThat(emptyPageSource.getNextPage()).isNull();
            }

            TupleDomain<ColumnHandle> nonSelectivePredicate = TupleDomain.withColumnDomains(
                    ImmutableMap.of(
                            keyColumnHandle,
                            Domain.singleValue(INTEGER, (long) keyColumnValue)));
            try (ConnectorPageSource nonEmptyPageSource = createTestingPageSource(transaction, deltaLakeConfig, split, tableHandle, ImmutableList.of(keyColumnHandle, dataColumnHandle), getDynamicFilter(nonSelectivePredicate))) {
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
        String dateColumnName = "date";
        DeltaLakeColumnHandle dateColumnHandle = new DeltaLakeColumnHandle(dateColumnName, DATE, OptionalInt.empty(), dateColumnName, DATE, PARTITION_KEY, Optional.empty());
        long dateColumnValue = LocalDate.of(2023, 1, 10).toEpochDay();
        String receiptColumnName = "receipt";
        DeltaLakeColumnHandle receiptColumnHandle = new DeltaLakeColumnHandle(receiptColumnName, VARCHAR, OptionalInt.empty(), receiptColumnName, VARCHAR, REGULAR, Optional.empty());
        String receiptColumnValue = "#12345";
        String amountColumnName = "amount";
        DecimalType amountColumnType = DecimalType.createDecimalType(10, 2);
        DeltaLakeColumnHandle amountColumnHandle = new DeltaLakeColumnHandle(amountColumnName, amountColumnType, OptionalInt.empty(), amountColumnName, amountColumnType, REGULAR, Optional.empty());
        BigDecimal amountColumnValue = new BigDecimal("1234567.65");
        ParquetSchemaConverter schemaConverter = new ParquetSchemaConverter(
                ImmutableList.of(VARCHAR, amountColumnType),
                ImmutableList.of(receiptColumnName, amountColumnName),
                false,
                false);

        DeltaLakeConfig icebergConfig = new DeltaLakeConfig();
        HiveTransactionHandle transaction = new HiveTransactionHandle(false);
        try (TempFile file = new TempFile()) {
            Files.delete(file.path());

            TrinoOutputFile outputFile = new LocalOutputFile(file.file());
            TrinoInputFile inputFile = new LocalInputFile(file.file());

            try (ParquetWriter writer = createParquetWriter(outputFile, schemaConverter)) {
                BlockBuilder receiptBuilder = VARCHAR.createBlockBuilder(null, 1);
                VARCHAR.writeString(receiptBuilder, receiptColumnValue);
                BlockBuilder amountBuilder = amountColumnType.createBlockBuilder(null, 1);
                writeShortDecimal(amountBuilder, amountColumnValue.unscaledValue().longValueExact());
                writer.write(new Page(receiptBuilder.build(), amountBuilder.build()));
            }

            DeltaLakeSplit split = new DeltaLakeSplit(
                    inputFile.location().toString(),
                    0,
                    inputFile.length(),
                    inputFile.length(),
                    Optional.empty(),
                    0,
                    Optional.empty(),
                    SplitWeight.standard(),
                    TupleDomain.all(),
                    ImmutableMap.of(dateColumnName, Optional.of("2023-01-10")));

            MetadataEntry metadataEntry = new MetadataEntry(
                    "id",
                    "name",
                    "description",
                    new MetadataEntry.Format("provider", ImmutableMap.of()),
                    "{\"type\":\"struct\",\"fields\":[{\"name\":\"date\",\"type\":\"date\",\"nullable\":true,\"metadata\":{}},{\"name\":\"receipt\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"amount\",\"type\":\"decimal(10,2)\",\"nullable\":true,\"metadata\":{}}]}",
                    ImmutableList.of(dateColumnName),
                    ImmutableMap.of(),
                    0);

            TableHandle tableHandle = new TableHandle(
                    TEST_CATALOG_HANDLE,
                    new DeltaLakeTableHandle(
                            "test_schema_name",
                            "unpartitioned_table",
                            true,
                            "test_location",
                            metadataEntry,
                            new ProtocolEntry(1, 2, Optional.empty(), Optional.empty()),
                            TupleDomain.all(),
                            TupleDomain.all(),
                            Optional.empty(),
                            Optional.of(Set.of(dateColumnHandle, receiptColumnHandle, amountColumnHandle)),
                            Optional.empty(),
                            Optional.empty(),
                            Optional.empty(),
                            0,
                            false),
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

    private static ParquetWriter createParquetWriter(TrinoOutputFile outputFile, ParquetSchemaConverter schemaConverter)
            throws IOException
    {
        return new ParquetWriter(
                outputFile.create(),
                schemaConverter.getMessageType(),
                schemaConverter.getPrimitiveTypes(),
                ParquetWriterOptions.builder().build(),
                CompressionCodec.SNAPPY,
                "test",
                Optional.of(DateTimeZone.UTC),
                Optional.empty());
    }

    private static ConnectorPageSource createTestingPageSource(
            HiveTransactionHandle transaction,
            DeltaLakeConfig deltaLakeConfig,
            DeltaLakeSplit split,
            TableHandle tableHandle,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter)
    {
        FileFormatDataSourceStats stats = new FileFormatDataSourceStats();
        DeltaLakePageSourceProvider provider = new DeltaLakePageSourceProvider(
                new HdfsFileSystemFactory(HDFS_ENVIRONMENT, HDFS_FILE_SYSTEM_STATS),
                stats,
                PARQUET_READER_CONFIG,
                deltaLakeConfig,
                TESTING_TYPE_MANAGER);

        return provider.createPageSource(
                transaction,
                getSession(deltaLakeConfig),
                split,
                tableHandle.connectorHandle(),
                columns,
                dynamicFilter);
    }

    private static TestingConnectorSession getSession(DeltaLakeConfig deltaLakeConfig)
    {
        return TestingConnectorSession.builder()
                .setPropertyMetadata(new DeltaLakeSessionProperties(deltaLakeConfig, PARQUET_READER_CONFIG, PARQUET_WRITER_CONFIG).getSessionProperties())
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
