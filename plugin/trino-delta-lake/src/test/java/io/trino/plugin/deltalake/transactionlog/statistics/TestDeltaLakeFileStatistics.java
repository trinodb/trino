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
package io.trino.plugin.deltalake.transactionlog.statistics;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import io.airlift.json.ObjectMapperProvider;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.hdfs.HdfsFileSystemFactory;
import io.trino.plugin.deltalake.DeltaLakeColumnHandle;
import io.trino.plugin.deltalake.DeltaLakeConfig;
import io.trino.plugin.deltalake.transactionlog.DeltaLakeTransactionLogEntry;
import io.trino.plugin.deltalake.transactionlog.MetadataEntry;
import io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointEntryIterator;
import io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointSchemaManager;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.VarcharType;
import org.testng.annotations.Test;

import java.io.File;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.collect.Iterators.getOnlyElement;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.deltalake.DeltaLakeColumnType.REGULAR;
import static io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointEntryIterator.EntryType.METADATA;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.Decimals.encodeScaledValue;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.lang.Float.floatToIntBits;
import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

public class TestDeltaLakeFileStatistics
{
    private final ObjectMapper objectMapper = new ObjectMapperProvider().get();

    @Test
    public void testParseJsonStatistics()
            throws Exception
    {
        File statsFile = new File(getClass().getResource("all_type_statistics.json").toURI());
        DeltaLakeFileStatistics fileStatistics = objectMapper.readValue(statsFile, DeltaLakeJsonFileStatistics.class);
        testStatisticsValues(fileStatistics);
    }

    @Test
    public void testParseParquetStatistics()
            throws Exception
    {
        File statsFile = new File(getClass().getResource("/databricks/pruning/parquet_struct_statistics/_delta_log/00000000000000000010.checkpoint.parquet").toURI());

        TypeManager typeManager = TESTING_TYPE_MANAGER;
        CheckpointSchemaManager checkpointSchemaManager = new CheckpointSchemaManager(typeManager);

        TrinoFileSystem fileSystem = new HdfsFileSystemFactory(HDFS_ENVIRONMENT).create(SESSION);
        TrinoInputFile checkpointFile = fileSystem.newInputFile(statsFile.toURI().toString());

        CheckpointEntryIterator metadataEntryIterator = new CheckpointEntryIterator(
                checkpointFile,
                SESSION,
                checkpointFile.length(),
                checkpointSchemaManager,
                typeManager,
                ImmutableSet.of(METADATA),
                Optional.empty(),
                new FileFormatDataSourceStats(),
                new ParquetReaderConfig().toParquetReaderOptions(),
                true,
                new DeltaLakeConfig().getDomainCompactionThreshold());
        MetadataEntry metadataEntry = getOnlyElement(metadataEntryIterator).getMetaData();

        CheckpointEntryIterator checkpointEntryIterator = new CheckpointEntryIterator(
                checkpointFile,
                SESSION,
                checkpointFile.length(),
                checkpointSchemaManager,
                typeManager,
                ImmutableSet.of(CheckpointEntryIterator.EntryType.ADD),
                Optional.of(metadataEntry),
                new FileFormatDataSourceStats(),
                new ParquetReaderConfig().toParquetReaderOptions(),
                true,
                new DeltaLakeConfig().getDomainCompactionThreshold());
        DeltaLakeTransactionLogEntry matchingAddFileEntry = null;
        while (checkpointEntryIterator.hasNext()) {
            DeltaLakeTransactionLogEntry entry = checkpointEntryIterator.next();
            if (entry.getAdd() != null && entry.getAdd().getPath().contains("part-00000-17951bea-0d04-43c1-979c-ea1fac19b382-c000.snappy.parquet")) {
                assertNull(matchingAddFileEntry);
                matchingAddFileEntry = entry;
            }
        }
        assertNotNull(matchingAddFileEntry);
        assertThat(matchingAddFileEntry.getAdd().getStats()).isPresent();
        testStatisticsValues(matchingAddFileEntry.getAdd().getStats().get());
    }

    private static void testStatisticsValues(DeltaLakeFileStatistics fileStatistics)
    {
        assertEquals(fileStatistics.getNumRecords(), Optional.of(1L));
        assertEquals(
                fileStatistics.getMinColumnValue(new DeltaLakeColumnHandle("byt", TINYINT, OptionalInt.empty(), "byt", TINYINT, REGULAR)),
                Optional.of(42L));
        assertEquals(
                fileStatistics.getMinColumnValue(new DeltaLakeColumnHandle("dat", DATE, OptionalInt.empty(), "dat", DATE, REGULAR)),
                Optional.of(LocalDate.parse("5000-01-01").toEpochDay()));
        assertEquals(
                fileStatistics.getMinColumnValue(new DeltaLakeColumnHandle("dec_long", DecimalType.createDecimalType(25, 3), OptionalInt.empty(), "dec_long", DecimalType.createDecimalType(25, 3), REGULAR)),
                Optional.of(encodeScaledValue(new BigDecimal("999999999999.123"), 3)));
        assertEquals(
                fileStatistics.getMinColumnValue(new DeltaLakeColumnHandle("dec_short", DecimalType.createDecimalType(5, 1), OptionalInt.empty(), "dec_short", DecimalType.createDecimalType(5, 1), REGULAR)),
                Optional.of(new BigDecimal("10.1").unscaledValue().longValueExact()));
        assertEquals(
                fileStatistics.getMinColumnValue(new DeltaLakeColumnHandle("dou", DoubleType.DOUBLE, OptionalInt.empty(), "dou", DoubleType.DOUBLE, REGULAR)),
                Optional.of(0.321));
        assertEquals(
                fileStatistics.getMinColumnValue(new DeltaLakeColumnHandle("fl", REAL, OptionalInt.empty(), "fl", REAL, REGULAR)),
                Optional.of((long) floatToIntBits(0.123f)));
        assertEquals(
                fileStatistics.getMinColumnValue(new DeltaLakeColumnHandle("in", INTEGER, OptionalInt.empty(), "in", INTEGER, REGULAR)),
                Optional.of(20000000L));
        assertEquals(
                fileStatistics.getMinColumnValue(new DeltaLakeColumnHandle("l", BIGINT, OptionalInt.empty(), "l", BIGINT, REGULAR)),
                Optional.of(10000000L));
        Type rowType = RowType.rowType(RowType.field("s1", INTEGER), RowType.field("s3", VarcharType.createUnboundedVarcharType()));
        assertEquals(
                fileStatistics.getMinColumnValue(new DeltaLakeColumnHandle("row", rowType, OptionalInt.empty(), "row", rowType, REGULAR)),
                Optional.empty());
        assertEquals(
                fileStatistics.getMinColumnValue(new DeltaLakeColumnHandle("arr", new ArrayType(INTEGER), OptionalInt.empty(), "arr", new ArrayType(INTEGER), REGULAR)),
                Optional.empty());
        assertEquals(
                fileStatistics.getMinColumnValue(new DeltaLakeColumnHandle("m", new MapType(INTEGER, VarcharType.createUnboundedVarcharType(), new TypeOperators()), OptionalInt.empty(), "m", new MapType(INTEGER, VarcharType.createUnboundedVarcharType(), new TypeOperators()), REGULAR)),
                Optional.empty());
        assertEquals(
                fileStatistics.getMinColumnValue(new DeltaLakeColumnHandle("sh", SMALLINT, OptionalInt.empty(), "sh", SMALLINT, REGULAR)),
                Optional.of(123L));
        assertEquals(
                fileStatistics.getMinColumnValue(new DeltaLakeColumnHandle("str", VarcharType.createUnboundedVarcharType(), OptionalInt.empty(), "str", VarcharType.createUnboundedVarcharType(), REGULAR)),
                Optional.of(utf8Slice("a")));
        assertEquals(
                fileStatistics.getMinColumnValue(new DeltaLakeColumnHandle("ts", TIMESTAMP_TZ_MILLIS, OptionalInt.empty(), "ts", TIMESTAMP_TZ_MILLIS, REGULAR)),
                Optional.of(packDateTimeWithZone(LocalDateTime.parse("2960-10-31T01:00:00.000").toInstant(UTC).toEpochMilli(), UTC_KEY)));
        assertEquals(
                fileStatistics.getMinColumnValue(new DeltaLakeColumnHandle("bool", BOOLEAN, OptionalInt.empty(), "bool", BOOLEAN, REGULAR)),
                Optional.empty());
        assertEquals(
                fileStatistics.getMinColumnValue(new DeltaLakeColumnHandle("bin", VARBINARY, OptionalInt.empty(), "bin", VARBINARY, REGULAR)),
                Optional.empty());
    }
}
