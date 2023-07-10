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
package io.trino.plugin.hive.parquet;

import com.google.common.collect.ImmutableList;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.local.LocalInputFile;
import io.trino.parquet.BloomFilterStore;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.predicate.TupleDomainParquetPredicate;
import io.trino.parquet.reader.MetadataReader;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.SortedRangeSet;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import io.trino.spi.type.UuidType;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.mapred.JobConf;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.values.bloomfilter.BloomFilter;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.joda.time.DateTimeZone;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.hadoop.ConfigurationInstantiator.newEmptyConfiguration;
import static io.trino.plugin.hive.HiveTestUtils.toNativeContainerValue;
import static io.trino.spi.predicate.Domain.multipleValues;
import static io.trino.spi.predicate.TupleDomain.withColumnDomains;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.hive.common.type.Date.ofEpochDay;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardStructObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaByteObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaDateObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaFloatObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaIntObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaShortObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaStringObjectInspector;
import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_1_0;
import static org.apache.parquet.hadoop.ParquetOutputFormat.BLOOM_FILTER_ENABLED;
import static org.apache.parquet.hadoop.ParquetOutputFormat.WRITER_VERSION;
import static org.apache.parquet.hadoop.metadata.ColumnPath.fromDotString;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.joda.time.DateTimeZone.UTC;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestBloomFilterStore
{
    private static final String COLUMN_NAME = "test_column";
    private static final int DOMAIN_COMPACTION_THRESHOLD = 32;

    // here PrimitiveType#getPrimitiveTypeName is dummy, since predicate matches is via column name
    ColumnDescriptor columnDescriptor = new ColumnDescriptor(new String[] {COLUMN_NAME}, new PrimitiveType(REQUIRED, BINARY, COLUMN_NAME), 0, 0);

    @DataProvider
    public Object[][] bloomFilterTypeTests()
    {
        return new Object[][] {
                {
                        // varchar test case
                        new BloomFilterTypeTestCase(
                                Arrays.asList("hello", "parquet", "bloom", "filter"),
                                Arrays.asList("NotExist", "fdsvit"),
                                createVarcharType(255),
                                javaStringObjectInspector)
                },
                {
                        // integer test case, 32-bit signed two’s complement integer, between -2^31 and 2^31 - 1
                        new BloomFilterTypeTestCase(
                                Arrays.asList(12321, 3344, 72334, 321, Integer.MAX_VALUE, Integer.MIN_VALUE),
                                Arrays.asList(89899, 897773),
                                INTEGER,
                                javaIntObjectInspector)
                },
                {
                        // double test case, 64-bit inexact
                        new BloomFilterTypeTestCase(
                                Arrays.asList(892.22d, 341112.2222d, 43232.222121d, 99988.22d, Double.MAX_VALUE, Double.POSITIVE_INFINITY, Double.MIN_VALUE, Double.NEGATIVE_INFINITY),
                                Arrays.asList(321.44d, 776541.3214d, Double.MAX_VALUE / 2),
                                DOUBLE,
                                javaDoubleObjectInspector)
                },
                {
                        // real test case, 32-bit inexact
                        new BloomFilterTypeTestCase(
                                Arrays.asList(32.22f, 341112.2222f, 43232.222121f, 32322.22f, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY, Float.MIN_VALUE, Float.MAX_VALUE),
                                Arrays.asList(321.44f, 321.3214f, Float.MIN_VALUE / 2),
                                REAL,
                                javaFloatObjectInspector)
                },
                {
                        // tinyint test case, 8 bits signed integer, between -2^7 and 2^7 - 1
                        new BloomFilterTypeTestCase(
                                Arrays.asList((byte) 32, (byte) 67, Byte.MAX_VALUE, Byte.MAX_VALUE, (byte) 89),
                                Arrays.asList((byte) 0, (byte) 33, (byte) 75),
                                TINYINT,
                                javaByteObjectInspector)
                },
                {
                        // smallint test case, 16 bits signed integer, between -2^15 and 2^15 - 1
                        new BloomFilterTypeTestCase(
                                Arrays.asList((short) 32, (short) 3000, Short.MIN_VALUE, Short.MAX_VALUE),
                                Arrays.asList((short) 0, (short) 33, (short) 43),
                                SMALLINT,
                                javaShortObjectInspector)
                },
                {
                        // date test case
                        new BloomFilterTypeTestCase(
                                Arrays.asList(ofEpochDay(0), ofEpochDay(325), ofEpochDay(99875553), ofEpochDay(2456524)),
                                Arrays.asList(ofEpochDay(45), ofEpochDay(67439216)),
                                DATE,
                                javaDateObjectInspector)
                },
                {
                        // varbinary test case, variable length binary data.
                        new BloomFilterTypeTestCase(
                                Arrays.asList("hello".getBytes(StandardCharsets.UTF_8), "parquet  ".getBytes(StandardCharsets.UTF_8), "bloom".getBytes(StandardCharsets.UTF_8), "filter".getBytes(StandardCharsets.UTF_8)),
                                Arrays.asList("not".getBytes(StandardCharsets.UTF_8), "exist".getBytes(StandardCharsets.UTF_8), "testcaseX".getBytes(StandardCharsets.UTF_8), "parquet".getBytes(StandardCharsets.UTF_8)),
                                VARBINARY,
                                javaByteArrayObjectInspector)
                },
                {
                        // uuid test case, represents a UUID
                        new BloomFilterTypeTestCase(
                                Arrays.asList(uuidToBytes(UUID.fromString("783176de-b6c5-4c5a-905d-0460ae103050")), uuidToBytes(UUID.fromString("b1a71c78-bd96-4117-a91a-18671530196a"))),
                                Arrays.asList(uuidToBytes(UUID.fromString("98a5f99c-7adb-4a92-ae10-6d2469d59423")), uuidToBytes(UUID.fromString("19fd9aed-7a93-4ada-8966-f89014f499ec"))),
                                UuidType.UUID,
                                javaByteArrayObjectInspector)
                }
        };
    }

    @Test(dataProvider = "bloomFilterTypeTests")
    public void testReadBloomFilter(BloomFilterTypeTestCase typeTestCase)
            throws Exception
    {
        try (ParquetTester.TempFile tempFile = new ParquetTester.TempFile("testbloomfilter", ".parquet")) {
            BloomFilterStore bloomFilterEnabled = generateBloomFilterStore(tempFile, true, typeTestCase.writeValues, typeTestCase.objectInspector);
            assertTrue(bloomFilterEnabled.getBloomFilter(fromDotString(COLUMN_NAME)).isPresent());
            BloomFilter bloomFilter = bloomFilterEnabled.getBloomFilter(fromDotString(COLUMN_NAME)).get();

            for (Object data : typeTestCase.matchingValues) {
                assertTrue(TupleDomainParquetPredicate.checkInBloomFilter(bloomFilter, data, typeTestCase.sqlType));
            }
            for (Object data : typeTestCase.nonMatchingValues) {
                assertFalse(TupleDomainParquetPredicate.checkInBloomFilter(bloomFilter, data, typeTestCase.sqlType));
            }
        }

        try (ParquetTester.TempFile tempFile = new ParquetTester.TempFile("testbloomfilter", ".parquet")) {
            BloomFilterStore bloomFilterNotEnabled = generateBloomFilterStore(tempFile, false, typeTestCase.writeValues, typeTestCase.objectInspector);
            assertTrue(bloomFilterNotEnabled.getBloomFilter(fromDotString(COLUMN_NAME)).isEmpty());
        }
    }

    @Test(dataProvider = "bloomFilterTypeTests")
    public void testMatchesWithBloomFilter(BloomFilterTypeTestCase typeTestCase)
            throws Exception
    {
        try (ParquetTester.TempFile tempFile = new ParquetTester.TempFile("testbloomfilter", ".parquet")) {
            BloomFilterStore bloomFilterStore = generateBloomFilterStore(tempFile, true, typeTestCase.writeValues, typeTestCase.objectInspector);

            TupleDomain<ColumnDescriptor> domain = withColumnDomains(singletonMap(columnDescriptor, multipleValues(typeTestCase.sqlType, typeTestCase.matchingValues)));
            TupleDomainParquetPredicate parquetPredicate = new TupleDomainParquetPredicate(domain, singletonList(columnDescriptor), UTC);
            // bloomfilter store has the column, and values match
            assertTrue(parquetPredicate.matches(bloomFilterStore, DOMAIN_COMPACTION_THRESHOLD));

            TupleDomain<ColumnDescriptor> domainWithoutMatch = withColumnDomains(singletonMap(columnDescriptor, multipleValues(typeTestCase.sqlType, typeTestCase.nonMatchingValues)));
            TupleDomainParquetPredicate parquetPredicateWithoutMatch = new TupleDomainParquetPredicate(domainWithoutMatch, singletonList(columnDescriptor), UTC);
            // bloomfilter store has the column, but values not match
            assertFalse(parquetPredicateWithoutMatch.matches(bloomFilterStore, DOMAIN_COMPACTION_THRESHOLD));

            ColumnDescriptor columnDescriptor = new ColumnDescriptor(new String[] {"non_exist_path"}, Types.optional(BINARY).named("Test column"), 0, 0);
            TupleDomain<ColumnDescriptor> domainForColumnWithoutBloomFilter = withColumnDomains(singletonMap(columnDescriptor, multipleValues(typeTestCase.sqlType, typeTestCase.nonMatchingValues)));
            TupleDomainParquetPredicate predicateForColumnWithoutBloomFilter = new TupleDomainParquetPredicate(domainForColumnWithoutBloomFilter, singletonList(columnDescriptor), UTC);
            // bloomfilter store does not have the column
            assertTrue(predicateForColumnWithoutBloomFilter.matches(bloomFilterStore, DOMAIN_COMPACTION_THRESHOLD));
        }
    }

    @Test
    public void testMatchesWithBloomFilterExpand()
            throws Exception
    {
        try (ParquetTester.TempFile tempFile = new ParquetTester.TempFile("testbloomfilter", ".parquet")) {
            BloomFilterStore bloomFilterStore = generateBloomFilterStore(tempFile, true, Arrays.asList(60, 61, 62, 63, 64, 65), javaIntObjectInspector);

            // case 1, bloomfilter store has the column, and ranges expanded successfully and overlap
            TupleDomain<ColumnDescriptor> domain = TupleDomain.withColumnDomains(singletonMap(columnDescriptor, Domain.create(SortedRangeSet.copyOf(INTEGER,
                    ImmutableList.of(Range.range(INTEGER, 60L, true, 68L, true))), false)));
            TupleDomainParquetPredicate parquetPredicate = new TupleDomainParquetPredicate(domain, singletonList(columnDescriptor), UTC);
            assertTrue(parquetPredicate.matches(bloomFilterStore, DOMAIN_COMPACTION_THRESHOLD));

            // case 2, bloomfilter store does not have the column, but ranges exceeded DOMAIN_COMPACTION_THRESHOLD
            domain = TupleDomain.withColumnDomains(singletonMap(columnDescriptor, Domain.create(SortedRangeSet.copyOf(INTEGER,
                    ImmutableList.of(Range.range(INTEGER, -68L, true, 0L, true))), false)));
            parquetPredicate = new TupleDomainParquetPredicate(domain, singletonList(columnDescriptor), UTC);
            assertTrue(parquetPredicate.matches(bloomFilterStore, DOMAIN_COMPACTION_THRESHOLD));

            // case 3, bloomfilter store has the column, and ranges expanded successfully but does not overlap
            domain = TupleDomain.withColumnDomains(singletonMap(columnDescriptor, Domain.create(SortedRangeSet.copyOf(INTEGER,
                    ImmutableList.of(Range.range(INTEGER, -68L, true, -60L, true))), false)));
            parquetPredicate = new TupleDomainParquetPredicate(domain, singletonList(columnDescriptor), UTC);
            assertFalse(parquetPredicate.matches(bloomFilterStore, DOMAIN_COMPACTION_THRESHOLD));
        }
    }

    @Test
    public void testMatchesWithBloomFilterNullValues()
            throws Exception
    {
        // null values in parquet will only update column's repetition level and definition level, bloomfilter matching will be based on non-null values
        try (ParquetTester.TempFile tempFile = new ParquetTester.TempFile("testbloomfilter", ".parquet")) {
            BloomFilterStore bloomFilterStore = generateBloomFilterStore(tempFile, true, Arrays.asList(null, null, 62, 63, 64, 65), javaIntObjectInspector);

            TupleDomain<ColumnDescriptor> domain = TupleDomain.withColumnDomains(singletonMap(columnDescriptor, Domain.create(SortedRangeSet.copyOf(INTEGER,
                    ImmutableList.of(Range.range(INTEGER, 60L, true, 68L, true))), false)));
            TupleDomainParquetPredicate parquetPredicate = new TupleDomainParquetPredicate(domain, singletonList(columnDescriptor), UTC);
            // bloomfilter store has the column, and ranges overlap
            assertTrue(parquetPredicate.matches(bloomFilterStore, DOMAIN_COMPACTION_THRESHOLD));

            TupleDomain<ColumnDescriptor> domainWithoutMatch = TupleDomain.withColumnDomains(singletonMap(columnDescriptor, Domain.create(SortedRangeSet.copyOf(INTEGER,
                    ImmutableList.of(Range.range(INTEGER, -68L, true, -60L, true))), false)));
            // bloomfilter store has the column, but ranges not overlap
            TupleDomainParquetPredicate parquetPredicateWithoutMatch = new TupleDomainParquetPredicate(domainWithoutMatch, singletonList(columnDescriptor), UTC);
            assertFalse(parquetPredicateWithoutMatch.matches(bloomFilterStore, DOMAIN_COMPACTION_THRESHOLD));
        }
    }

    @Test
    public void testMatchesWithBloomFilterNullPredicate()
            throws Exception
    {
        // if the predicate contains null values, bloomfilter matches will return true, since the bloom filter bitset contains only non-null values
        try (ParquetTester.TempFile tempFile = new ParquetTester.TempFile("testbloomfilter", ".parquet")) {
            BloomFilterStore bloomFilterStore = generateBloomFilterStore(tempFile, true, Arrays.asList(62, 63, 64, 65), javaIntObjectInspector);

            TupleDomain<ColumnDescriptor> domainWithoutMatch = TupleDomain.withColumnDomains(singletonMap(columnDescriptor, Domain.create(SortedRangeSet.copyOf(INTEGER,
                    ImmutableList.of(Range.range(INTEGER, -68L, true, -60L, true))), true)));
            TupleDomainParquetPredicate parquetPredicateWithoutMatch = new TupleDomainParquetPredicate(domainWithoutMatch, singletonList(columnDescriptor), UTC);
            assertTrue(parquetPredicateWithoutMatch.matches(bloomFilterStore, DOMAIN_COMPACTION_THRESHOLD));
        }
    }

    private static BloomFilterStore generateBloomFilterStore(ParquetTester.TempFile tempFile, boolean enableBloomFilter, List<Object> testValues, ObjectInspector objectInspector)
            throws Exception
    {
        List<ObjectInspector> objectInspectors = singletonList(objectInspector);
        List<String> columnNames = ImmutableList.of(COLUMN_NAME);

        JobConf jobConf = new JobConf(newEmptyConfiguration());
        jobConf.setEnum(WRITER_VERSION, PARQUET_1_0);
        jobConf.setBoolean(BLOOM_FILTER_ENABLED, enableBloomFilter);

        ParquetTester.writeParquetColumn(
                jobConf,
                tempFile.getFile(),
                CompressionCodec.SNAPPY,
                ParquetTester.createTableProperties(columnNames, objectInspectors),
                getStandardStructObjectInspector(columnNames, objectInspectors),
                new Iterator<?>[] {testValues.iterator()},
                Optional.empty(),
                false,
                DateTimeZone.getDefault());

        TrinoInputFile inputFile = new LocalInputFile(tempFile.getFile());
        TrinoParquetDataSource dataSource = new TrinoParquetDataSource(inputFile, new ParquetReaderOptions(), new FileFormatDataSourceStats());

        ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, Optional.empty());
        ColumnChunkMetaData columnChunkMetaData = getOnlyElement(getOnlyElement(parquetMetadata.getBlocks()).getColumns());

        return new BloomFilterStore(dataSource, getOnlyElement(parquetMetadata.getBlocks()), Set.of(columnChunkMetaData.getPath()));
    }

    private static class BloomFilterTypeTestCase
    {
        private final List<Object> matchingValues;
        private final List<Object> nonMatchingValues;
        private final List<Object> writeValues;
        private final Type sqlType;
        private final ObjectInspector objectInspector;

        private BloomFilterTypeTestCase(List<Object> writeValues, List<Object> nonMatchingValues, Type sqlType, ObjectInspector objectInspector)
        {
            this.sqlType = requireNonNull(sqlType);
            this.objectInspector = requireNonNull(objectInspector);
            this.writeValues = requireNonNull(writeValues);

            this.matchingValues = writeValues.stream()
                    .map(data -> toNativeContainerValue(sqlType, data))
                    .collect(toImmutableList());
            this.nonMatchingValues = nonMatchingValues.stream()
                    .map(data -> toNativeContainerValue(sqlType, data))
                    .collect(toImmutableList());
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("writeValues", writeValues)
                    .add("sqlType", sqlType)
                    .toString();
        }
    }

    private static byte[] uuidToBytes(UUID uuid)
    {
        return ByteBuffer.allocate(16)
                .putLong(uuid.getMostSignificantBits())
                .putLong(uuid.getLeastSignificantBits())
                .array();
    }
}
