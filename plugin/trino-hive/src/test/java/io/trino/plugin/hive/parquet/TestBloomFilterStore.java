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
import io.trino.parquet.metadata.ColumnChunkMetadata;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.parquet.predicate.TupleDomainParquetPredicate;
import io.trino.parquet.reader.MetadataReader;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
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
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.joda.time.DateTimeZone;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.plugin.hive.HiveTestUtils.toNativeContainerValue;
import static io.trino.spi.predicate.Domain.multipleValues;
import static io.trino.spi.predicate.TupleDomain.withColumnDomains;
import static io.trino.spi.type.BigintType.BIGINT;
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
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaLongObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaShortObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaStringObjectInspector;
import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_1_0;
import static org.apache.parquet.hadoop.ParquetOutputFormat.BLOOM_FILTER_ENABLED;
import static org.apache.parquet.hadoop.ParquetOutputFormat.WRITER_VERSION;
import static org.apache.parquet.hadoop.metadata.ColumnPath.fromDotString;
import static org.apache.parquet.schema.LogicalTypeAnnotation.decimalType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.joda.time.DateTimeZone.UTC;

public class TestBloomFilterStore
{
    private static final String COLUMN_NAME = "test_column";
    private static final int DOMAIN_COMPACTION_THRESHOLD = 32;

    // the bloom filter is looked up by column name, but the physical type decides whether the filter
    // was hashed at the same width the predicate is looked up with, so it has to match what was written
    private static ColumnDescriptor columnDescriptor(PrimitiveTypeName typeName)
    {
        return new ColumnDescriptor(new String[] {COLUMN_NAME}, primitiveType(typeName), 0, 0);
    }

    private static PrimitiveType primitiveType(PrimitiveTypeName typeName)
    {
        return new PrimitiveType(REQUIRED, typeName, COLUMN_NAME);
    }

    static Stream<BloomFilterTypeTestCase> bloomFilterTypeTests()
    {
        return Stream.of(
                // varchar test case
                new BloomFilterTypeTestCase(
                        Arrays.asList("hello", "parquet", "bloom", "filter"),
                        Arrays.asList("NotExist", "fdsvit"),
                        createVarcharType(255),
                        BINARY,
                        javaStringObjectInspector),
                // integer test case, 32-bit signed two’s complement integer, between -2^31 and 2^31 - 1
                new BloomFilterTypeTestCase(
                        Arrays.asList(12321, 3344, 72334, 321, Integer.MAX_VALUE, Integer.MIN_VALUE),
                        Arrays.asList(89899, 897773),
                        INTEGER,
                        INT32,
                        javaIntObjectInspector),
                // double test case, 64-bit inexact
                new BloomFilterTypeTestCase(
                        Arrays.asList(892.22d, 341112.2222d, 43232.222121d, 99988.22d, Double.MAX_VALUE, Double.POSITIVE_INFINITY, Double.MIN_VALUE, Double.NEGATIVE_INFINITY),
                        Arrays.asList(321.44d, 776541.3214d, Double.MAX_VALUE / 2),
                        DOUBLE,
                        PrimitiveTypeName.DOUBLE,
                        javaDoubleObjectInspector),
                // real test case, 32-bit inexact
                new BloomFilterTypeTestCase(
                        Arrays.asList(32.22f, 341112.2222f, 43232.222121f, 32322.22f, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY, Float.MIN_VALUE, Float.MAX_VALUE),
                        Arrays.asList(321.44f, 321.3214f, Float.MIN_VALUE / 2),
                        REAL,
                        FLOAT,
                        javaFloatObjectInspector),
                // tinyint test case, 8 bits signed integer, between -2^7 and 2^7 - 1
                new BloomFilterTypeTestCase(
                        Arrays.asList((byte) 32, (byte) 67, Byte.MAX_VALUE, Byte.MAX_VALUE, (byte) 89),
                        Arrays.asList((byte) 0, (byte) 33, (byte) 75),
                        TINYINT,
                        INT32,
                        javaByteObjectInspector),
                // smallint test case, 16 bits signed integer, between -2^15 and 2^15 - 1
                new BloomFilterTypeTestCase(
                        Arrays.asList((short) 32, (short) 3000, Short.MIN_VALUE, Short.MAX_VALUE),
                        Arrays.asList((short) 0, (short) 33, (short) 43),
                        SMALLINT,
                        INT32,
                        javaShortObjectInspector),
                // date test case
                new BloomFilterTypeTestCase(
                        Arrays.asList(ofEpochDay(0), ofEpochDay(325), ofEpochDay(99875553), ofEpochDay(2456524)),
                        Arrays.asList(ofEpochDay(45), ofEpochDay(67439216)),
                        DATE,
                        INT32,
                        javaDateObjectInspector),
                // varbinary test case, variable length binary data.
                new BloomFilterTypeTestCase(
                        Arrays.asList("hello".getBytes(StandardCharsets.UTF_8), "parquet  ".getBytes(StandardCharsets.UTF_8), "bloom".getBytes(StandardCharsets.UTF_8), "filter".getBytes(StandardCharsets.UTF_8)),
                        Arrays.asList("not".getBytes(StandardCharsets.UTF_8), "exist".getBytes(StandardCharsets.UTF_8), "testcaseX".getBytes(StandardCharsets.UTF_8), "parquet".getBytes(StandardCharsets.UTF_8)),
                        VARBINARY,
                        BINARY,
                        javaByteArrayObjectInspector),
                // uuid test case, represents a UUID
                new BloomFilterTypeTestCase(
                        Arrays.asList(uuidToBytes(UUID.fromString("783176de-b6c5-4c5a-905d-0460ae103050")), uuidToBytes(UUID.fromString("b1a71c78-bd96-4117-a91a-18671530196a"))),
                        Arrays.asList(uuidToBytes(UUID.fromString("98a5f99c-7adb-4a92-ae10-6d2469d59423")), uuidToBytes(UUID.fromString("19fd9aed-7a93-4ada-8966-f89014f499ec"))),
                        UuidType.UUID,
                        BINARY,
                        javaByteArrayObjectInspector));
    }

    @ParameterizedTest
    @MethodSource("bloomFilterTypeTests")
    void testReadBloomFilter(BloomFilterTypeTestCase typeTestCase)
            throws Exception
    {
        try (ParquetTester.TempFile tempFile = new ParquetTester.TempFile("testbloomfilter", ".parquet")) {
            BloomFilterStore bloomFilterEnabled = generateBloomFilterStore(tempFile, true, typeTestCase.writeValues, typeTestCase.objectInspector);
            assertThat(bloomFilterEnabled.getBloomFilter(fromDotString(COLUMN_NAME))).isPresent();
            BloomFilter bloomFilter = bloomFilterEnabled.getBloomFilter(fromDotString(COLUMN_NAME)).get();

            PrimitiveType parquetType = primitiveType(typeTestCase.parquetType);
            for (Object data : typeTestCase.matchingValues) {
                assertThat(TupleDomainParquetPredicate.checkInBloomFilter(bloomFilter, data, typeTestCase.sqlType, parquetType)).isTrue();
            }
            for (Object data : typeTestCase.nonMatchingValues) {
                assertThat(TupleDomainParquetPredicate.checkInBloomFilter(bloomFilter, data, typeTestCase.sqlType, parquetType)).isFalse();
            }

            // the filter was hashed at the physical width the column was written with, so it cannot be looked up as
            // anything else and the row group is kept even for the values it would otherwise rule out
            PrimitiveType mismatchedParquetType = primitiveType(PrimitiveTypeName.INT96);
            for (Object data : typeTestCase.nonMatchingValues) {
                assertThat(TupleDomainParquetPredicate.checkInBloomFilter(bloomFilter, data, typeTestCase.sqlType, mismatchedParquetType)).isTrue();
            }
        }

        try (ParquetTester.TempFile tempFile = new ParquetTester.TempFile("testbloomfilter", ".parquet")) {
            BloomFilterStore bloomFilterNotEnabled = generateBloomFilterStore(tempFile, false, typeTestCase.writeValues, typeTestCase.objectInspector);
            assertThat(bloomFilterNotEnabled.getBloomFilter(fromDotString(COLUMN_NAME))).isEmpty();
        }
    }

    @ParameterizedTest
    @MethodSource("bloomFilterTypeTests")
    void testMatchesWithBloomFilter(BloomFilterTypeTestCase typeTestCase)
            throws Exception
    {
        try (ParquetTester.TempFile tempFile = new ParquetTester.TempFile("testbloomfilter", ".parquet")) {
            BloomFilterStore bloomFilterStore = generateBloomFilterStore(tempFile, true, typeTestCase.writeValues, typeTestCase.objectInspector);

            ColumnDescriptor columnDescriptor = columnDescriptor(typeTestCase.parquetType);
            TupleDomain<ColumnDescriptor> domain = withColumnDomains(singletonMap(columnDescriptor, multipleValues(typeTestCase.sqlType, typeTestCase.matchingValues)));
            TupleDomainParquetPredicate parquetPredicate = new TupleDomainParquetPredicate(domain, singletonList(columnDescriptor), UTC);
            // bloomfilter store has the column, and values match
            assertThat(parquetPredicate.matches(bloomFilterStore, DOMAIN_COMPACTION_THRESHOLD)).isTrue();

            TupleDomain<ColumnDescriptor> domainWithoutMatch = withColumnDomains(singletonMap(columnDescriptor, multipleValues(typeTestCase.sqlType, typeTestCase.nonMatchingValues)));
            TupleDomainParquetPredicate parquetPredicateWithoutMatch = new TupleDomainParquetPredicate(domainWithoutMatch, singletonList(columnDescriptor), UTC);
            // bloomfilter store has the column, but values not match
            assertThat(parquetPredicateWithoutMatch.matches(bloomFilterStore, DOMAIN_COMPACTION_THRESHOLD)).isFalse();

            ColumnDescriptor missingColumnDescriptor = new ColumnDescriptor(new String[] {"non_exist_path"}, Types.optional(typeTestCase.parquetType).named("Test column"), 0, 0);
            TupleDomain<ColumnDescriptor> domainForColumnWithoutBloomFilter = withColumnDomains(singletonMap(missingColumnDescriptor, multipleValues(typeTestCase.sqlType, typeTestCase.nonMatchingValues)));
            TupleDomainParquetPredicate predicateForColumnWithoutBloomFilter = new TupleDomainParquetPredicate(domainForColumnWithoutBloomFilter, singletonList(missingColumnDescriptor), UTC);
            // bloomfilter store does not have the column
            assertThat(predicateForColumnWithoutBloomFilter.matches(bloomFilterStore, DOMAIN_COMPACTION_THRESHOLD)).isTrue();
        }
    }

    @Test
    public void testBloomFilterIgnoredWhenIntColumnIsReadAsBigint()
            throws Exception
    {
        // A column promoted from integer to bigint keeps the bloom filter written for its int32 values.
        // parquet-mr hashed those as ints, so looking them up as longs finds nothing and the row group
        // would be eliminated even though it holds matching rows.
        try (ParquetTester.TempFile tempFile = new ParquetTester.TempFile("testbloomfilter", ".parquet")) {
            BloomFilterStore bloomFilterStore = generateBloomFilterStore(tempFile, true, Arrays.asList(62, 63, 64, 65), javaIntObjectInspector);
            ColumnDescriptor columnDescriptor = columnDescriptor(INT32);

            // looked up as the type it was written with, the filter is consulted and does eliminate the row group
            assertThat(matchesBloomFilter(bloomFilterStore, columnDescriptor, multipleValues(INTEGER, ImmutableList.of(1L, 2L)))).isFalse();

            assertThat(matchesBloomFilter(bloomFilterStore, columnDescriptor, multipleValues(BIGINT, ImmutableList.of(62L, 63L)))).isTrue();
        }
    }

    @Test
    public void testBloomFilterUsedForBigintColumn()
            throws Exception
    {
        // int64 is the width parquet-mr hashes a bigint at, and no other test writes one, so without this the guard
        // could be widened to reject INT64 and every bigint column would silently lose bloom filter pruning
        try (ParquetTester.TempFile tempFile = new ParquetTester.TempFile("testbloomfilter", ".parquet")) {
            BloomFilterStore bloomFilterStore = generateBloomFilterStore(tempFile, true, Arrays.asList(1L << 40, (1L << 40) + 1), javaLongObjectInspector);
            ColumnDescriptor columnDescriptor = columnDescriptor(INT64);

            assertThat(matchesBloomFilter(bloomFilterStore, columnDescriptor, multipleValues(BIGINT, ImmutableList.of(1L << 40)))).isTrue();
            assertThat(matchesBloomFilter(bloomFilterStore, columnDescriptor, multipleValues(BIGINT, ImmutableList.of(1L, 2L)))).isFalse();

            // the same filter cannot answer for a column read as the narrower type, which hashes four bytes
            assertThat(matchesBloomFilter(bloomFilterStore, columnDescriptor, multipleValues(INTEGER, ImmutableList.of(1L, 2L)))).isTrue();
        }
    }

    @Test
    public void testBloomFilterIgnoredWhenRealColumnIsReadAsDouble()
            throws Exception
    {
        try (ParquetTester.TempFile tempFile = new ParquetTester.TempFile("testbloomfilter", ".parquet")) {
            BloomFilterStore bloomFilterStore = generateBloomFilterStore(tempFile, true, Arrays.asList(1.5f, 2.5f), javaFloatObjectInspector);
            ColumnDescriptor columnDescriptor = columnDescriptor(FLOAT);

            // looked up as the type it was written with, the filter is consulted and does eliminate the row group
            assertThat(matchesBloomFilter(bloomFilterStore, columnDescriptor, multipleValues(REAL, ImmutableList.of(toNativeContainerValue(REAL, 9.5f))))).isFalse();

            assertThat(matchesBloomFilter(bloomFilterStore, columnDescriptor, multipleValues(DOUBLE, ImmutableList.of(1.5d)))).isTrue();
        }
    }

    @Test
    public void testBloomFilterLookupDependsOnPhysicalType()
            throws Exception
    {
        // A single filter written from string values is enough to pin the dispatch, because every lookup below uses a
        // value the filter does not hold: consulting the filter answers false, and declining to consult it answers true
        try (ParquetTester.TempFile tempFile = new ParquetTester.TempFile("testbloomfilter", ".parquet")) {
            BloomFilterStore bloomFilterStore = generateBloomFilterStore(tempFile, true, Arrays.asList("hello", "parquet", "bloom", "filter"), javaStringObjectInspector);
            BloomFilter bloomFilter = bloomFilterStore.getBloomFilter(fromDotString(COLUMN_NAME)).orElseThrow();
            Type varcharType = createVarcharType(255);
            Object absentValue = toNativeContainerValue(varcharType, "NotExist");

            // a Binary is hashed by its bytes alone, so a varbinary or a uuid can be looked up over either byte array
            // physical type, and a varchar over the one the reader renders as text
            assertThat(TupleDomainParquetPredicate.checkInBloomFilter(bloomFilter, absentValue, varcharType, primitiveType(BINARY))).isFalse();
            assertThat(TupleDomainParquetPredicate.checkInBloomFilter(bloomFilter, absentValue, VARBINARY, primitiveType(BINARY))).isFalse();
            assertThat(TupleDomainParquetPredicate.checkInBloomFilter(bloomFilter, absentValue, VARBINARY, primitiveType(FIXED_LEN_BYTE_ARRAY))).isFalse();
            assertThat(TupleDomainParquetPredicate.checkInBloomFilter(bloomFilter, absentValue, UuidType.UUID, primitiveType(FIXED_LEN_BYTE_ARRAY))).isFalse();

            // ColumnReaderFactory reads a fixed length byte array as a varchar only when that varchar is unbounded, so
            // a bounded one would have every row group pruned away and the read that should have failed never happens
            assertThat(TupleDomainParquetPredicate.checkInBloomFilter(bloomFilter, absentValue, varcharType, primitiveType(FIXED_LEN_BYTE_ARRAY))).isTrue();

            // a decimal column read as varchar is rendered as text rather than as the stored two's complement bytes.
            // Read as varbinary the same column does yield those bytes, so it is unaffected
            PrimitiveType decimalAnnotated = primitiveType(BINARY).withLogicalTypeAnnotation(decimalType(2, 10));
            assertThat(TupleDomainParquetPredicate.checkInBloomFilter(bloomFilter, absentValue, varcharType, decimalAnnotated)).isTrue();
            assertThat(TupleDomainParquetPredicate.checkInBloomFilter(bloomFilter, absentValue, VARBINARY, decimalAnnotated)).isFalse();
        }
    }

    private static boolean matchesBloomFilter(BloomFilterStore bloomFilterStore, ColumnDescriptor columnDescriptor, Domain domain)
    {
        TupleDomain<ColumnDescriptor> tupleDomain = withColumnDomains(singletonMap(columnDescriptor, domain));
        return new TupleDomainParquetPredicate(tupleDomain, singletonList(columnDescriptor), UTC)
                .matches(bloomFilterStore, DOMAIN_COMPACTION_THRESHOLD);
    }

    @Test
    public void testMatchesWithBloomFilterExpand()
            throws Exception
    {
        try (ParquetTester.TempFile tempFile = new ParquetTester.TempFile("testbloomfilter", ".parquet")) {
            BloomFilterStore bloomFilterStore = generateBloomFilterStore(tempFile, true, Arrays.asList(60, 61, 62, 63, 64, 65), javaIntObjectInspector);
            ColumnDescriptor columnDescriptor = columnDescriptor(INT32);

            // case 1, bloomfilter store has the column, and ranges expanded successfully and overlap
            TupleDomain<ColumnDescriptor> domain = TupleDomain.withColumnDomains(singletonMap(columnDescriptor, Domain.create(SortedRangeSet.copyOf(
                    INTEGER,
                    ImmutableList.of(Range.range(INTEGER, 60L, true, 68L, true))), false)));
            TupleDomainParquetPredicate parquetPredicate = new TupleDomainParquetPredicate(domain, singletonList(columnDescriptor), UTC);
            assertThat(parquetPredicate.matches(bloomFilterStore, DOMAIN_COMPACTION_THRESHOLD)).isTrue();

            // case 2, bloomfilter store does not have the column, but ranges exceeded DOMAIN_COMPACTION_THRESHOLD
            domain = TupleDomain.withColumnDomains(singletonMap(columnDescriptor, Domain.create(SortedRangeSet.copyOf(
                    INTEGER,
                    ImmutableList.of(Range.range(INTEGER, -68L, true, 0L, true))), false)));
            parquetPredicate = new TupleDomainParquetPredicate(domain, singletonList(columnDescriptor), UTC);
            assertThat(parquetPredicate.matches(bloomFilterStore, DOMAIN_COMPACTION_THRESHOLD)).isTrue();

            // case 3, bloomfilter store has the column, and ranges expanded successfully but does not overlap
            domain = TupleDomain.withColumnDomains(singletonMap(columnDescriptor, Domain.create(SortedRangeSet.copyOf(
                    INTEGER,
                    ImmutableList.of(Range.range(INTEGER, -68L, true, -60L, true))), false)));
            parquetPredicate = new TupleDomainParquetPredicate(domain, singletonList(columnDescriptor), UTC);
            assertThat(parquetPredicate.matches(bloomFilterStore, DOMAIN_COMPACTION_THRESHOLD)).isFalse();
        }
    }

    @Test
    public void testMatchesWithBloomFilterNullValues()
            throws Exception
    {
        // null values in parquet will only update column's repetition level and definition level, bloomfilter matching will be based on non-null values
        try (ParquetTester.TempFile tempFile = new ParquetTester.TempFile("testbloomfilter", ".parquet")) {
            BloomFilterStore bloomFilterStore = generateBloomFilterStore(tempFile, true, Arrays.asList(null, null, 62, 63, 64, 65), javaIntObjectInspector);
            ColumnDescriptor columnDescriptor = columnDescriptor(INT32);

            TupleDomain<ColumnDescriptor> domain = TupleDomain.withColumnDomains(singletonMap(columnDescriptor, Domain.create(SortedRangeSet.copyOf(
                    INTEGER,
                    ImmutableList.of(Range.range(INTEGER, 60L, true, 68L, true))), false)));
            TupleDomainParquetPredicate parquetPredicate = new TupleDomainParquetPredicate(domain, singletonList(columnDescriptor), UTC);
            // bloomfilter store has the column, and ranges overlap
            assertThat(parquetPredicate.matches(bloomFilterStore, DOMAIN_COMPACTION_THRESHOLD)).isTrue();

            TupleDomain<ColumnDescriptor> domainWithoutMatch = TupleDomain.withColumnDomains(singletonMap(columnDescriptor, Domain.create(SortedRangeSet.copyOf(
                    INTEGER,
                    ImmutableList.of(Range.range(INTEGER, -68L, true, -60L, true))), false)));
            // bloomfilter store has the column, but ranges not overlap
            TupleDomainParquetPredicate parquetPredicateWithoutMatch = new TupleDomainParquetPredicate(domainWithoutMatch, singletonList(columnDescriptor), UTC);
            assertThat(parquetPredicateWithoutMatch.matches(bloomFilterStore, DOMAIN_COMPACTION_THRESHOLD)).isFalse();
        }
    }

    @Test
    public void testMatchesWithBloomFilterNullPredicate()
            throws Exception
    {
        // if the predicate contains null values, bloomfilter matches will return true, since the bloom filter bitset contains only non-null values
        try (ParquetTester.TempFile tempFile = new ParquetTester.TempFile("testbloomfilter", ".parquet")) {
            BloomFilterStore bloomFilterStore = generateBloomFilterStore(tempFile, true, Arrays.asList(62, 63, 64, 65), javaIntObjectInspector);
            ColumnDescriptor columnDescriptor = columnDescriptor(INT32);

            TupleDomain<ColumnDescriptor> domainWithoutMatch = TupleDomain.withColumnDomains(singletonMap(columnDescriptor, Domain.create(SortedRangeSet.copyOf(
                    INTEGER,
                    ImmutableList.of(Range.range(INTEGER, -68L, true, -60L, true))), true)));
            TupleDomainParquetPredicate parquetPredicateWithoutMatch = new TupleDomainParquetPredicate(domainWithoutMatch, singletonList(columnDescriptor), UTC);
            assertThat(parquetPredicateWithoutMatch.matches(bloomFilterStore, DOMAIN_COMPACTION_THRESHOLD)).isTrue();
        }
    }

    private static BloomFilterStore generateBloomFilterStore(ParquetTester.TempFile tempFile, boolean enableBloomFilter, List<Object> testValues, ObjectInspector objectInspector)
            throws Exception
    {
        List<ObjectInspector> objectInspectors = singletonList(objectInspector);
        List<String> columnNames = ImmutableList.of(COLUMN_NAME);

        JobConf jobConf = new JobConf(false);
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
        TrinoParquetDataSource dataSource = new TrinoParquetDataSource(inputFile, ParquetReaderOptions.defaultOptions(), new FileFormatDataSourceStats());

        ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, Optional.empty());
        ColumnChunkMetadata columnChunkMetaData = getOnlyElement(getOnlyElement(parquetMetadata.getBlocks()).columns());

        return new BloomFilterStore(dataSource, getOnlyElement(parquetMetadata.getBlocks()), Set.of(columnChunkMetaData.getPath()), Optional.empty());
    }

    private static class BloomFilterTypeTestCase
    {
        private final List<Object> matchingValues;
        private final List<Object> nonMatchingValues;
        private final List<Object> writeValues;
        private final Type sqlType;
        private final PrimitiveTypeName parquetType;
        private final ObjectInspector objectInspector;

        private BloomFilterTypeTestCase(List<Object> writeValues, List<Object> nonMatchingValues, Type sqlType, PrimitiveTypeName parquetType, ObjectInspector objectInspector)
        {
            this.sqlType = requireNonNull(sqlType);
            this.parquetType = requireNonNull(parquetType);
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
