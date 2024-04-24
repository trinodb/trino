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
package io.trino.orc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Longs;
import io.airlift.slice.Slice;
import io.trino.hive.orc.util.Murmur3;
import io.trino.orc.OrcWriterOptions.WriterIdentification;
import io.trino.orc.metadata.ColumnMetadata;
import io.trino.orc.metadata.CompressedMetadataWriter;
import io.trino.orc.metadata.CompressionKind;
import io.trino.orc.metadata.OrcMetadataReader;
import io.trino.orc.metadata.OrcMetadataWriter;
import io.trino.orc.metadata.statistics.BloomFilter;
import io.trino.orc.metadata.statistics.ColumnStatistics;
import io.trino.orc.metadata.statistics.IntegerStatistics;
import io.trino.orc.metadata.statistics.Utf8BloomFilterBuilder;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.RealType;
import io.trino.spi.type.Type;
import org.apache.orc.OrcProto;
import org.apache.orc.protobuf.CodedInputStream;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.orc.TupleDomainOrcPredicate.checkInBloomFilter;
import static io.trino.orc.metadata.OrcColumnId.ROOT_COLUMN;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Float.floatToIntBits;
import static java.lang.Float.intBitsToFloat;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;

public class TestOrcBloomFilters
{
    private static final byte[] TEST_STRING = "ORC_STRING".getBytes(UTF_8);
    private static final byte[] TEST_STRING_NOT_WRITTEN = "ORC_STRING_not".getBytes(UTF_8);
    private static final int TEST_INTEGER = 12345;

    private static final Map<Object, Type> TEST_VALUES = ImmutableMap.<Object, Type>builder()
            .put(wrappedBuffer(TEST_STRING), VARCHAR)
            .put(wrappedBuffer(new byte[] {12, 34, 56}), VARBINARY)
            .put(4312L, BIGINT)
            .put(123, INTEGER)
            .put(789, SMALLINT)
            .put(77, TINYINT)
            .put(901, DATE)
            .put(987654L, TIMESTAMP_MILLIS)
            .put(234.567, DOUBLE)
            .put((long) floatToIntBits(987.654f), REAL)
            .buildOrThrow();

    @Test
    public void testHiveBloomFilterSerde()
    {
        BloomFilter bloomFilter = new BloomFilter(1_000_000L, 0.05);

        // String
        bloomFilter.add(TEST_STRING);
        assertThat(bloomFilter.test(TEST_STRING)).isTrue();
        assertThat(bloomFilter.testSlice(wrappedBuffer(TEST_STRING))).isTrue();
        assertThat(bloomFilter.test(TEST_STRING_NOT_WRITTEN)).isFalse();
        assertThat(bloomFilter.testSlice(wrappedBuffer(TEST_STRING_NOT_WRITTEN))).isFalse();

        // Integer
        bloomFilter.addLong(TEST_INTEGER);
        assertThat(bloomFilter.testLong(TEST_INTEGER)).isTrue();
        assertThat(bloomFilter.testLong(TEST_INTEGER + 1)).isFalse();

        // Re-construct
        BloomFilter newBloomFilter = new BloomFilter(bloomFilter.getBitSet(), bloomFilter.getNumHashFunctions());

        // String
        assertThat(newBloomFilter.test(TEST_STRING)).isTrue();
        assertThat(newBloomFilter.testSlice(wrappedBuffer(TEST_STRING))).isTrue();
        assertThat(newBloomFilter.test(TEST_STRING_NOT_WRITTEN)).isFalse();
        assertThat(newBloomFilter.testSlice(wrappedBuffer(TEST_STRING_NOT_WRITTEN))).isFalse();

        // Integer
        assertThat(newBloomFilter.testLong(TEST_INTEGER)).isTrue();
        assertThat(newBloomFilter.testLong(TEST_INTEGER + 1)).isFalse();
    }

    @Test
    public void testOrcHiveBloomFilterSerde()
            throws Exception
    {
        BloomFilter bloomFilterWrite = new BloomFilter(1000L, 0.05);

        bloomFilterWrite.add(TEST_STRING);
        assertThat(bloomFilterWrite.test(TEST_STRING)).isTrue();
        assertThat(bloomFilterWrite.testSlice(wrappedBuffer(TEST_STRING))).isTrue();

        Slice bloomFilterBytes = new CompressedMetadataWriter(new OrcMetadataWriter(WriterIdentification.TRINO), CompressionKind.NONE, 1024)
                .writeBloomFilters(ImmutableList.of(bloomFilterWrite));

        // Read through method
        InputStream inputStream = bloomFilterBytes.getInput();
        OrcMetadataReader metadataReader = new OrcMetadataReader(new OrcReaderOptions());
        List<BloomFilter> bloomFilters = metadataReader.readBloomFilterIndexes(inputStream);

        assertThat(bloomFilters.size()).isEqualTo(1);

        assertThat(bloomFilters.get(0).test(TEST_STRING)).isTrue();
        assertThat(bloomFilters.get(0).testSlice(wrappedBuffer(TEST_STRING))).isTrue();
        assertThat(bloomFilters.get(0).test(TEST_STRING_NOT_WRITTEN)).isFalse();
        assertThat(bloomFilters.get(0).testSlice(wrappedBuffer(TEST_STRING_NOT_WRITTEN))).isFalse();

        assertThat(bloomFilterWrite.getNumBits()).isEqualTo(bloomFilters.get(0).getNumBits());
        assertThat(bloomFilterWrite.getNumHashFunctions()).isEqualTo(bloomFilters.get(0).getNumHashFunctions());

        // Validate bit set
        assertThat(Arrays.equals(bloomFilters.get(0).getBitSet(), bloomFilterWrite.getBitSet())).isTrue();

        // Read directly: allows better inspection of the bit sets (helped to fix a lot of bugs)
        CodedInputStream input = CodedInputStream.newInstance(bloomFilterBytes.getBytes());
        OrcProto.BloomFilterIndex deserializedBloomFilterIndex = OrcProto.BloomFilterIndex.parseFrom(input);
        List<OrcProto.BloomFilter> bloomFilterList = deserializedBloomFilterIndex.getBloomFilterList();
        assertThat(bloomFilterList.size()).isEqualTo(1);

        OrcProto.BloomFilter bloomFilterRead = bloomFilterList.get(0);

        // Validate contents of ORC bloom filter bit set
        assertThat(Arrays.equals(Longs.toArray(bloomFilterRead.getBitsetList()), bloomFilterWrite.getBitSet())).isTrue();

        // hash functions
        assertThat(bloomFilterWrite.getNumHashFunctions()).isEqualTo(bloomFilterRead.getNumHashFunctions());

        // bit size
        assertThat(bloomFilterWrite.getBitSet().length).isEqualTo(bloomFilterRead.getBitsetCount());
    }

    @Test
    public void testBloomFilterPredicateValuesExisting()
    {
        BloomFilter bloomFilter = new BloomFilter(TEST_VALUES.size() * 10L, 0.01);

        for (Map.Entry<Object, Type> testValue : TEST_VALUES.entrySet()) {
            Object o = testValue.getKey();
            if (o instanceof Long) {
                if (testValue.getValue() instanceof RealType) {
                    bloomFilter.addDouble(intBitsToFloat(((Number) o).intValue()));
                }
                else {
                    bloomFilter.addLong((Long) o);
                }
            }
            else if (o instanceof Integer) {
                bloomFilter.addLong((Integer) o);
            }
            else if (o instanceof String) {
                bloomFilter.add(((String) o).getBytes(UTF_8));
            }
            else if (o instanceof BigDecimal) {
                bloomFilter.add(o.toString().getBytes(UTF_8));
            }
            else if (o instanceof Slice) {
                bloomFilter.add(((Slice) o).getBytes());
            }
            else if (o instanceof Timestamp) {
                bloomFilter.addLong(((Timestamp) o).getTime());
            }
            else if (o instanceof Double) {
                bloomFilter.addDouble((Double) o);
            }
            else {
                fail("Unsupported type " + o.getClass());
            }
        }

        for (Map.Entry<Object, Type> testValue : TEST_VALUES.entrySet()) {
            boolean matched = checkInBloomFilter(bloomFilter, testValue.getKey(), testValue.getValue());
            assertThat(matched)
                    .describedAs("type " + testValue.getClass())
                    .isTrue();
        }
    }

    @Test
    public void testBloomFilterPredicateValuesNonExisting()
    {
        BloomFilter bloomFilter = new BloomFilter(TEST_VALUES.size() * 10L, 0.01);

        for (Map.Entry<Object, Type> testValue : TEST_VALUES.entrySet()) {
            boolean matched = checkInBloomFilter(bloomFilter, testValue.getKey(), testValue.getValue());
            assertThat(matched)
                    .describedAs("type " + testValue.getKey().getClass())
                    .isFalse();
        }
    }

    @Test
    // simulate query on 2 columns where 1 is used as part of the where, with and without bloom filter
    public void testMatches()
    {
        TupleDomainOrcPredicate predicate = TupleDomainOrcPredicate.builder()
                .setBloomFiltersEnabled(true)
                .addColumn(ROOT_COLUMN, Domain.singleValue(BIGINT, 1234L))
                .build();
        TupleDomainOrcPredicate emptyPredicate = TupleDomainOrcPredicate.builder().build();

        ColumnMetadata<ColumnStatistics> matchingStatisticsByColumnIndex = new ColumnMetadata<>(ImmutableList.of(new ColumnStatistics(
                null,
                0,
                null,
                new IntegerStatistics(10L, 2000L, null),
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                new Utf8BloomFilterBuilder(1000, 0.01)
                        .addLong(1234L)
                        .buildBloomFilter())));

        ColumnMetadata<ColumnStatistics> nonMatchingStatisticsByColumnIndex = new ColumnMetadata<>(ImmutableList.of(new ColumnStatistics(
                null,
                0,
                null,
                new IntegerStatistics(10L, 2000L, null),
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                new Utf8BloomFilterBuilder(1000, 0.01)
                        .buildBloomFilter())));

        ColumnMetadata<ColumnStatistics> withoutBloomFilterStatisticsByColumnIndex = new ColumnMetadata<>(ImmutableList.of(new ColumnStatistics(
                null,
                0,
                null,
                new IntegerStatistics(10L, 2000L, null),
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null)));

        assertThat(predicate.matches(1L, matchingStatisticsByColumnIndex)).isTrue();
        assertThat(predicate.matches(1L, withoutBloomFilterStatisticsByColumnIndex)).isTrue();
        assertThat(predicate.matches(1L, nonMatchingStatisticsByColumnIndex)).isFalse();
        assertThat(emptyPredicate.matches(1L, matchingStatisticsByColumnIndex)).isTrue();
    }

    @Test
    public void testMatchesExpandedRange()
    {
        Range range = Range.range(BIGINT, 1233L, true, 1235L, true);
        TupleDomainOrcPredicate predicate = TupleDomainOrcPredicate.builder()
                .setBloomFiltersEnabled(true)
                .addColumn(ROOT_COLUMN, Domain.create(ValueSet.ofRanges(range), false))
                .setDomainCompactionThreshold(100)
                .build();

        ColumnMetadata<ColumnStatistics> matchingStatisticsByColumnIndex = new ColumnMetadata<>(ImmutableList.of(new ColumnStatistics(
                null,
                0,
                null,
                new IntegerStatistics(10L, 2000L, null),
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                new Utf8BloomFilterBuilder(1000, 0.01)
                        .addLong(1234L)
                        .buildBloomFilter())));

        ColumnMetadata<ColumnStatistics> nonMatchingStatisticsByColumnIndex = new ColumnMetadata<>(ImmutableList.of(new ColumnStatistics(
                null,
                0,
                null,
                new IntegerStatistics(10L, 2000L, null),
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                new Utf8BloomFilterBuilder(1000, 0.01)
                        .addLong(9876L)
                        .buildBloomFilter())));

        assertThat(predicate.matches(1L, matchingStatisticsByColumnIndex)).isTrue();
        assertThat(predicate.matches(1L, nonMatchingStatisticsByColumnIndex)).isFalse();
    }

    @Test
    public void testMatchesNonExpandedRange()
    {
        ColumnMetadata<ColumnStatistics> matchingStatisticsByColumnIndex = new ColumnMetadata<>(ImmutableList.of(new ColumnStatistics(
                null,
                0,
                null,
                new IntegerStatistics(10L, 2000L, null),
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                new Utf8BloomFilterBuilder(1000, 0.01)
                        .addLong(1500L)
                        .buildBloomFilter())));

        Range range = Range.range(BIGINT, 1233L, true, 1235L, true);
        TupleDomainOrcPredicate.TupleDomainOrcPredicateBuilder builder = TupleDomainOrcPredicate.builder()
                .setBloomFiltersEnabled(true)
                .addColumn(ROOT_COLUMN, Domain.create(ValueSet.ofRanges(range), false));

        // Domain expansion doesn't take place -> no bloom filtering -> ranges overlap
        assertThat(builder.setDomainCompactionThreshold(1).build().matches(1L, matchingStatisticsByColumnIndex)).isTrue();
        assertThat(builder.setDomainCompactionThreshold(100).build().matches(1L, matchingStatisticsByColumnIndex)).isFalse();
    }

    @Test
    public void testBloomFilterCompatibility()
    {
        for (int n = 0; n < 200; n++) {
            double fpp = ThreadLocalRandom.current().nextDouble(0.01, 0.10);
            int size = ThreadLocalRandom.current().nextInt(100, 10000);
            int entries = ThreadLocalRandom.current().nextInt(size / 2, size);

            BloomFilter actual = new BloomFilter(size, fpp);
            io.trino.hive.orc.util.BloomFilter expected = new io.trino.hive.orc.util.BloomFilter(size, fpp);

            assertThat(actual.test(null)).isFalse();
            assertThat(expected.test(null)).isFalse();

            byte[][] binaryValue = new byte[entries][];
            long[] longValue = new long[entries];
            double[] doubleValue = new double[entries];
            float[] floatValue = new float[entries];

            for (int i = 0; i < entries; i++) {
                binaryValue[i] = randomBytes(ThreadLocalRandom.current().nextInt(100));
                longValue[i] = ThreadLocalRandom.current().nextLong();
                doubleValue[i] = ThreadLocalRandom.current().nextDouble();
                floatValue[i] = ThreadLocalRandom.current().nextFloat();
            }

            for (int i = 0; i < entries; i++) {
                assertThat(actual.test(binaryValue[i])).isFalse();
                assertThat(actual.testSlice(wrappedBuffer(binaryValue[i]))).isFalse();
                assertThat(actual.testLong(longValue[i])).isFalse();
                assertThat(actual.testDouble(doubleValue[i])).isFalse();
                assertThat(actual.testFloat(floatValue[i])).isFalse();

                assertThat(expected.test(binaryValue[i])).isFalse();
                assertThat(expected.testLong(longValue[i])).isFalse();
                assertThat(expected.testDouble(doubleValue[i])).isFalse();
                assertThat(expected.testDouble(floatValue[i])).isFalse();
            }

            for (int i = 0; i < entries; i++) {
                actual.add(binaryValue[i]);
                actual.addLong(longValue[i]);
                actual.addDouble(doubleValue[i]);
                actual.addFloat(floatValue[i]);

                expected.add(binaryValue[i]);
                expected.addLong(longValue[i]);
                expected.addDouble(doubleValue[i]);
                expected.addDouble(floatValue[i]);
            }

            for (int i = 0; i < entries; i++) {
                assertThat(actual.test(binaryValue[i])).isTrue();
                assertThat(actual.testSlice(wrappedBuffer(binaryValue[i]))).isTrue();
                assertThat(actual.testLong(longValue[i])).isTrue();
                assertThat(actual.testDouble(doubleValue[i])).isTrue();
                assertThat(actual.testFloat(floatValue[i])).isTrue();

                assertThat(expected.test(binaryValue[i])).isTrue();
                assertThat(expected.testLong(longValue[i])).isTrue();
                assertThat(expected.testDouble(doubleValue[i])).isTrue();
                assertThat(expected.testDouble(floatValue[i])).isTrue();
            }

            actual.add((byte[]) null);
            expected.add(null);

            assertThat(actual.test(null)).isTrue();
            assertThat(actual.testSlice(null)).isTrue();
            assertThat(expected.test(null)).isTrue();

            assertThat(actual.getBitSet()).isEqualTo(expected.getBitSet());
        }
    }

    @Test
    public void testHashCompatibility()
    {
        for (int length = 0; length < 1000; length++) {
            for (int i = 0; i < 100; i++) {
                byte[] bytes = randomBytes(length);
                assertThat(BloomFilter.OrcMurmur3.hash64(bytes)).isEqualTo(Murmur3.hash64(bytes));
            }
        }
    }

    private static byte[] randomBytes(int length)
    {
        byte[] result = new byte[length];
        ThreadLocalRandom.current().nextBytes(result);
        return result;
    }
}
