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
package io.prestosql.orc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.orc.TupleDomainOrcPredicate.ColumnReference;
import io.prestosql.spi.predicate.NullableValue;
import io.prestosql.spi.type.SqlDate;
import io.prestosql.spi.type.SqlTimestamp;
import io.prestosql.spi.type.SqlVarbinary;
import io.prestosql.spi.type.TimeZoneKey;
import io.prestosql.spi.type.Type;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;

import static com.google.common.collect.Iterables.cycle;
import static com.google.common.collect.Iterables.limit;
import static com.google.common.collect.Lists.newArrayList;
import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.prestosql.orc.OrcReader.MAX_BATCH_SIZE;
import static io.prestosql.orc.OrcTester.Format.ORC_12;
import static io.prestosql.orc.OrcTester.HIVE_STORAGE_TIME_ZONE;
import static io.prestosql.orc.OrcTester.READER_OPTIONS;
import static io.prestosql.orc.OrcTester.writeOrcColumnHive;
import static io.prestosql.orc.metadata.CompressionKind.LZ4;
import static io.prestosql.spi.predicate.TupleDomain.fromFixedValues;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static java.lang.Float.floatToIntBits;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;

public class TestReadBloomFilter
{
    private static final TimeZoneKey TIME_ZONE = TimeZoneKey.getTimeZoneKey(HIVE_STORAGE_TIME_ZONE.getID());

    @Test
    public void test()
            throws Exception
    {
        testType(TINYINT, ImmutableList.of(1L, 50L, 100L), 50L, 77L);
        testType(SMALLINT, ImmutableList.of(1L, 5000L, 10_000L), 5000L, 7777L);
        testType(INTEGER, ImmutableList.of(1L, 500_000L, 1_000_000L), 500_000L, 777_777L);
        testType(BIGINT, ImmutableList.of(1L, 500_000L, 1_000_000L), 500_000L, 777_777L);

        testType(DATE, ImmutableList.of(new SqlDate(1), new SqlDate(5_000), new SqlDate(10_000)), 5_000L, 7_777L);
        testType(TIMESTAMP,
                ImmutableList.of(new SqlTimestamp(1, TIME_ZONE), new SqlTimestamp(500_000L, TIME_ZONE), new SqlTimestamp(1_000_000L, TIME_ZONE)),
                500_000L + HIVE_STORAGE_TIME_ZONE.getOffset(500_000L),
                777_777L + HIVE_STORAGE_TIME_ZONE.getOffset(777_777L));

        testType(REAL, ImmutableList.of(1.11f, 500_000.56f, 1_000_000.99f), (long) floatToIntBits(500_000.56f), (long) floatToIntBits(777_777.77f));
        testType(DOUBLE, ImmutableList.of(1.11, 500_000.55, 1_000_000.99), 500_000.55, 777_777.77);

        testType(VARCHAR, ImmutableList.of("a", "o", "z"), utf8Slice("o"), utf8Slice("w"));
        testType(VARBINARY,
                ImmutableList.of(new SqlVarbinary("a".getBytes(UTF_8)), new SqlVarbinary("o".getBytes(UTF_8)), new SqlVarbinary("z".getBytes(UTF_8))),
                utf8Slice("o"),
                utf8Slice("w"));
    }

    private static <T> void testType(Type type, List<T> uniqueValues, T inBloomFilter, T notInBloomFilter)
            throws Exception
    {
        Stream<T> writeValues = newArrayList(limit(cycle(uniqueValues), 30_000)).stream();

        try (TempFile tempFile = new TempFile()) {
            writeOrcColumnHive(tempFile.getFile(), ORC_12, LZ4, type, writeValues.iterator());

            // without predicate a normal block will be created
            try (OrcRecordReader recordReader = createCustomOrcRecordReader(tempFile, OrcPredicate.TRUE, type, MAX_BATCH_SIZE)) {
                assertEquals(recordReader.nextBatch(), 1024);
            }

            // predicate for specific value within the min/max range without bloom filter being enabled
            TupleDomainOrcPredicate<String> noBloomFilterPredicate = new TupleDomainOrcPredicate<>(
                    fromFixedValues(ImmutableMap.of("test", NullableValue.of(type, notInBloomFilter))),
                    ImmutableList.of(new ColumnReference<>("test", 0, type)),
                    false);

            try (OrcRecordReader recordReader = createCustomOrcRecordReader(tempFile, noBloomFilterPredicate, type, MAX_BATCH_SIZE)) {
                assertEquals(recordReader.nextBatch(), 1024);
            }

            // predicate for specific value within the min/max range with bloom filter enabled, but a value not in the bloom filter
            TupleDomainOrcPredicate<String> notMatchBloomFilterPredicate = new TupleDomainOrcPredicate<>(
                    fromFixedValues(ImmutableMap.of("test", NullableValue.of(type, notInBloomFilter))),
                    ImmutableList.of(new ColumnReference<>("test", 0, type)),
                    true);

            try (OrcRecordReader recordReader = createCustomOrcRecordReader(tempFile, notMatchBloomFilterPredicate, type, MAX_BATCH_SIZE)) {
                assertEquals(recordReader.nextBatch(), -1);
            }

            // predicate for specific value within the min/max range with bloom filter enabled, and a value in the bloom filter
            TupleDomainOrcPredicate<String> matchBloomFilterPredicate = new TupleDomainOrcPredicate<>(
                    fromFixedValues(ImmutableMap.of("test", NullableValue.of(type, inBloomFilter))),
                    ImmutableList.of(new ColumnReference<>("test", 0, type)),
                    true);

            try (OrcRecordReader recordReader = createCustomOrcRecordReader(tempFile, matchBloomFilterPredicate, type, MAX_BATCH_SIZE)) {
                assertEquals(recordReader.nextBatch(), 1024);
            }
        }
    }

    private static OrcRecordReader createCustomOrcRecordReader(TempFile tempFile, OrcPredicate predicate, Type type, int initialBatchSize)
            throws IOException
    {
        OrcDataSource orcDataSource = new FileOrcDataSource(tempFile.getFile(), READER_OPTIONS);
        OrcReader orcReader = new OrcReader(orcDataSource, READER_OPTIONS);

        assertEquals(orcReader.getColumnNames(), ImmutableList.of("test"));
        assertEquals(orcReader.getFooter().getRowsInRowGroup(), 10_000);

        return orcReader.createRecordReader(ImmutableMap.of(0, type), predicate, HIVE_STORAGE_TIME_ZONE, newSimpleAggregatedMemoryContext(), initialBatchSize);
    }
}
