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
import io.trino.orc.metadata.OrcColumnId;
import io.trino.spi.predicate.Domain;
import io.trino.spi.type.SqlDate;
import io.trino.spi.type.SqlTimestamp;
import io.trino.spi.type.SqlVarbinary;
import io.trino.spi.type.Type;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;

import static com.google.common.collect.Iterables.cycle;
import static com.google.common.collect.Iterables.limit;
import static com.google.common.collect.Lists.newArrayList;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.orc.OrcReader.MAX_BATCH_SIZE;
import static io.trino.orc.OrcTester.Format.ORC_12;
import static io.trino.orc.OrcTester.HIVE_STORAGE_TIME_ZONE;
import static io.trino.orc.OrcTester.READER_OPTIONS;
import static io.trino.orc.OrcTester.writeOrcColumnHive;
import static io.trino.orc.metadata.CompressionKind.LZ4;
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
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class TestReadBloomFilter
{
    @Test
    public void test()
            throws Exception
    {
        testType(TINYINT, ImmutableList.of(1L, 50L, 100L), 50L, 77L);
        testType(SMALLINT, ImmutableList.of(1L, 5000L, 10_000L), 5000L, 7777L);
        testType(INTEGER, ImmutableList.of(1L, 500_000L, 1_000_000L), 500_000L, 777_777L);
        testType(BIGINT, ImmutableList.of(1L, 500_000L, 1_000_000L), 500_000L, 777_777L);

        testType(DATE, ImmutableList.of(new SqlDate(1), new SqlDate(5_000), new SqlDate(10_000)), 5_000L, 7_777L);
        testType(TIMESTAMP_MILLIS,
                ImmutableList.of(SqlTimestamp.fromMillis(3, 1), SqlTimestamp.fromMillis(3, 500_000L), SqlTimestamp.fromMillis(3, 1_000_000L)),
                500_000L,
                777_777L);

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
                assertEquals(recordReader.nextPage().getLoadedPage().getPositionCount(), MAX_BATCH_SIZE);
            }

            // predicate for specific value within the min/max range without bloom filter being enabled
            TupleDomainOrcPredicate noBloomFilterPredicate = TupleDomainOrcPredicate.builder()
                    .addColumn(new OrcColumnId(1), Domain.singleValue(type, notInBloomFilter))
                    .build();

            try (OrcRecordReader recordReader = createCustomOrcRecordReader(tempFile, noBloomFilterPredicate, type, MAX_BATCH_SIZE)) {
                assertEquals(recordReader.nextPage().getLoadedPage().getPositionCount(), MAX_BATCH_SIZE);
            }

            // predicate for specific value within the min/max range with bloom filter enabled, but a value not in the bloom filter
            TupleDomainOrcPredicate notMatchBloomFilterPredicate = TupleDomainOrcPredicate.builder()
                    .addColumn(new OrcColumnId(1), Domain.singleValue(type, notInBloomFilter))
                    .setBloomFiltersEnabled(true)
                    .build();

            try (OrcRecordReader recordReader = createCustomOrcRecordReader(tempFile, notMatchBloomFilterPredicate, type, MAX_BATCH_SIZE)) {
                assertNull(recordReader.nextPage());
            }

            // predicate for specific value within the min/max range with bloom filter enabled, and a value in the bloom filter
            TupleDomainOrcPredicate matchBloomFilterPredicate = TupleDomainOrcPredicate.builder()
                    .addColumn(new OrcColumnId(1), Domain.singleValue(type, inBloomFilter))
                    .setBloomFiltersEnabled(true)
                    .build();

            try (OrcRecordReader recordReader = createCustomOrcRecordReader(tempFile, matchBloomFilterPredicate, type, MAX_BATCH_SIZE)) {
                assertEquals(recordReader.nextPage().getLoadedPage().getPositionCount(), MAX_BATCH_SIZE);
            }
        }
    }

    private static OrcRecordReader createCustomOrcRecordReader(TempFile tempFile, OrcPredicate predicate, Type type, int initialBatchSize)
            throws IOException
    {
        OrcDataSource orcDataSource = new FileOrcDataSource(tempFile.getFile(), READER_OPTIONS);
        OrcReader orcReader = OrcReader.createOrcReader(orcDataSource, READER_OPTIONS)
                .orElseThrow(() -> new RuntimeException("File is empty"));

        assertEquals(orcReader.getColumnNames(), ImmutableList.of("test"));
        assertEquals(orcReader.getFooter().getRowsInRowGroup().orElse(0), 10_000);

        return orcReader.createRecordReader(
                orcReader.getRootColumn().getNestedColumns(),
                ImmutableList.of(type),
                predicate,
                HIVE_STORAGE_TIME_ZONE,
                newSimpleAggregatedMemoryContext(),
                initialBatchSize,
                RuntimeException::new);
    }
}
