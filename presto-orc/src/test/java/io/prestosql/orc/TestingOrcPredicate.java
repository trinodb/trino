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

import com.google.common.collect.Ordering;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.prestosql.orc.metadata.statistics.BloomFilter;
import io.prestosql.orc.metadata.statistics.ColumnStatistics;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.CharType;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.MapType;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.SqlDate;
import io.prestosql.spi.type.SqlDecimal;
import io.prestosql.spi.type.SqlTimestamp;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarbinaryType;
import io.prestosql.spi.type.VarcharType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

import static com.google.common.collect.Lists.newArrayList;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public final class TestingOrcPredicate
{
    public static final int ORC_STRIPE_SIZE = 30_000;
    public static final int ORC_ROW_GROUP_SIZE = 10_000;

    private TestingOrcPredicate()
    {
    }

    public static OrcPredicate createOrcPredicate(Type type, Iterable<?> values)
    {
        List<Object> expectedValues = newArrayList(values);
        if (BOOLEAN.equals(type)) {
            return new BooleanOrcPredicate(expectedValues);
        }
        if (TINYINT.equals(type) || SMALLINT.equals(type) || INTEGER.equals(type) || BIGINT.equals(type)) {
            return new LongOrcPredicate(true,
                    expectedValues.stream()
                            .map(value -> value == null ? null : ((Number) value).longValue())
                            .collect(toList()));
        }
        if (TIMESTAMP.equals(type)) {
            return new LongOrcPredicate(false,
                    expectedValues.stream()
                            .map(value -> value == null ? null : ((SqlTimestamp) value).getMillisUtc())
                            .collect(toList()));
        }
        if (DATE.equals(type)) {
            return new DateOrcPredicate(
                    expectedValues.stream()
                            .map(value -> value == null ? null : (long) ((SqlDate) value).getDays())
                            .collect(toList()));
        }
        if (REAL.equals(type) || DOUBLE.equals(type)) {
            return new DoubleOrcPredicate(
                    expectedValues.stream()
                            .map(value -> value == null ? null : ((Number) value).doubleValue())
                            .collect(toList()));
        }
        if (type instanceof VarbinaryType) {
            // binary does not have stats
            return new BasicOrcPredicate<>(expectedValues, Object.class);
        }
        if (type instanceof VarcharType) {
            return new StringOrcPredicate(expectedValues);
        }
        if (type instanceof CharType) {
            return new CharOrcPredicate(expectedValues);
        }
        if (type instanceof DecimalType) {
            return new DecimalOrcPredicate(expectedValues);
        }

        if (type instanceof ArrayType || type instanceof MapType || type instanceof RowType) {
            return new BasicOrcPredicate<>(expectedValues, Object.class);
        }
        throw new IllegalArgumentException("Unsupported type " + type);
    }

    public static class BasicOrcPredicate<T>
            implements OrcPredicate
    {
        private final List<T> expectedValues;

        public BasicOrcPredicate(Iterable<?> expectedValues, Class<T> type)
        {
            List<T> values = new ArrayList<>();
            for (Object expectedValue : expectedValues) {
                values.add(type.cast(expectedValue));
            }
            this.expectedValues = Collections.unmodifiableList(values);
        }

        @Override
        public boolean matches(long numberOfRows, Map<Integer, ColumnStatistics> statisticsByColumnIndex)
        {
            ColumnStatistics columnStatistics = statisticsByColumnIndex.get(0);
            assertTrue(columnStatistics.hasNumberOfValues());

            if (numberOfRows == expectedValues.size()) {
                // whole file
                assertChunkStats(expectedValues, columnStatistics);
            }
            else if (numberOfRows == ORC_ROW_GROUP_SIZE) {
                // middle section
                matchMiddleSection(columnStatistics, ORC_ROW_GROUP_SIZE);
            }
            else if (numberOfRows == ORC_STRIPE_SIZE) {
                // middle section
                matchMiddleSection(columnStatistics, ORC_STRIPE_SIZE);
            }
            else if (numberOfRows == expectedValues.size() % ORC_ROW_GROUP_SIZE || numberOfRows == expectedValues.size() % ORC_STRIPE_SIZE) {
                // tail section
                List<T> chunk = expectedValues.subList((int) (expectedValues.size() - numberOfRows), expectedValues.size());
                assertChunkStats(chunk, columnStatistics);
            }
            else if (numberOfRows == expectedValues.size() % ORC_STRIPE_SIZE) {
                // tail section
                List<T> chunk = expectedValues.subList((int) (expectedValues.size() - numberOfRows), expectedValues.size());
                assertChunkStats(chunk, columnStatistics);
            }
            else {
                fail("Unexpected number of rows: " + numberOfRows);
            }
            return true;
        }

        private void matchMiddleSection(ColumnStatistics columnStatistics, int size)
        {
            int length;
            for (int offset = 0; offset < expectedValues.size(); offset += length) {
                length = Math.min(size, expectedValues.size() - offset);
                if (chunkMatchesStats(expectedValues.subList(offset, offset + length), columnStatistics)) {
                    return;
                }
            }
            fail("match not found for middle section");
        }

        private void assertChunkStats(List<T> chunk, ColumnStatistics columnStatistics)
        {
            assertTrue(chunkMatchesStats(chunk, columnStatistics));
        }

        protected boolean chunkMatchesStats(List<T> chunk, ColumnStatistics columnStatistics)
        {
            // verify non null count
            if (columnStatistics.getNumberOfValues() != chunk.stream().filter(Objects::nonNull).count()) {
                return false;
            }

            return true;
        }
    }

    public static class BooleanOrcPredicate
            extends BasicOrcPredicate<Boolean>
    {
        public BooleanOrcPredicate(Iterable<?> expectedValues)
        {
            super(expectedValues, Boolean.class);
        }

        @Override
        protected boolean chunkMatchesStats(List<Boolean> chunk, ColumnStatistics columnStatistics)
        {
            assertNull(columnStatistics.getIntegerStatistics());
            assertNull(columnStatistics.getDoubleStatistics());
            assertNull(columnStatistics.getStringStatistics());
            assertNull(columnStatistics.getDateStatistics());

            // check basic statistics
            if (!super.chunkMatchesStats(chunk, columnStatistics)) {
                return false;
            }

            // statistics can be missing for any reason
            if (columnStatistics.getBooleanStatistics() != null) {
                if (columnStatistics.getBooleanStatistics().getTrueValueCount() != chunk.stream().filter(Boolean.TRUE::equals).count()) {
                    return false;
                }
            }
            return true;
        }
    }

    public static class DoubleOrcPredicate
            extends BasicOrcPredicate<Double>
    {
        public DoubleOrcPredicate(Iterable<?> expectedValues)
        {
            super(expectedValues, Double.class);
        }

        @Override
        protected boolean chunkMatchesStats(List<Double> chunk, ColumnStatistics columnStatistics)
        {
            assertNull(columnStatistics.getBooleanStatistics());
            assertNull(columnStatistics.getIntegerStatistics());
            assertNull(columnStatistics.getStringStatistics());
            assertNull(columnStatistics.getDateStatistics());

            // check basic statistics
            if (!super.chunkMatchesStats(chunk, columnStatistics)) {
                return false;
            }

            BloomFilter bloomFilter = columnStatistics.getBloomFilter();
            if (bloomFilter != null) {
                for (Double value : chunk) {
                    if (value != null && !bloomFilter.testDouble(value)) {
                        return false;
                    }
                }
            }

            // statistics can be missing for any reason
            if (columnStatistics.getDoubleStatistics() != null) {
                if (chunk.stream().allMatch(Objects::isNull)) {
                    if (columnStatistics.getDoubleStatistics().getMin() != null || columnStatistics.getDoubleStatistics().getMax() != null) {
                        return false;
                    }
                }
                else {
                    // verify min
                    if (Math.abs(columnStatistics.getDoubleStatistics().getMin() - Ordering.natural().nullsLast().min(chunk)) > 0.001) {
                        return false;
                    }

                    // verify max
                    if (Math.abs(columnStatistics.getDoubleStatistics().getMax() - Ordering.natural().nullsFirst().max(chunk)) > 0.001) {
                        return false;
                    }
                }
            }
            return true;
        }
    }

    private static class DecimalOrcPredicate
            extends BasicOrcPredicate<SqlDecimal>
    {
        public DecimalOrcPredicate(Iterable<?> expectedValues)
        {
            super(expectedValues, SqlDecimal.class);
        }
    }

    public static class LongOrcPredicate
            extends BasicOrcPredicate<Long>
    {
        private final boolean testBloomFilter;

        public LongOrcPredicate(boolean testBloomFilter, Iterable<?> expectedValues)
        {
            super(expectedValues, Long.class);
            this.testBloomFilter = testBloomFilter;
        }

        @Override
        protected boolean chunkMatchesStats(List<Long> chunk, ColumnStatistics columnStatistics)
        {
            assertNull(columnStatistics.getBooleanStatistics());
            assertNull(columnStatistics.getDoubleStatistics());
            assertNull(columnStatistics.getStringStatistics());
            assertNull(columnStatistics.getDateStatistics());

            // check basic statistics
            if (!super.chunkMatchesStats(chunk, columnStatistics)) {
                return false;
            }

            // statistics can be missing for any reason
            if (columnStatistics.getIntegerStatistics() != null) {
                if (chunk.stream().allMatch(Objects::isNull)) {
                    if (columnStatistics.getIntegerStatistics().getMin() != null || columnStatistics.getIntegerStatistics().getMax() != null) {
                        return false;
                    }
                }
                else {
                    // verify min
                    if (!columnStatistics.getIntegerStatistics().getMin().equals(Ordering.natural().nullsLast().min(chunk))) {
                        return false;
                    }

                    // verify max
                    if (!columnStatistics.getIntegerStatistics().getMax().equals(Ordering.natural().nullsFirst().max(chunk))) {
                        return false;
                    }
                }
                long sum = chunk.stream()
                        .filter(Objects::nonNull)
                        .mapToLong(Long::longValue)
                        .sum();
                if (columnStatistics.getIntegerStatistics().getSum() != sum) {
                    return false;
                }

                BloomFilter bloomFilter = columnStatistics.getBloomFilter();
                if (testBloomFilter && bloomFilter != null) {
                    for (Long value : chunk) {
                        if (value != null && !bloomFilter.testLong(value)) {
                            return false;
                        }
                    }
                }
            }

            return true;
        }
    }

    public static class StringOrcPredicate
            extends BasicOrcPredicate<String>
    {
        public StringOrcPredicate(Iterable<?> expectedValues)
        {
            super(expectedValues, String.class);
        }

        @Override
        protected boolean chunkMatchesStats(List<String> chunk, ColumnStatistics columnStatistics)
        {
            assertNull(columnStatistics.getBooleanStatistics());
            assertNull(columnStatistics.getIntegerStatistics());
            assertNull(columnStatistics.getDoubleStatistics());
            assertNull(columnStatistics.getDateStatistics());

            // check basic statistics
            if (!super.chunkMatchesStats(chunk, columnStatistics)) {
                return false;
            }

            List<Slice> slices = chunk.stream()
                    .filter(Objects::nonNull)
                    .map(Slices::utf8Slice)
                    .collect(toList());

            BloomFilter bloomFilter = columnStatistics.getBloomFilter();
            if (bloomFilter != null) {
                for (Slice slice : slices) {
                    if (!bloomFilter.testSlice(slice)) {
                        return false;
                    }
                }
                int falsePositive = 0;
                byte[] testBuffer = new byte[32];
                for (int i = 0; i < 100_000; i++) {
                    ThreadLocalRandom.current().nextBytes(testBuffer);
                    if (bloomFilter.test(testBuffer)) {
                        falsePositive++;
                    }
                }
                if (falsePositive != 0 && 1.0 * falsePositive / 100_000 > 0.55) {
                    return false;
                }
            }

            // statistics can be missing for any reason
            if (columnStatistics.getStringStatistics() != null) {
                if (slices.isEmpty()) {
                    if (columnStatistics.getStringStatistics().getMin() != null || columnStatistics.getStringStatistics().getMax() != null) {
                        return false;
                    }
                }
                else {
                    Slice chunkMin = Ordering.natural().nullsLast().min(slices);
                    Slice chunkMax = Ordering.natural().nullsFirst().max(slices);
                    return columnStatistics.getStringStatistics().getMin().equals(chunkMin) &&
                            columnStatistics.getStringStatistics().getMax().equals(chunkMax);
                }
            }

            return true;
        }
    }

    public static class CharOrcPredicate
            extends BasicOrcPredicate<String>
    {
        public CharOrcPredicate(Iterable<?> expectedValues)
        {
            super(expectedValues, String.class);
        }

        @Override
        protected boolean chunkMatchesStats(List<String> chunk, ColumnStatistics columnStatistics)
        {
            assertNull(columnStatistics.getBooleanStatistics());
            assertNull(columnStatistics.getIntegerStatistics());
            assertNull(columnStatistics.getDoubleStatistics());
            assertNull(columnStatistics.getDateStatistics());

            // bloom filter for char type in ORC require padded values (padded according to the type in the footer)
            // this is difficult to support so we skip for now
            assertNull(columnStatistics.getBloomFilter());

            // check basic statistics
            if (!super.chunkMatchesStats(chunk, columnStatistics)) {
                return false;
            }

            List<String> strings = chunk.stream()
                    .filter(Objects::nonNull)
                    .map(String::trim)
                    .collect(toList());

            // statistics can be missing for any reason
            if (columnStatistics.getStringStatistics() != null) {
                if (strings.isEmpty()) {
                    if (columnStatistics.getStringStatistics().getMin() != null || columnStatistics.getStringStatistics().getMax() != null) {
                        return false;
                    }
                }
                else {
                    // verify min
                    String chunkMin = Ordering.natural().nullsLast().min(strings);
                    if (columnStatistics.getStringStatistics().getMin().toStringUtf8().trim().compareTo(chunkMin) > 0) {
                        return false;
                    }

                    // verify max
                    String chunkMax = Ordering.natural().nullsFirst().max(strings);
                    if (columnStatistics.getStringStatistics().getMax().toStringUtf8().trim().compareTo(chunkMax) < 0) {
                        return false;
                    }
                }
            }

            return true;
        }
    }

    public static class DateOrcPredicate
            extends BasicOrcPredicate<Long>
    {
        public DateOrcPredicate(Iterable<?> expectedValues)
        {
            super(expectedValues, Long.class);
        }

        @Override
        protected boolean chunkMatchesStats(List<Long> chunk, ColumnStatistics columnStatistics)
        {
            assertNull(columnStatistics.getBooleanStatistics());
            assertNull(columnStatistics.getIntegerStatistics());
            assertNull(columnStatistics.getDoubleStatistics());
            assertNull(columnStatistics.getStringStatistics());

            // check basic statistics
            if (!super.chunkMatchesStats(chunk, columnStatistics)) {
                return false;
            }

            // statistics can be missing for any reason
            if (columnStatistics.getDateStatistics() != null) {
                if (chunk.stream().allMatch(Objects::isNull)) {
                    if (columnStatistics.getDateStatistics().getMin() != null || columnStatistics.getDateStatistics().getMax() != null) {
                        return false;
                    }
                }
                else {
                    // verify min
                    Long min = columnStatistics.getDateStatistics().getMin().longValue();
                    if (!min.equals(Ordering.natural().nullsLast().min(chunk))) {
                        return false;
                    }

                    // verify max
                    Long statMax = columnStatistics.getDateStatistics().getMax().longValue();
                    Long chunkMax = Ordering.natural().nullsFirst().max(chunk);
                    if (!statMax.equals(chunkMax)) {
                        return false;
                    }

                    BloomFilter bloomFilter = columnStatistics.getBloomFilter();
                    if (bloomFilter != null) {
                        for (Long value : chunk) {
                            if (value != null && !bloomFilter.testLong(value)) {
                                return false;
                            }
                        }
                    }
                }
            }

            return true;
        }
    }
}
