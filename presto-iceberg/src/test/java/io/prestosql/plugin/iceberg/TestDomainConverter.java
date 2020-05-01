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
package io.prestosql.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slices;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.Type;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.plugin.iceberg.DomainConverter.convertTupleDomainTypes;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.TimeType.TIME;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.testng.Assert.assertEquals;

public class TestDomainConverter
{
    private static final Function<Type, IcebergColumnHandle> ICEBERG_COLUMN_PROVIDER = type -> new IcebergColumnHandle(0, "column", type, Optional.empty());

    @Test
    public void testSimple()
    {
        assertTupleDomain(TupleDomain.all(), TupleDomain.all());
        assertTupleDomain(TupleDomain.all(), TupleDomain.all());
    }

    @Test
    public void testBoolean()
    {
        assertTupleDomainUnchanged(
                TupleDomain.withColumnDomains(
                        ImmutableMap.of(ICEBERG_COLUMN_PROVIDER.apply(BOOLEAN), Domain.singleValue(BOOLEAN, true))));

        assertTupleDomainUnchanged(
                TupleDomain.withColumnDomains(
                        ImmutableMap.of(ICEBERG_COLUMN_PROVIDER.apply(BOOLEAN), Domain.singleValue(BOOLEAN, false))));
    }

    @Test
    public void testDate()
    {
        assertTupleDomainUnchanged(
                TupleDomain.withColumnDomains(
                        ImmutableMap.of(ICEBERG_COLUMN_PROVIDER.apply(DATE), Domain.singleValue(DATE, 1L))));

        assertTupleDomainUnchanged(
                TupleDomain.withColumnDomains(
                        ImmutableMap.of(ICEBERG_COLUMN_PROVIDER.apply(DATE), Domain.multipleValues(DATE, ImmutableList.of(1L, 2L, 3L)))));
    }

    @Test
    public void testVarbinary()
    {
        assertTupleDomainUnchanged(
                TupleDomain.withColumnDomains(
                        ImmutableMap.of(ICEBERG_COLUMN_PROVIDER.apply(VARBINARY), Domain.singleValue(VARBINARY, Slices.utf8Slice("apple")))));

        assertTupleDomainUnchanged(
                TupleDomain.withColumnDomains(
                        ImmutableMap.of(ICEBERG_COLUMN_PROVIDER.apply(VARBINARY), Domain.multipleValues(VARBINARY, ImmutableList.of(Slices.utf8Slice("apple"), Slices.utf8Slice("banana"))))));
    }

    @Test
    public void testDouble()
    {
        assertTupleDomainUnchanged(
                TupleDomain.withColumnDomains(
                        ImmutableMap.of(ICEBERG_COLUMN_PROVIDER.apply(DOUBLE), Domain.singleValue(DOUBLE, 1.0d))));

        assertTupleDomainUnchanged(
                TupleDomain.withColumnDomains(
                        ImmutableMap.of(ICEBERG_COLUMN_PROVIDER.apply(DOUBLE), Domain.multipleValues(DOUBLE, ImmutableList.of(1.0d, 2.0d, 3.0d)))));
    }

    @Test
    public void testBigint()
    {
        assertTupleDomainUnchanged(
                TupleDomain.withColumnDomains(
                        ImmutableMap.of(ICEBERG_COLUMN_PROVIDER.apply(BIGINT), Domain.singleValue(BIGINT, 1L))));

        assertTupleDomainUnchanged(
                TupleDomain.withColumnDomains(
                        ImmutableMap.of(ICEBERG_COLUMN_PROVIDER.apply(BIGINT), Domain.multipleValues(BIGINT, ImmutableList.of(1L, 2L, 3L)))));
    }

    @Test
    public void testReal()
    {
        assertTupleDomainUnchanged(
                TupleDomain.withColumnDomains(
                        ImmutableMap.of(ICEBERG_COLUMN_PROVIDER.apply(REAL), Domain.singleValue(REAL, 1L))));

        assertTupleDomainUnchanged(
                TupleDomain.withColumnDomains(
                        ImmutableMap.of(ICEBERG_COLUMN_PROVIDER.apply(REAL), Domain.multipleValues(REAL, ImmutableList.of(1L, 2L, 3L)))));
    }

    @Test
    public void testInteger()
    {
        assertTupleDomainUnchanged(
                TupleDomain.withColumnDomains(
                        ImmutableMap.of(ICEBERG_COLUMN_PROVIDER.apply(BIGINT), Domain.singleValue(INTEGER, 1L))));

        assertTupleDomainUnchanged(
                TupleDomain.withColumnDomains(
                        ImmutableMap.of(ICEBERG_COLUMN_PROVIDER.apply(BIGINT), Domain.multipleValues(INTEGER, ImmutableList.of(1L, 2L, 3L)))));
    }

    @Test
    public void testVarchar()
    {
        assertTupleDomainUnchanged(
                TupleDomain.withColumnDomains(
                        ImmutableMap.of(ICEBERG_COLUMN_PROVIDER.apply(VARCHAR), Domain.singleValue(VARCHAR, Slices.utf8Slice("apple")))));

        assertTupleDomainUnchanged(
                TupleDomain.withColumnDomains(
                        ImmutableMap.of(ICEBERG_COLUMN_PROVIDER.apply(VARCHAR), Domain.multipleValues(VARCHAR, ImmutableList.of(Slices.utf8Slice("apple"), Slices.utf8Slice("banana"))))));
    }

    @Test
    public void testTimestamp()
    {
        assertTupleDomain(
                TupleDomain.withColumnDomains(
                        ImmutableMap.of(ICEBERG_COLUMN_PROVIDER.apply(TIMESTAMP), Domain.singleValue(TIMESTAMP, 1_234_567_890_123L))),
                TupleDomain.withColumnDomains(
                        ImmutableMap.of(ICEBERG_COLUMN_PROVIDER.apply(TIMESTAMP), Domain.singleValue(TIMESTAMP, 1_234_567_890_123_000L))));

        assertTupleDomain(
                TupleDomain.withColumnDomains(
                        ImmutableMap.of(ICEBERG_COLUMN_PROVIDER.apply(TIMESTAMP), Domain.multipleValues(TIMESTAMP, ImmutableList.of(1_234_567_890_123L, 1_234_567_890_124L)))),
                TupleDomain.withColumnDomains(
                        ImmutableMap.of(ICEBERG_COLUMN_PROVIDER.apply(TIMESTAMP), Domain.multipleValues(TIMESTAMP, ImmutableList.of(1_234_567_890_123_000L, 1_234_567_890_124_000L)))));
    }

    @Test
    public void testTime()
    {
        assertTupleDomain(
                TupleDomain.withColumnDomains(
                        ImmutableMap.of(ICEBERG_COLUMN_PROVIDER.apply(TIME), Domain.singleValue(TIME, 1_234_567_890_123L))),
                TupleDomain.withColumnDomains(
                        ImmutableMap.of(ICEBERG_COLUMN_PROVIDER.apply(TIME), Domain.singleValue(TIME, 1_234_567_890_123_000L))));

        assertTupleDomain(
                TupleDomain.withColumnDomains(
                        ImmutableMap.of(ICEBERG_COLUMN_PROVIDER.apply(TIME), Domain.multipleValues(TIME, ImmutableList.of(1_234_567_890_123L, 1_234_567_890_124L)))),
                TupleDomain.withColumnDomains(
                        ImmutableMap.of(ICEBERG_COLUMN_PROVIDER.apply(TIME), Domain.multipleValues(TIME, ImmutableList.of(1_234_567_890_123_000L, 1_234_567_890_124_000L)))));
    }

    @Test
    public void testTimestampWithTimezone()
    {
        assertTupleDomain(
                TupleDomain.withColumnDomains(
                        ImmutableMap.of(ICEBERG_COLUMN_PROVIDER.apply(TIMESTAMP_WITH_TIME_ZONE), Domain.singleValue(TIMESTAMP_WITH_TIME_ZONE, 1_234_567_890_123L))),
                TupleDomain.withColumnDomains(
                        ImmutableMap.of(ICEBERG_COLUMN_PROVIDER.apply(TIMESTAMP_WITH_TIME_ZONE), Domain.singleValue(TIMESTAMP_WITH_TIME_ZONE, MILLISECONDS.toMicros(unpackMillisUtc(1_234_567_890_123L))))));

        List<Long> list = ImmutableList.of(1_234_567_890_123L, 1_234_567_890_124L);
        assertTupleDomain(
                TupleDomain.withColumnDomains(
                        ImmutableMap.of(ICEBERG_COLUMN_PROVIDER.apply(TIMESTAMP_WITH_TIME_ZONE), Domain.multipleValues(TIMESTAMP_WITH_TIME_ZONE, list))),
                TupleDomain.withColumnDomains(
                        ImmutableMap.of(ICEBERG_COLUMN_PROVIDER.apply(TIMESTAMP_WITH_TIME_ZONE), Domain.multipleValues(TIMESTAMP_WITH_TIME_ZONE, list.stream().map(value -> MILLISECONDS.toMicros(unpackMillisUtc(value))).collect(toImmutableList())))));
    }

    @Test
    public void testMultipleColumnsTupleDomain()
    {
        assertTupleDomain(
                TupleDomain.withColumnDomains(
                        ImmutableMap.of(
                                ICEBERG_COLUMN_PROVIDER.apply(TIMESTAMP_WITH_TIME_ZONE), Domain.singleValue(TIMESTAMP_WITH_TIME_ZONE, 1_234_567_890_123L),
                                ICEBERG_COLUMN_PROVIDER.apply(TIME), Domain.singleValue(TIME, 1_234_567_890_123L),
                                ICEBERG_COLUMN_PROVIDER.apply(BIGINT), Domain.multipleValues(BIGINT, ImmutableList.of(1L, 2L, 3L)))),
                TupleDomain.withColumnDomains(
                        ImmutableMap.of(
                                ICEBERG_COLUMN_PROVIDER.apply(TIMESTAMP_WITH_TIME_ZONE), Domain.singleValue(TIMESTAMP_WITH_TIME_ZONE, MILLISECONDS.toMicros(unpackMillisUtc(1_234_567_890_123L))),
                                ICEBERG_COLUMN_PROVIDER.apply(TIME), Domain.singleValue(TIME, 1_234_567_890_123_000L),
                                ICEBERG_COLUMN_PROVIDER.apply(BIGINT), Domain.multipleValues(BIGINT, ImmutableList.of(1L, 2L, 3L)))));
    }

    private void assertTupleDomainUnchanged(TupleDomain<IcebergColumnHandle> domain)
    {
        assertTupleDomain(domain, domain);
    }

    private void assertTupleDomain(TupleDomain<IcebergColumnHandle> actual, TupleDomain<IcebergColumnHandle> expected)
    {
        assertEquals(convertTupleDomainTypes(actual), expected);
    }
}
