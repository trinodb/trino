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
package io.trino.sql;

import com.google.common.collect.ImmutableList;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.Int128;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.predicate.Domain.multipleValues;
import static io.trino.spi.predicate.Range.range;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.TIME_NANOS;
import static io.trino.spi.type.TimeType.TIME_PICOS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.DynamicFilters.collapseConsecutiveValues;
import static org.assertj.core.api.Assertions.assertThat;

final class TestDynamicFilters
{
    @Test
    public void testCollapseConsecutiveValues()
    {
        for (Type type : ImmutableList.of(TINYINT, SMALLINT, INTEGER, BIGINT, DATE, TIME_PICOS, TIMESTAMP_MICROS, createDecimalType(10, 2))) {
            assertThat(collapseConsecutiveValues(multipleValues(type, ImmutableList.of(3L, 4L, 5L))))
                    .isEqualTo(Domain.create(ValueSet.ofRanges(range(type, 3L, true, 5L, true)), false));
            assertThat(collapseConsecutiveValues(multipleValues(type, ImmutableList.of(10L, 11L), true)))
                    .isEqualTo(Domain.create(ValueSet.ofRanges(range(type, 10L, true, 11L, true)), true));
        }
    }

    @Test
    public void testCollapseValuesAtTypeBoundaries()
    {
        assertThat(collapseConsecutiveValues(multipleValues(BIGINT, ImmutableList.of(Long.MIN_VALUE, Long.MIN_VALUE + 1))))
                .isEqualTo(Domain.create(ValueSet.ofRanges(range(BIGINT, Long.MIN_VALUE, true, Long.MIN_VALUE + 1, true)), false));
        assertThat(collapseConsecutiveValues(multipleValues(BIGINT, ImmutableList.of(Long.MAX_VALUE - 1, Long.MAX_VALUE))))
                .isEqualTo(Domain.create(ValueSet.ofRanges(range(BIGINT, Long.MAX_VALUE - 1, true, Long.MAX_VALUE, true)), false));
        assertUnchanged(multipleValues(BIGINT, ImmutableList.of(Long.MIN_VALUE, Long.MAX_VALUE)));
        assertUnchanged(multipleValues(INTEGER, ImmutableList.of((long) Integer.MIN_VALUE, (long) Integer.MAX_VALUE)));
    }

    @Test
    public void testKeepNonConsecutiveValues()
    {
        assertUnchanged(multipleValues(INTEGER, ImmutableList.of(1L, 2L, 4L)));
        assertUnchanged(Domain.create(ValueSet.ofRanges(range(BIGINT, 1L, true, 2L, true), range(BIGINT, 3L, true, 4L, true)), false));
    }

    @Test
    public void testKeepValuesOfOtherTypes()
    {
        assertUnchanged(multipleValues(TIME_NANOS, ImmutableList.of(1000L, 2000L)));
        assertUnchanged(multipleValues(TIMESTAMP_MILLIS, ImmutableList.of(1000L, 2000L)));
        assertUnchanged(multipleValues(createDecimalType(20, 2), ImmutableList.of(Int128.valueOf(1), Int128.valueOf(2))));
        assertUnchanged(multipleValues(DOUBLE, ImmutableList.of(1.0, 2.0)));
        assertUnchanged(multipleValues(REAL, ImmutableList.of(1L, 2L)));
        assertUnchanged(multipleValues(VARCHAR, ImmutableList.of(utf8Slice("a"), utf8Slice("b"))));
    }

    @Test
    public void testKeepDomainsWithoutMultipleValues()
    {
        for (Domain domain : ImmutableList.of(Domain.singleValue(BIGINT, 1L), Domain.none(BIGINT), Domain.all(BIGINT), Domain.onlyNull(BIGINT))) {
            assertUnchanged(domain);
        }
    }

    private static void assertUnchanged(Domain domain)
    {
        assertThat(collapseConsecutiveValues(domain)).isSameAs(domain);
    }
}
