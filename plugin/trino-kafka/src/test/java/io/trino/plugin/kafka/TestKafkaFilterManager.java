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
package io.trino.plugin.kafka;

import com.google.common.collect.ImmutableList;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.SortedRangeSet;
import org.testng.annotations.Test;

import java.util.Set;

import static io.trino.spi.predicate.Domain.multipleValues;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestKafkaFilterManager
{
    @Test
    public void testFilterValuesByDomain()
    {
        Set<Long> source = Set.of(1L, 2L, 3L, 4L, 5L, 6L);

        Domain testDomain = Domain.singleValue(BIGINT, 1L);
        assertEquals(KafkaFilterManager.filterValuesByDomain(testDomain, source), Set.of(1L));

        testDomain = multipleValues(BIGINT, ImmutableList.of(3L, 8L));
        assertEquals(KafkaFilterManager.filterValuesByDomain(testDomain, source), Set.of(3L));

        testDomain = Domain.create(SortedRangeSet.copyOf(BIGINT,
                ImmutableList.of(
                        Range.range(BIGINT, 2L, true, 4L, true))),
                false);

        assertEquals(KafkaFilterManager.filterValuesByDomain(testDomain, source), Set.of(2L, 3L, 4L));
    }

    @Test
    public void testFilterRangeByDomain()
    {
        Domain testDomain = Domain.singleValue(BIGINT, 1L);
        assertTrue(KafkaFilterManager.filterRangeByDomain(testDomain).isPresent());
        assertEquals(KafkaFilterManager.filterRangeByDomain(testDomain).get().getBegin(), 1L);
        assertEquals(KafkaFilterManager.filterRangeByDomain(testDomain).get().getEnd(), 2L);

        testDomain = multipleValues(BIGINT, ImmutableList.of(3L, 8L));
        assertTrue(KafkaFilterManager.filterRangeByDomain(testDomain).isPresent());
        assertEquals(KafkaFilterManager.filterRangeByDomain(testDomain).get().getBegin(), 3L);
        assertEquals(KafkaFilterManager.filterRangeByDomain(testDomain).get().getEnd(), 9L);

        testDomain = Domain.create(SortedRangeSet.copyOf(BIGINT,
                ImmutableList.of(
                        Range.range(BIGINT, 2L, true, 4L, true))),
                false);

        assertTrue(KafkaFilterManager.filterRangeByDomain(testDomain).isPresent());
        assertEquals(KafkaFilterManager.filterRangeByDomain(testDomain).get().getBegin(), 2L);
        assertEquals(KafkaFilterManager.filterRangeByDomain(testDomain).get().getEnd(), 5L);

        testDomain = Domain.create(SortedRangeSet.copyOf(TIMESTAMP_MILLIS,
                ImmutableList.of(Range.greaterThan(TIMESTAMP_MILLIS, 1642054805000000L))), false);
        assertEquals(testDomain.getValues().getRanges().getSpan().toString(), "(2022-01-13 06:20:05.000, <max>)");
        assertTrue(KafkaFilterManager.filterRangeByDomain(testDomain).isPresent());
        assertEquals(KafkaFilterManager.filterRangeByDomain(testDomain).get().getBegin(), 1642054805001000L);
        assertEquals(KafkaFilterManager.filterRangeByDomain(testDomain).get().getEnd(), Long.MAX_VALUE);

        testDomain = Domain.create(SortedRangeSet.copyOf(TIMESTAMP_MILLIS,
                ImmutableList.of(Range.lessThan(TIMESTAMP_MILLIS, 1642054805000000L))), false);
        assertEquals(testDomain.getValues().getRanges().getSpan().toString(), "(<min>, 2022-01-13 06:20:05.000)");
        assertTrue(KafkaFilterManager.filterRangeByDomain(testDomain).isPresent());
        assertEquals(KafkaFilterManager.filterRangeByDomain(testDomain).get().getBegin(), 0L);
        assertEquals(KafkaFilterManager.filterRangeByDomain(testDomain).get().getEnd(), 1642054804999001L);
    }
}
