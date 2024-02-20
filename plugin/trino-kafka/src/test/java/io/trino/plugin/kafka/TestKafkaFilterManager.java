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
import org.junit.jupiter.api.Test;

import java.util.Set;

import static io.trino.spi.predicate.Domain.multipleValues;
import static io.trino.spi.type.BigintType.BIGINT;
import static org.assertj.core.api.Assertions.assertThat;

public class TestKafkaFilterManager
{
    @Test
    public void testFilterValuesByDomain()
    {
        Set<Long> source = Set.of(1L, 2L, 3L, 4L, 5L, 6L);

        Domain testDomain = Domain.singleValue(BIGINT, 1L);
        assertThat(KafkaFilterManager.filterValuesByDomain(testDomain, source)).isEqualTo(Set.of(1L));

        testDomain = multipleValues(BIGINT, ImmutableList.of(3L, 8L));
        assertThat(KafkaFilterManager.filterValuesByDomain(testDomain, source)).isEqualTo(Set.of(3L));

        testDomain = Domain.create(SortedRangeSet.copyOf(BIGINT,
                ImmutableList.of(
                        Range.range(BIGINT, 2L, true, 4L, true))),
                false);

        assertThat(KafkaFilterManager.filterValuesByDomain(testDomain, source)).isEqualTo(Set.of(2L, 3L, 4L));
    }

    @Test
    public void testFilterRangeByDomain()
    {
        Domain testDomain = Domain.singleValue(BIGINT, 1L);
        assertThat(KafkaFilterManager.filterRangeByDomain(testDomain).isPresent()).isTrue();
        assertThat(KafkaFilterManager.filterRangeByDomain(testDomain).get().getBegin()).isEqualTo(1L);
        assertThat(KafkaFilterManager.filterRangeByDomain(testDomain).get().getEnd()).isEqualTo(2L);

        testDomain = multipleValues(BIGINT, ImmutableList.of(3L, 8L));
        assertThat(KafkaFilterManager.filterRangeByDomain(testDomain).isPresent()).isTrue();
        assertThat(KafkaFilterManager.filterRangeByDomain(testDomain).get().getBegin()).isEqualTo(3L);
        assertThat(KafkaFilterManager.filterRangeByDomain(testDomain).get().getEnd()).isEqualTo(9L);

        testDomain = Domain.create(SortedRangeSet.copyOf(BIGINT,
                ImmutableList.of(
                        Range.range(BIGINT, 2L, true, 4L, true))),
                false);

        assertThat(KafkaFilterManager.filterRangeByDomain(testDomain).isPresent()).isTrue();
        assertThat(KafkaFilterManager.filterRangeByDomain(testDomain).get().getBegin()).isEqualTo(2L);
        assertThat(KafkaFilterManager.filterRangeByDomain(testDomain).get().getEnd()).isEqualTo(5L);
    }
}
