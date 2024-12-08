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
package io.trino.plugin.base.cache;

import com.google.common.collect.ImmutableMap;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.cache.CacheColumnId;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.SortedRangeSet;
import io.trino.spi.predicate.ValueSet;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.base.cache.CacheUtils.normalizeTupleDomain;
import static io.trino.spi.block.BlockTestUtils.assertBlockEquals;
import static io.trino.spi.predicate.Domain.singleValue;
import static io.trino.spi.predicate.TupleDomain.none;
import static io.trino.spi.predicate.TupleDomain.withColumnDomains;
import static io.trino.spi.type.BigintType.BIGINT;
import static org.assertj.core.api.Assertions.assertThat;

public class TestCacheUtils
{
    @Test
    public void testNormalizeTupleDomainEmptyTupleDomain()
    {
        assertThat(none()).isSameAs(none());
    }

    @Test
    public void testNormalizeTupleDomainKeyOrder()
    {
        Optional<Map<CacheColumnId, Domain>> domains = normalizeTupleDomain(withColumnDomains(ImmutableMap.of(
                new CacheColumnId("col2"), singleValue(BIGINT, 1L),
                new CacheColumnId("col1"), singleValue(BIGINT, 2L))))
                .getDomains();
        assertThat(domains).isPresent();
        assertThat(domains.get()).containsExactlyEntriesOf(ImmutableMap.of(
                new CacheColumnId("col1"), singleValue(BIGINT, 2L),
                new CacheColumnId("col2"), singleValue(BIGINT, 1L)));
    }

    @Test
    public void testNormalizeTupleDomainSortedRanges()
    {
        SortedRangeSet values = (SortedRangeSet) ValueSet.of(BIGINT, 0L, -1L);
        SortedRangeSet normalizedValues = (SortedRangeSet) normalizeTupleDomain(withColumnDomains(ImmutableMap.of(
                new CacheColumnId("col1"), Domain.create(values, false))))
                .getDomains()
                .orElseThrow()
                .get(new CacheColumnId("col1"))
                .getValues();
        // make sure normalization preserves equality of TupleDomains
        assertThat(normalizedValues).isEqualTo(values);
        assertBlockEquals(BIGINT, normalizedValues.getSortedRanges(), values.getSortedRanges());
        assertThat(values.getSortedRanges()).isInstanceOf(DictionaryBlock.class);
        assertThat(normalizedValues.getSortedRanges()).isInstanceOf(LongArrayBlock.class);

        // further normalization shouldn't change SortedRangeSet underlying block
        SortedRangeSet doubleNormalizedValues = (SortedRangeSet) normalizeTupleDomain(withColumnDomains(ImmutableMap.of(
                new CacheColumnId("col1"), Domain.create(normalizedValues, false))))
                .getDomains()
                .orElseThrow()
                .get(new CacheColumnId("col1"))
                .getValues();
        assertThat(doubleNormalizedValues.getSortedRanges()).isInstanceOf(LongArrayBlock.class);
        assertBlockEquals(BIGINT, doubleNormalizedValues.getSortedRanges(), normalizedValues.getSortedRanges());
    }
}
