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
package io.trino.execution;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.execution.DynamicFiltersCollector.VersionedDynamicFilterDomains;
import io.trino.spi.predicate.Domain;
import io.trino.sql.planner.plan.DynamicFilterId;
import org.junit.jupiter.api.Test;

import static io.trino.execution.DynamicFiltersCollector.INITIAL_DYNAMIC_FILTERS_VERSION;
import static io.trino.spi.predicate.Domain.multipleValues;
import static io.trino.spi.predicate.Domain.singleValue;
import static io.trino.spi.type.BigintType.BIGINT;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDynamicFiltersCollector
{
    @Test
    public void testDynamicFiltersCollector()
    {
        DynamicFilterId filter = new DynamicFilterId("filter");
        DynamicFiltersCollector collector = new DynamicFiltersCollector(() -> {});

        VersionedDynamicFilterDomains domains = collector.acknowledgeAndGetNewDomains(INITIAL_DYNAMIC_FILTERS_VERSION);

        // there should be no domains initially
        assertThat(domains.getVersion()).isEqualTo(INITIAL_DYNAMIC_FILTERS_VERSION);
        assertThat(domains.getDynamicFilterDomains()).isEqualTo(ImmutableMap.of());

        Domain initialDomain = multipleValues(BIGINT, ImmutableList.of(1L, 2L, 3L));
        collector.updateDomains(ImmutableMap.of(filter, initialDomain));

        domains = collector.acknowledgeAndGetNewDomains(INITIAL_DYNAMIC_FILTERS_VERSION);
        assertThat(domains.getVersion()).isEqualTo(1L);
        assertThat(domains.getDynamicFilterDomains()).isEqualTo(ImmutableMap.of(filter, initialDomain));

        // make sure domains are still available when requested again with old version
        domains = collector.acknowledgeAndGetNewDomains(INITIAL_DYNAMIC_FILTERS_VERSION);
        assertThat(domains.getVersion()).isEqualTo(1L);
        assertThat(domains.getDynamicFilterDomains()).isEqualTo(ImmutableMap.of(filter, initialDomain));

        // make sure domains are intersected
        collector.updateDomains(ImmutableMap.of(filter, multipleValues(BIGINT, ImmutableList.of(2L))));
        collector.updateDomains(ImmutableMap.of(filter, multipleValues(BIGINT, ImmutableList.of(3L, 4L))));

        domains = collector.acknowledgeAndGetNewDomains(1L);
        assertThat(domains.getVersion()).isEqualTo(3L);
        assertThat(domains.getDynamicFilterDomains()).isEqualTo(ImmutableMap.of(filter, Domain.none(BIGINT)));

        // make sure old domains are removed
        DynamicFilterId filter2 = new DynamicFilterId("filter2");
        collector.updateDomains(ImmutableMap.of(filter2, singleValue(BIGINT, 1L)));
        domains = collector.acknowledgeAndGetNewDomains(3L);
        assertThat(domains.getVersion()).isEqualTo(4L);
        assertThat(domains.getDynamicFilterDomains()).isEqualTo(ImmutableMap.of(filter2, singleValue(BIGINT, 1L)));
    }
}
