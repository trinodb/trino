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
package io.prestosql.execution;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.execution.DynamicFiltersCollector.VersionedDomain;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.sql.planner.plan.DynamicFilterId;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;

import static io.prestosql.execution.DynamicFiltersCollector.IGNORED_DOMAIN;
import static io.prestosql.execution.DynamicFiltersCollector.IGNORED_DYNAMIC_FILTER_VERSION;
import static io.prestosql.execution.DynamicFiltersCollector.INITIAL_DYNAMIC_FILTER_VERSION;
import static io.prestosql.execution.DynamicFiltersCollector.acknowledgeIgnoredDomains;
import static io.prestosql.spi.predicate.Domain.multipleValues;
import static io.prestosql.spi.predicate.Domain.singleValue;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestDynamicFiltersCollector
{
    @Test
    public void testDynamicFiltersCollector()
    {
        DynamicFilterId filter = new DynamicFilterId("filter");
        DynamicFiltersCollector collector = new DynamicFiltersCollector(() -> {});

        Map<DynamicFilterId, VersionedDomain> domains = collector.acknowledgeAndGetNewDomains(ImmutableMap.of(filter, INITIAL_DYNAMIC_FILTER_VERSION));

        // there should be no domains initially
        assertTrue(domains.isEmpty());

        Domain initialDomain = multipleValues(BIGINT, ImmutableList.of(1L, 2L, 3L));
        collector.updateDomains(ImmutableMap.of(filter, initialDomain));

        domains = collector.acknowledgeAndGetNewDomains(ImmutableMap.of(filter, INITIAL_DYNAMIC_FILTER_VERSION));
        assertEquals(domains, ImmutableMap.of(filter, new VersionedDomain(1L, Optional.of(initialDomain))));

        // make sure domains are still available when requested again with old version
        domains = collector.acknowledgeAndGetNewDomains(ImmutableMap.of(filter, INITIAL_DYNAMIC_FILTER_VERSION));
        assertEquals(domains, ImmutableMap.of(filter, new VersionedDomain(1L, Optional.of(initialDomain))));

        // make sure domains are intersected
        collector.updateDomains(ImmutableMap.of(filter, multipleValues(BIGINT, ImmutableList.of(2L))));
        collector.updateDomains(ImmutableMap.of(filter, multipleValues(BIGINT, ImmutableList.of(3L, 4L))));

        domains = collector.acknowledgeAndGetNewDomains(ImmutableMap.of(filter, 1L));
        assertEquals(domains, ImmutableMap.of(filter, new VersionedDomain(3L, Optional.of(Domain.none(BIGINT)))));

        // make sure old domains are removed
        DynamicFilterId filter2 = new DynamicFilterId("filter2");
        collector.updateDomains(ImmutableMap.of(filter2, singleValue(BIGINT, 1L)));
        domains = collector.acknowledgeAndGetNewDomains(ImmutableMap.of(filter, 3L, filter2, INITIAL_DYNAMIC_FILTER_VERSION));
        assertEquals(domains, ImmutableMap.of(filter2, new VersionedDomain(4L, Optional.of(singleValue(BIGINT, 1L)))));
    }

    @Test
    public void testIgnoredDynamicFilter()
    {
        DynamicFilterId filter = new DynamicFilterId("filter");
        DynamicFiltersCollector collector = new DynamicFiltersCollector(() -> {});

        collector.updateDomains(ImmutableMap.of(filter, multipleValues(BIGINT, ImmutableList.of(1L, 2L))));

        // make sure filter is available
        Map<DynamicFilterId, VersionedDomain> domains = collector.acknowledgeAndGetNewDomains(ImmutableMap.of(filter, INITIAL_DYNAMIC_FILTER_VERSION));
        assertEquals(domains, ImmutableMap.of(filter, new VersionedDomain(1L, Optional.of(multipleValues(BIGINT, ImmutableList.of(1L, 2L))))));

        // ignore filter
        domains = collector.acknowledgeAndGetNewDomains(ImmutableMap.of(filter, IGNORED_DYNAMIC_FILTER_VERSION));
        assertEquals(domains, ImmutableMap.of(filter, IGNORED_DOMAIN));
        assertEquals(collector.getDynamicFilterVersions(), ImmutableMap.of(filter, IGNORED_DYNAMIC_FILTER_VERSION));

        collector.updateDomains(ImmutableMap.of(filter, singleValue(BIGINT, 1L)));
        assertEquals(collector.getDynamicFilterVersions(), ImmutableMap.of(filter, IGNORED_DYNAMIC_FILTER_VERSION));
    }

    @Test
    public void testAcknowledgeIgnoredDomains()
    {
        DynamicFilterId filter = new DynamicFilterId("filter");
        assertEquals(acknowledgeIgnoredDomains(ImmutableMap.of(filter, IGNORED_DYNAMIC_FILTER_VERSION)), ImmutableMap.of(filter, IGNORED_DOMAIN));
    }
}
