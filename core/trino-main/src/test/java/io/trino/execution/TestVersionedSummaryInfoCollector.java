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
import io.trino.execution.VersionedSummaryInfoCollector.VersionedSummaryInfo;
import io.trino.spi.predicate.Domain;
import io.trino.sql.planner.plan.DynamicFilterId;
import org.testng.annotations.Test;

import static io.trino.execution.VersionedSummaryInfoCollector.INITIAL_SUMMARY_VERSION;
import static io.trino.spi.predicate.Domain.multipleValues;
import static io.trino.spi.predicate.Domain.singleValue;
import static io.trino.spi.type.BigintType.BIGINT;
import static org.testng.Assert.assertEquals;

public class TestVersionedSummaryInfoCollector
{
    @Test
    public void testDynamicFiltersCollection()
    {
        VersionedSummaryInfoCollector collector = new VersionedSummaryInfoCollector(() -> {});

        VersionedSummaryInfo versionedSummaryInfo = collector.acknowledgeAndGetNewSummaryInfo(INITIAL_SUMMARY_VERSION);

        // there should be no domains initially
        assertEquals(versionedSummaryInfo.getVersion(), INITIAL_SUMMARY_VERSION);
        assertEquals(versionedSummaryInfo.getSummaryInfo(), ImmutableList.of(new DynamicFilterSummary(ImmutableMap.of())));

        Domain initialDomain = multipleValues(BIGINT, ImmutableList.of(1L, 2L, 3L));
        DynamicFilterId filter = new DynamicFilterId("filter");
        DynamicFilterSummary dynamicFilterSummary = new DynamicFilterSummary(ImmutableMap.of(filter, initialDomain));
        collector.updateSummary(dynamicFilterSummary);

        versionedSummaryInfo = collector.acknowledgeAndGetNewSummaryInfo(INITIAL_SUMMARY_VERSION);
        assertEquals(versionedSummaryInfo.getVersion(), 1L);
        assertEquals(versionedSummaryInfo.getSummaryInfo(), ImmutableList.of(dynamicFilterSummary));

        // make sure domains are still available when requested again with old version
        versionedSummaryInfo = collector.acknowledgeAndGetNewSummaryInfo(INITIAL_SUMMARY_VERSION);
        assertEquals(versionedSummaryInfo.getVersion(), 1L);
        assertEquals(versionedSummaryInfo.getSummaryInfo(), ImmutableList.of(dynamicFilterSummary));

        // make sure domains are intersected
        collector.updateSummary(new DynamicFilterSummary(ImmutableMap.of(filter, multipleValues(BIGINT, ImmutableList.of(2L)))));
        collector.updateSummary(new DynamicFilterSummary(ImmutableMap.of(filter, multipleValues(BIGINT, ImmutableList.of(3L, 4L)))));

        versionedSummaryInfo = collector.acknowledgeAndGetNewSummaryInfo(1L);
        assertEquals(versionedSummaryInfo.getVersion(), 3L);
        assertEquals(
                versionedSummaryInfo.getSummaryInfo(),
                ImmutableList.of(new DynamicFilterSummary(ImmutableMap.of(filter, Domain.none(BIGINT)))));

        // make sure old domains are removed
        DynamicFilterId filter2 = new DynamicFilterId("filter2");
        dynamicFilterSummary = new DynamicFilterSummary(ImmutableMap.of(filter2, singleValue(BIGINT, 1L)));
        collector.updateSummary(dynamicFilterSummary);
        versionedSummaryInfo = collector.acknowledgeAndGetNewSummaryInfo(3L);
        assertEquals(versionedSummaryInfo.getVersion(), 4L);
        assertEquals(versionedSummaryInfo.getSummaryInfo(), ImmutableList.of(dynamicFilterSummary));

        // verify acknowledge removes old domains
        assertEquals(collector.getCurrentSummaryInfo(), versionedSummaryInfo);
        collector.acknowledge(4L);
        assertEquals(
                collector.getCurrentSummaryInfo(),
                new VersionedSummaryInfo(4L, ImmutableList.of(new DynamicFilterSummary(ImmutableMap.of()))));
    }
}
