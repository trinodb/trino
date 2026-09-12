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

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.operator.RetryPolicy;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import static io.trino.operator.RetryPolicy.NONE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@Execution(SAME_THREAD)
public class TestCoordinatorDynamicFiltering
        extends AbstractTestCoordinatorDynamicFiltering
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return DistributedQueryRunner.builder(getDefaultSession())
                .setExtraProperties(ImmutableMap.of(
                        "retry-policy", getRetryPolicy().name(),
                        // keep limits lower to test edge cases
                        "dynamic-filtering.partitioned.max-distinct-values-per-driver", "10",
                        "dynamic-filtering.max-distinct-values-per-driver", "10",
                        "dynamic-filtering.range-row-limit-per-driver", "2000",
                        "dynamic-filtering.partitioned.range-row-limit-per-driver", "500"))
                .build();
    }

    @Override
    protected RetryPolicy getRetryPolicy()
    {
        return NONE;
    }

    @Test
    public void testRuntimeConstraintQueryInfoStatistics()
    {
        var result = getDistributedQueryRunner().executeWithPlan(
                Session.builder(getSession())
                        .setSystemProperty("legacy_dynamic_filtering", "false")
                        .build(),
                "SELECT count(*) FROM tpch.tiny.lineitem JOIN tpch.tiny.supplier ON lineitem.suppkey = supplier.suppkey AND supplier.name = 'Supplier#000000001'");
        var statistics = getDistributedQueryRunner().getCoordinator().getQueryManager()
                .getFullQueryInfo(result.queryId())
                .getQueryStats()
                .getDynamicFiltersStats();

        assertThat(statistics.getTotalDynamicFilters()).isEqualTo(1);
        assertThat(statistics.getDynamicFiltersCompleted()).isEqualTo(1);
        assertThat(statistics.getDynamicFilterDomainStats()).singleElement().satisfies(domain -> {
            assertThat(domain.getDynamicFilterId().toString()).startsWith("join_");
            assertThat(domain.getCollectionDuration()).isPresent();
        });
    }
}
