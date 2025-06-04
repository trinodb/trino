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
package io.trino.faulttolerant;

import com.google.common.collect.ImmutableMap;
import io.trino.execution.AbstractTestCoordinatorDynamicFiltering;
import io.trino.operator.RetryPolicy;
import io.trino.plugin.exchange.filesystem.FileSystemExchangePlugin;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.FaultTolerantExecutionConnectorTestHelper;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingMetadata;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.parallel.Execution;

import java.util.Map;
import java.util.Set;

import static io.trino.operator.RetryPolicy.TASK;
import static io.trino.spi.predicate.Range.range;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.OptimizerConfig.JoinDistributionType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@Execution(SAME_THREAD)
public class TestFaultTolerantExecutionDynamicFiltering
        extends AbstractTestCoordinatorDynamicFiltering
{
    private static final TestingMetadata.TestingColumnHandle PART_KEY_HANDLE = new TestingMetadata.TestingColumnHandle("partkey", 1, BIGINT);

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Map<String, String> exchangeManagerProperties = ImmutableMap.<String, String>builder()
                .put("exchange.base-directories", System.getProperty("java.io.tmpdir") + "/trino-local-file-system-exchange-manager")
                .buildOrThrow();

        return DistributedQueryRunner.builder(getDefaultSession())
                .setExtraProperties(FaultTolerantExecutionConnectorTestHelper.getExtraProperties())
                // keep limits lower to test edge cases
                .addExtraProperty("enable-large-dynamic-filters", "false")
                .addExtraProperty("dynamic-filtering.small.max-distinct-values-per-driver", "10")
                .addExtraProperty("dynamic-filtering.small.range-row-limit-per-driver", "100")
                .setAdditionalSetup(runner -> {
                    runner.installPlugin(new FileSystemExchangePlugin());
                    runner.loadExchangeManager("filesystem", exchangeManagerProperties);
                })
                .build();
    }

    @Override
    protected RetryPolicy getRetryPolicy()
    {
        return TASK;
    }

    // Tests with non-selective build side are overridden because moving dynamic filter collection to the build source side in task retry mode
    // results in each instance of DynamicFilterSourceOperator receiving fewer input rows. Therefore, testing max-distinct-values-per-driver
    // requires larger build side and the assertions on the collected domain are adjusted for multiple ranges instead of single range.
    @Override
    protected void testSemiJoinWithNonSelectiveBuildSide(JoinDistributionType joinDistributionType)
    {
        assertQueryDynamicFilters(
                noJoinReordering(joinDistributionType),
                "SELECT * FROM lineitem WHERE lineitem.partkey IN (SELECT part.partkey FROM tpch.tiny.part)",
                Set.of(PART_KEY_HANDLE),
                collectedDomain -> {
                    TupleDomain<ColumnHandle> expectedRange = TupleDomain.withColumnDomains(ImmutableMap.of(
                            PART_KEY_HANDLE,
                            Domain.create(ValueSet.ofRanges(range(BIGINT, 1L, true, 2000L, true)), false)));
                    // Collected domain is {[1,41], [42,82], [83,123], [124,164], ..., [1928,2000]}
                    assertThat(collectedDomain.simplify(2)).isEqualTo(expectedRange);
                    collectedDomain.getDomains().orElseThrow().values().forEach(domain -> assertThat(domain.isNullableDiscreteSet()).isFalse());
                    assertThat(collectedDomain.intersect(expectedRange)).isEqualTo(collectedDomain);
                });
    }

    @Override
    protected void testJoinWithNonSelectiveBuildSide(JoinDistributionType joinDistributionType)
    {
        assertQueryDynamicFilters(
                noJoinReordering(joinDistributionType),
                "SELECT * FROM lineitem l JOIN tpch.tiny.part p ON l.partkey = p.partkey",
                Set.of(PART_KEY_HANDLE),
                collectedDomain -> {
                    TupleDomain<ColumnHandle> expectedRange = TupleDomain.withColumnDomains(ImmutableMap.of(
                            PART_KEY_HANDLE,
                            Domain.create(ValueSet.ofRanges(range(BIGINT, 1L, true, 2000L, true)), false)));
                    // Collected domain is {[1,41], [42,82], [83,123], [124,164], ..., [1928,2000]}
                    assertThat(collectedDomain.simplify(2)).isEqualTo(expectedRange);
                    collectedDomain.getDomains().orElseThrow().values().forEach(domain -> assertThat(domain.isNullableDiscreteSet()).isFalse());
                    assertThat(collectedDomain.intersect(expectedRange)).isEqualTo(collectedDomain);
                });
    }

    @Override
    @Test
    @Timeout(30)
    public void testRightJoinWithNonSelectiveBuildSide()
    {
        assertQueryDynamicFilters(
                noJoinReordering(),
                "SELECT * FROM lineitem l RIGHT JOIN tpch.tiny.part p ON l.partkey = p.partkey",
                Set.of(PART_KEY_HANDLE),
                collectedDomain -> {
                    TupleDomain<ColumnHandle> expectedRange = TupleDomain.withColumnDomains(ImmutableMap.of(
                            PART_KEY_HANDLE,
                            Domain.create(ValueSet.ofRanges(range(BIGINT, 1L, true, 2000L, true)), false)));
                    // Collected domain is {[1,41], [42,82], [83,123], [124,164], ..., [1928,2000]}
                    assertThat(collectedDomain.simplify(2)).isEqualTo(expectedRange);
                    collectedDomain.getDomains().orElseThrow().values().forEach(domain -> assertThat(domain.isNullableDiscreteSet()).isFalse());
                    assertThat(collectedDomain.intersect(expectedRange)).isEqualTo(collectedDomain);
                });
    }
}
