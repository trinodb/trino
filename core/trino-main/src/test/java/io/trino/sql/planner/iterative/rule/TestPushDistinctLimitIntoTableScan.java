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
package io.trino.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.connector.MockConnectorColumnHandle;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorFactory.ApplyAggregation;
import io.trino.connector.MockConnectorTableHandle;
import io.trino.metadata.TableHandle;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.AggregationApplicationResult;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.testing.LocalQueryRunner;
import io.trino.testing.TestingSession;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.assertions.PlanMatchPattern.limit;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.testing.TestingHandles.TEST_CATALOG_NAME;
import static java.util.stream.Collectors.toUnmodifiableList;
import static org.assertj.core.api.Assertions.assertThat;

@ResourceLock("TestPushDistinctLimitIntoTableScan")
public class TestPushDistinctLimitIntoTableScan
        extends BaseRuleTest
{
    private PushDistinctLimitIntoTableScan rule;
    private TableHandle tableHandle;

    private ApplyAggregation testApplyAggregation;

    @Override
    protected Optional<LocalQueryRunner> createLocalQueryRunner()
    {
        Session defaultSession = TestingSession.testSessionBuilder()
                .setCatalog(TEST_CATALOG_NAME)
                .setSchema("tiny")
                .build();

        LocalQueryRunner queryRunner = LocalQueryRunner.create(defaultSession);

        queryRunner.createCatalog(
                TEST_CATALOG_NAME,
                MockConnectorFactory.builder()
                        .withApplyAggregation(
                                (session, handle, aggregates, assignments, groupingSets) -> {
                                    if (testApplyAggregation != null) {
                                        return testApplyAggregation.apply(session, handle, aggregates, assignments, groupingSets);
                                    }
                                    return Optional.empty();
                                })
                        .build(),
                Map.of());

        return Optional.of(queryRunner);
    }

    @BeforeAll
    public void init()
    {
        rule = new PushDistinctLimitIntoTableScan(tester().getPlannerContext(), tester().getTypeAnalyzer());

        tableHandle = tester().getCurrentCatalogTableHandle("mock_schema", "mock_nation");
    }

    @Test
    public void testDoesNotFireIfNoTableScan()
    {
        testApplyAggregation = null;
        tester().assertThat(rule)
                .on(p -> p.values(p.symbol("a", BIGINT)))
                .doesNotFire();
    }

    @Test
    public void testNoEffect()
    {
        AtomicInteger applyCallCounter = new AtomicInteger();
        testApplyAggregation = (session, handle, aggregates, assignments, groupingSets) -> {
            applyCallCounter.incrementAndGet();
            return Optional.empty();
        };

        tester().assertThat(rule)
                .on(p -> {
                    Symbol regionkey = p.symbol("regionkey");
                    return p.distinctLimit(10, List.of(regionkey),
                            p.tableScan(
                                    tableHandle,
                                    ImmutableList.of(regionkey),
                                    ImmutableMap.of(regionkey, new MockConnectorColumnHandle("regionkey", BIGINT))));
                })
                .doesNotFire();

        assertThat(applyCallCounter).as("applyCallCounter").hasValue(1);
    }

    @Test
    public void testPushDistinct()
    {
        AtomicInteger applyCallCounter = new AtomicInteger();
        AtomicReference<List<AggregateFunction>> applyAggregates = new AtomicReference<>();
        AtomicReference<Map<String, ColumnHandle>> applyAssignments = new AtomicReference<>();
        AtomicReference<List<List<ColumnHandle>>> applyGroupingSets = new AtomicReference<>();

        testApplyAggregation = (session, handle, aggregates, assignments, groupingSets) -> {
            applyCallCounter.incrementAndGet();
            applyAggregates.set(List.copyOf(aggregates));
            applyAssignments.set(Map.copyOf(assignments));
            applyGroupingSets.set(groupingSets.stream()
                    .map(List::copyOf)
                    .collect(toUnmodifiableList()));

            return Optional.of(new AggregationApplicationResult<>(
                    new MockConnectorTableHandle(new SchemaTableName("mock_schema", "mock_nation_aggregated")),
                    List.of(),
                    List.of(),
                    Map.of(),
                    false));
        };

        MockConnectorColumnHandle regionkeyHandle = new MockConnectorColumnHandle("regionkey", BIGINT);
        tester().assertThat(rule)
                .on(p -> {
                    Symbol regionkeySymbol = p.symbol("regionkey_symbol");
                    return p.distinctLimit(43, List.of(regionkeySymbol),
                            p.tableScan(
                                    tableHandle,
                                    ImmutableList.of(regionkeySymbol),
                                    ImmutableMap.of(regionkeySymbol, regionkeyHandle)));
                })
                .matches(
                        limit(43,
                                project(
                                        tableScan("mock_nation_aggregated"))));

        assertThat(applyCallCounter).as("applyCallCounter").hasValue(1);
        assertThat(applyAggregates).as("applyAggregates").hasValue(List.of());
        assertThat(applyAssignments).as("applyAssignments").hasValue(Map.of("regionkey_symbol", regionkeyHandle));
        assertThat(applyGroupingSets).as("applyGroupingSets").hasValue(List.of(List.of(regionkeyHandle)));
    }
}
