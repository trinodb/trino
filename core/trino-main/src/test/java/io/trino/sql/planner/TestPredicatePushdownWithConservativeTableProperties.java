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
package io.trino.sql.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.connector.MockConnectorColumnHandle;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorTableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.testing.PlanTester;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.List;
import java.util.Optional;
import java.util.stream.LongStream;

import static io.trino.SystemSessionProperties.ENABLE_DYNAMIC_FILTERING;
import static io.trino.SystemSessionProperties.ITERATIVE_OPTIMIZER_TIMEOUT;
import static io.trino.SystemSessionProperties.ITERATIVE_PREDICATE_PUSHDOWN_ENABLED;
import static io.trino.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static io.trino.SystemSessionProperties.PREDICATE_PUSHDOWN_USE_TABLE_PROPERTIES;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;

public class TestPredicatePushdownWithConservativeTableProperties
        extends BasePlanTest
{
    private static final ColumnHandle COLUMN = new MockConnectorColumnHandle("c", BIGINT);

    @Override
    protected PlanTester createPlanTester()
    {
        PlanTester planTester = PlanTester.create(testSessionBuilder()
                .setCatalog("mock")
                .setSchema("default")
                .setSystemProperty(ENABLE_DYNAMIC_FILTERING, "false")
                .setSystemProperty(JOIN_REORDERING_STRATEGY, "NONE")
                .setSystemProperty(ITERATIVE_OPTIMIZER_TIMEOUT, "3s")
                .build());
        planTester.createCatalog("mock", MockConnectorFactory.builder()
                .withGetColumns(_ -> ImmutableList.of(new ColumnMetadata("c", BIGINT)))
                .withApplyFilter((_, table, constraint) -> {
                    MockConnectorTableHandle handle = (MockConnectorTableHandle) table;
                    TupleDomain<ColumnHandle> enforced = handle.getConstraint().intersect(constraint.getSummary());
                    if (!handle.getTableName().getTableName().equals("r") || enforced.equals(handle.getConstraint())) {
                        return Optional.empty();
                    }
                    return Optional.of(new ConstraintApplicationResult<>(
                            new MockConnectorTableHandle(handle.getTableName(), enforced, handle.getColumns()),
                            TupleDomain.all(),
                            constraint.getExpression(),
                            false));
                })
                // A connector can enforce a pushed filter without advertising it in table properties.
                .withGetTableProperties((_, _) -> new ConnectorTableProperties())
                .build(), ImmutableMap.of());
        return planTester;
    }

    @ParameterizedTest
    @CsvSource({
            "true, true, false",
            "true, false, false",
            "false, true, false",
            "false, false, false",
            "true, true, true",
            "true, false, true",
            "false, true, true",
            "false, false, true",
    })
    public void testEnforcedJoinPredicateIsNotReinserted(boolean iterativePredicatePushdown, boolean useTableProperties, boolean discreteDomain)
    {
        Session session = Session.builder(getPlanTester().getDefaultSession())
                .setSystemProperty(ITERATIVE_PREDICATE_PUSHDOWN_ENABLED, Boolean.toString(iterativePredicatePushdown))
                .setSystemProperty(PREDICATE_PUSHDOWN_USE_TABLE_PROPERTIES, Boolean.toString(useTableProperties))
                .build();
        List<Long> values = LongStream.range(0, 65).map(value -> value * 2).boxed().toList();
        String predicate = discreteDomain ? "l.c IN (" + values.stream().map(Object::toString).collect(joining(", ")) + ")" : "l.c > 10";
        Domain expectedDomain = discreteDomain ? Domain.multipleValues(BIGINT, values) : Domain.create(ValueSet.ofRanges(Range.greaterThan(BIGINT, 10L)), false);
        Plan plan = getPlanTester().inTransaction(session, transactionSession -> getPlanTester().createPlan(
                transactionSession,
                "SELECT l.c, r.c FROM l JOIN r ON l.c = r.c WHERE " + predicate));
        TableScanNode scan = (TableScanNode) searchFrom(plan.getRoot())
                .where(node -> node instanceof TableScanNode tableScan &&
                        ((MockConnectorTableHandle) tableScan.getTable().connectorHandle()).getTableName().getTableName().equals("r"))
                .findOnlyElement();
        TupleDomain<ColumnHandle> expectedConstraint = TupleDomain.withColumnDomains(ImmutableMap.of(COLUMN, expectedDomain));
        assertThat(scan.getEnforcedConstraint()).isEqualTo(expectedConstraint);
        assertThat(((MockConnectorTableHandle) scan.getTable().connectorHandle()).getConstraint()).isEqualTo(expectedConstraint);
    }
}
