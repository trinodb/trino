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
package io.trino.delta;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.cost.PlanNodeStatsEstimate;
import io.trino.cost.StatsProvider;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.Metadata;
import io.trino.spi.predicate.Domain;
import io.trino.sql.planner.Plan;
import io.trino.sql.planner.assertions.MatchResult;
import io.trino.sql.planner.assertions.Matcher;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.assertions.SymbolAliases;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.TableScanNode;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.Map;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.predicate.Domain.multipleValues;
import static io.trino.spi.predicate.Domain.notNull;
import static io.trino.spi.predicate.Domain.onlyNull;
import static io.trino.spi.predicate.Domain.singleValue;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.planner.assertions.MatchResult.NO_MATCH;
import static io.trino.sql.planner.assertions.MatchResult.match;
import static io.trino.sql.planner.assertions.PlanAssert.assertPlan;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.transaction.TransactionBuilder.transaction;
import static java.lang.String.format;

/**
 * Integrations tests for various optimization (such as filter pushdown, nested column project/filter pushdown etc)
 * that speed up reading data from Delta tables.
 */
public class TestDeltaScanOptimizations
        extends AbstractDeltaDistributedQueryTestBase
{
    @Test
    public void filterOnRegularColumn()
    {
        String tableName = "data-reader-primitives";
        String testQuery = format("SELECT as_int, as_string FROM \"%s\" WHERE as_int = 1", tableName);
        String expResultsQuery = "SELECT 1, cast('1' as varchar)";

        assertDeltaQueryOptimized(
                tableName,
                testQuery,
                expResultsQuery,
                ImmutableMap.of("as_int", singleValue(INTEGER, 1L)),
                Collections.emptyMap());
    }

    @Test
    public void filterOnPartitionColumn()
    {
        String tableName = "deltatbl-partition-prune";
        String testQuery = format("SELECT date, name, city, cnt FROM \"%s\" WHERE city in ('sh', 'sz')", tableName);
        String expResultsQuery = "SELECT * FROM VALUES('20180512', 'Jay', 'sh', 4),('20181212', 'Linda', 'sz', 8)";

        assertDeltaQueryOptimized(
                tableName,
                testQuery,
                expResultsQuery,
                ImmutableMap.of("city", multipleValues(VARCHAR, ImmutableList.of(utf8Slice("sh"), utf8Slice("sz")))),
                ImmutableMap.of("city", multipleValues(VARCHAR, ImmutableList.of(utf8Slice("sh"), utf8Slice("sz")))));
    }

    @Test
    public void filterOnMultiplePartitionColumns()
    {
        String tableName = "deltatbl-partition-prune";
        String testQuery =
                format("SELECT date, name, city, cnt FROM \"%s\" WHERE city in ('sh', 'sz') AND \"date\" = '20180512'", tableName);
        String expResultsQuery = "SELECT * FROM VALUES('20180512', 'Jay', 'sh', 4)";

        assertDeltaQueryOptimized(
                tableName,
                testQuery,
                expResultsQuery,
                ImmutableMap.of(
                        "city", multipleValues(VARCHAR, ImmutableList.of(utf8Slice("sh"), utf8Slice("sz"))),
                        "date", singleValue(VARCHAR, utf8Slice("20180512"))),
                ImmutableMap.of(
                        "city", multipleValues(VARCHAR, ImmutableList.of(utf8Slice("sh"), utf8Slice("sz"))),
                        "date", singleValue(VARCHAR, utf8Slice("20180512"))));
    }

    @Test
    public void filterOnPartitionColumnAndRegularColumns()
    {
        String tableName = "deltatbl-partition-prune";
        String testQuery = format("SELECT date, name, city, cnt FROM \"%s\" WHERE city in ('sh', 'sz') AND name = 'Linda'", tableName);
        String expResultsQuery = "SELECT * FROM VALUES('20181212', 'Linda', 'sz', 8)";

        assertDeltaQueryOptimized(
                tableName,
                testQuery,
                expResultsQuery,
                ImmutableMap.of(
                        "city", multipleValues(VARCHAR, ImmutableList.of(utf8Slice("sh"), utf8Slice("sz"))),
                        "name", singleValue(VARCHAR, utf8Slice("Linda"))),
                ImmutableMap.of("city", multipleValues(VARCHAR, ImmutableList.of(utf8Slice("sh"), utf8Slice("sz")))));
    }

    @Test
    public void nullPartitionFilter()
    {
        String tableName = "data-reader-partition-values";
        String testQuery =
                format("SELECT value, as_boolean FROM \"%s\" WHERE as_int is null and value is not null", tableName);
        String expResultsQuery = "SELECT * FROM VALUES('2', null)";

        assertDeltaQueryOptimized(
                tableName,
                testQuery,
                expResultsQuery,
                ImmutableMap.of(
                        "as_int", onlyNull(INTEGER),
                        "value", notNull(VARCHAR)),
                ImmutableMap.of("as_int", onlyNull(INTEGER)));
    }

    @Test
    public void notNullPartitionFilter()
    {
        String tableName = "data-reader-partition-values";
        String testQuery = format("SELECT value, as_boolean FROM \"%s\" WHERE as_int is not null and value = '1'", tableName);
        String expResultsQuery = "SELECT * FROM VALUES('1', false)";

        assertDeltaQueryOptimized(
                tableName,
                testQuery,
                expResultsQuery,
                ImmutableMap.of(
                        "as_int", notNull(INTEGER),
                        "value", singleValue(VARCHAR, utf8Slice("1"))),
                ImmutableMap.of("as_int", notNull(INTEGER)));
    }

    protected void assertDeltaQueryOptimized(
            String tableName,
            String testQuery,
            String expResultsQuery,
            Map<String, Domain> expectedConstraint,
            Map<String, Domain> expectedEnforcedConstraint)
    {
        try {
            registerDeltaTableInHMS(tableName, tableName);

            // verify the query returns correct results
            assertQuery(testQuery, expResultsQuery);

            // verify the plan contains filter pushed down into scan appropriately
            transaction(getQueryRunner().getTransactionManager(), getQueryRunner().getAccessControl())
                    .execute(getSession(), session -> {
                        Plan plan = getQueryRunner().createPlan(session, testQuery, WarningCollector.NOOP);
                        assertPlan(
                                session,
                                getQueryRunner().getMetadata(),
                                (node, sourceStats, lookup, ignore, types) -> PlanNodeStatsEstimate.unknown(),
                                plan,
                                anyTree(tableScanWithConstraints(
                                        tableName,
                                        expectedConstraint,
                                        expectedEnforcedConstraint)));
                    });
        }
        finally {
            unregisterDeltaTableInHMS(tableName);
        }
    }

    /**
     * Utility plan verification method that checks whether the table scan node has given constraint.
     */
    private static PlanMatchPattern tableScanWithConstraints(
            String tableName,
            Map<String, Domain> expectedConstraint,
            Map<String, Domain> expectedEnforcedConstraint)
    {
        return PlanMatchPattern.tableScan(tableName).with(new Matcher()
        {
            @Override
            public boolean shapeMatches(PlanNode node)
            {
                return node instanceof TableScanNode;
            }

            @Override
            public MatchResult detailMatches(PlanNode node, StatsProvider stats, Session session, Metadata metadata, SymbolAliases symbolAliases)
            {
                TableScanNode tableScan = (TableScanNode) node;
                Map<String, Domain> actualEnforcedConstraint = tableScan.getEnforcedConstraint()
                        .transformKeys(DeltaColumnHandle.class::cast)
                        .transformKeys(DeltaColumnHandle::getName)
                        .getDomains()
                        .get();
                DeltaTableHandle deltaTableHandle = (DeltaTableHandle) tableScan.getTable().getConnectorHandle();
                Map<String, Domain> actualConstraint = deltaTableHandle.getPredicate()
                        .transformKeys(DeltaColumnHandle::getName)
                        .getDomains()
                        .get();

                if (!expectedConstraint.equals(actualConstraint) || !expectedEnforcedConstraint.equals(actualEnforcedConstraint)) {
                    return NO_MATCH;
                }

                return match();
            }
        });
    }
}
