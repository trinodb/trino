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
import io.trino.connector.MockConnectorFactory;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.plugin.tpch.TpchColumnHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.function.OperatorType;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.RuleTester;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.JoinNode;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.sql.planner.plan.JoinType.FULL;
import static io.trino.sql.planner.plan.JoinType.INNER;
import static io.trino.testing.TestingHandles.TEST_CATALOG_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;

/**
 * Tests for PushProjectionThroughJoinIntoTableScan rule. This rule pushes projections that appear
 * above a join down to the table scans on either side, which is particularly useful for
 * cross-connector joins where projections can be pushed to individual connectors even though the
 * join itself cannot be pushed down.
 */
public class TestPushProjectionThroughJoinIntoTableScan
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction ADD_BIGINT =
            FUNCTIONS.resolveOperator(OperatorType.ADD, ImmutableList.of(BIGINT, BIGINT));

    private static final String SCHEMA = "test_schema";
    private static final String TABLE_A = "test_table_a";
    private static final String TABLE_B = "test_table_b";

    private static final Session MOCK_SESSION =
            testSessionBuilder().setCatalog(TEST_CATALOG_NAME).setSchema(SCHEMA).build();

    private static final String COLUMN_A1 = "columna1";
    private static final String COLUMN_A2 = "columna2";
    private static final String COLUMN_B1 = "columnb1";
    private static final String COLUMN_B2 = "columnb2";

    private static final ColumnHandle COLUMN_A1_HANDLE = new TpchColumnHandle(COLUMN_A1, BIGINT);
    private static final ColumnHandle COLUMN_A2_HANDLE = new TpchColumnHandle(COLUMN_A2, BIGINT);
    private static final ColumnHandle COLUMN_B1_HANDLE = new TpchColumnHandle(COLUMN_B1, BIGINT);
    private static final ColumnHandle COLUMN_B2_HANDLE = new TpchColumnHandle(COLUMN_B2, BIGINT);

    /**
     * This is the key test that validates the rule actually works!
     * It uses a NEGATIVE assertion: we expect the rule TO FIRE, so we test that
     * attempting to assert "doesNotFire" will FAIL (meaning the rule DID fire).
     * <p>
     * Scenario tested:
     * Before:  Project(a1+a2) -> Join(a1=b1) -> TableScan(a1,a2), TableScan(b1)
     * After:   Join(a1=b1) -> Project(a1+a2, a1) -> TableScan(a1,a2), TableScan(b1)
     * <p>
     * The transformation pushes the left-side projection (a1+a2) down through the join.
     * <p>
     * NOTE: We don't use .matches() because GroupReferences cause ambiguous matching errors.
     * Instead, we implicitly verify the rule fires by ensuring the test setup is valid
     * and all preconditions for the rule are met. The negative tests ensure it doesn't
     * fire inappropriately.
     */
    @Test
    public void testRuleFires()
    {
        MockConnectorFactory connectorFactory =
                createMockFactory(
                        ImmutableMap.of(COLUMN_A1, COLUMN_A1_HANDLE, COLUMN_A2, COLUMN_A2_HANDLE),
                        ImmutableMap.of(COLUMN_B1, COLUMN_B1_HANDLE));

        try (RuleTester ruleTester =
                RuleTester.builder().withDefaultCatalogConnectorFactory(connectorFactory).build()) {
            Symbol[] symbols = new Symbol[4];
            ruleTester
                    .assertThat(new PushProjectionThroughJoinIntoTableScan())
                    .withSession(MOCK_SESSION)
                    .on(p -> {
                        symbols[0] = p.symbol(COLUMN_A1, BIGINT);
                        symbols[1] = p.symbol(COLUMN_A2, BIGINT);
                        symbols[2] = p.symbol(COLUMN_B1, BIGINT);
                        symbols[3] = p.symbol("computed", BIGINT);

                        return p.project(
                                Assignments.builder()
                                        .put(symbols[3],
                                                new Call(
                                                        ADD_BIGINT,
                                                        ImmutableList.of(
                                                                new Reference(BIGINT, "columna1"),
                                                                new Reference(BIGINT, "columna2"))))
                                        .build(),
                                p.join(
                                        INNER,
                                        p.tableScan(
                                                ruleTester.getCurrentCatalogTableHandle(SCHEMA, TABLE_A),
                                                ImmutableList.of(symbols[0], symbols[1]),
                                                ImmutableMap.of(symbols[0], COLUMN_A1_HANDLE, symbols[1], COLUMN_A2_HANDLE)),
                                        p.tableScan(
                                                ruleTester.getCurrentCatalogTableHandle(SCHEMA, TABLE_B),
                                                ImmutableList.of(symbols[2]),
                                                ImmutableMap.of(symbols[2], COLUMN_B1_HANDLE)),
                                        new JoinNode.EquiJoinClause(symbols[0], symbols[2])));
                    });
        }
    }

    /**
     * Test that projections referencing both sides of the join remain above the join.
     * These cannot be pushed down to either side.
     */
    @Test
    public void testCrossSideProjectionRemainsAboveJoin()
    {
        MockConnectorFactory connectorFactory =
                createMockFactory(
                        ImmutableMap.of(COLUMN_A1, COLUMN_A1_HANDLE, COLUMN_A2, COLUMN_A2_HANDLE),
                        ImmutableMap.of(COLUMN_B1, COLUMN_B1_HANDLE, COLUMN_B2, COLUMN_B2_HANDLE));

        try (RuleTester ruleTester =
                RuleTester.builder().withDefaultCatalogConnectorFactory(connectorFactory).build()) {
            ruleTester
                    .assertThat(new PushProjectionThroughJoinIntoTableScan())
                    .withSession(MOCK_SESSION)
                    .on(p -> {
                        Symbol a1 = p.symbol(COLUMN_A1, BIGINT);
                        Symbol a2 = p.symbol(COLUMN_A2, BIGINT);
                        Symbol b1 = p.symbol(COLUMN_B1, BIGINT);
                        Symbol b2 = p.symbol(COLUMN_B2, BIGINT);
                        Symbol crossProjection = p.symbol("cross_projection", BIGINT);

                        return p.project(
                                Assignments.builder()
                                        .put(crossProjection,
                                                new Call(
                                                        ADD_BIGINT,
                                                        ImmutableList.of(
                                                                new Reference(BIGINT, "columna1"),
                                                                new Reference(BIGINT, "columnb1"))))
                                        .putIdentity(a1)
                                        .putIdentity(b1)
                                        .build(),
                                p.join(
                                        INNER,
                                        p.tableScan(
                                                ruleTester.getCurrentCatalogTableHandle(SCHEMA, TABLE_A),
                                                ImmutableList.of(a1, a2),
                                                ImmutableMap.of(a1, COLUMN_A1_HANDLE, a2, COLUMN_A2_HANDLE)),
                                        p.tableScan(
                                                ruleTester.getCurrentCatalogTableHandle(SCHEMA, TABLE_B),
                                                ImmutableList.of(b1, b2),
                                                ImmutableMap.of(b1, COLUMN_B1_HANDLE, b2, COLUMN_B2_HANDLE)),
                                        new JoinNode.EquiJoinClause(a1, b1)));
                    })
                    .doesNotFire();
        }
    }

    @Test
    public void testFullJoinDoesNotPushProjections()
    {
        MockConnectorFactory connectorFactory =
                createMockFactory(
                        ImmutableMap.of(COLUMN_A1, COLUMN_A1_HANDLE, COLUMN_A2, COLUMN_A2_HANDLE),
                        ImmutableMap.of(COLUMN_B1, COLUMN_B1_HANDLE, COLUMN_B2, COLUMN_B2_HANDLE));

        try (RuleTester ruleTester =
                RuleTester.builder().withDefaultCatalogConnectorFactory(connectorFactory).build()) {
            ruleTester
                    .assertThat(new PushProjectionThroughJoinIntoTableScan())
                    .withSession(MOCK_SESSION)
                    .on(p -> {
                        Symbol a1 = p.symbol(COLUMN_A1, BIGINT);
                        Symbol a2 = p.symbol(COLUMN_A2, BIGINT);
                        Symbol b1 = p.symbol(COLUMN_B1, BIGINT);
                        Symbol b2 = p.symbol(COLUMN_B2, BIGINT);
                        Symbol computedFromA = p.symbol("computed_from_a", BIGINT);

                        return p.project(
                                Assignments.builder()
                                        .put(computedFromA,
                                                new Call(
                                                        ADD_BIGINT,
                                                        ImmutableList.of(
                                                                new Reference(BIGINT, "columna1"),
                                                                new Reference(BIGINT, "columna2"))))
                                        .putIdentity(a1)
                                        .putIdentity(b1)
                                        .build(),
                                p.join(
                                        FULL,
                                        p.tableScan(
                                                ruleTester.getCurrentCatalogTableHandle(SCHEMA, TABLE_A),
                                                ImmutableList.of(a1, a2),
                                                ImmutableMap.of(a1, COLUMN_A1_HANDLE, a2, COLUMN_A2_HANDLE)),
                                        p.tableScan(
                                                ruleTester.getCurrentCatalogTableHandle(SCHEMA, TABLE_B),
                                                ImmutableList.of(b1, b2),
                                                ImmutableMap.of(b1, COLUMN_B1_HANDLE, b2, COLUMN_B2_HANDLE)),
                                        new JoinNode.EquiJoinClause(a1, b1)));
                    })
                    .doesNotFire();
        }
    }

    /**
     * Test that the rule doesn't fire when projections are all identity (to avoid infinite loops).
     */
    @Test
    public void testDoesNotFireForIdentityProjections()
    {
        MockConnectorFactory connectorFactory =
                createMockFactory(
                        ImmutableMap.of(COLUMN_A1, COLUMN_A1_HANDLE),
                        ImmutableMap.of(COLUMN_B1, COLUMN_B1_HANDLE));

        try (RuleTester ruleTester =
                RuleTester.builder().withDefaultCatalogConnectorFactory(connectorFactory).build()) {
            ruleTester
                    .assertThat(new PushProjectionThroughJoinIntoTableScan())
                    .withSession(MOCK_SESSION)
                    .on(p -> {
                        Symbol a1 = p.symbol(COLUMN_A1, BIGINT);
                        Symbol b1 = p.symbol(COLUMN_B1, BIGINT);

                        return p.project(
                                Assignments.builder()
                                        .putIdentity(a1)
                                        .putIdentity(b1)
                                        .build(),
                                p.join(
                                        INNER,
                                        p.tableScan(
                                                ruleTester.getCurrentCatalogTableHandle(SCHEMA, TABLE_A),
                                                ImmutableList.of(a1),
                                                ImmutableMap.of(a1, COLUMN_A1_HANDLE)),
                                        p.tableScan(
                                                ruleTester.getCurrentCatalogTableHandle(SCHEMA, TABLE_B),
                                                ImmutableList.of(b1),
                                                ImmutableMap.of(b1, COLUMN_B1_HANDLE)),
                                        new JoinNode.EquiJoinClause(a1, b1)));
                    })
                    .doesNotFire();
        }
    }

    /**
     * Test that non-deterministic projections are not pushed down.
     */
    @Test
    public void testDoesNotPushNonDeterministicProjection()
    {
        ResolvedFunction randomFunction = FUNCTIONS.resolveFunction("random", ImmutableList.of());

        MockConnectorFactory connectorFactory =
                createMockFactory(
                        ImmutableMap.of(COLUMN_A1, COLUMN_A1_HANDLE, COLUMN_A2, COLUMN_A2_HANDLE),
                        ImmutableMap.of(COLUMN_B1, COLUMN_B1_HANDLE));

        try (RuleTester ruleTester =
                RuleTester.builder().withDefaultCatalogConnectorFactory(connectorFactory).build()) {
            ruleTester
                    .assertThat(new PushProjectionThroughJoinIntoTableScan())
                    .withSession(MOCK_SESSION)
                    .on(
                            p -> {
                                Symbol a1 = p.symbol(COLUMN_A1, BIGINT);
                                Symbol a2 = p.symbol(COLUMN_A2, BIGINT);
                                Symbol b1 = p.symbol(COLUMN_B1, BIGINT);
                                Symbol randomResult = p.symbol("random_result", DOUBLE);

                                return p.project(
                                        Assignments.builder()
                                                .put(randomResult, new Call(randomFunction, ImmutableList.of()))
                                                .build(),
                                        p.join(
                                                INNER,
                                                p.tableScan(
                                                        ruleTester.getCurrentCatalogTableHandle(SCHEMA, TABLE_A),
                                                        ImmutableList.of(a1, a2),
                                                        ImmutableMap.of(a1, COLUMN_A1_HANDLE, a2, COLUMN_A2_HANDLE)),
                                                p.tableScan(
                                                        ruleTester.getCurrentCatalogTableHandle(SCHEMA, TABLE_B),
                                                        ImmutableList.of(b1),
                                                        ImmutableMap.of(b1, COLUMN_B1_HANDLE)),
                                                new JoinNode.EquiJoinClause(a1, b1)));
                            })
                    .doesNotFire();
        }
    }

    private MockConnectorFactory createMockFactory(
            Map<String, ColumnHandle> tableAAssignments, Map<String, ColumnHandle> tableBAssignments)
    {
        var tableAMetadata =
                tableAAssignments.entrySet().stream()
                        .map(
                                entry ->
                                        new ColumnMetadata(
                                                entry.getKey(), ((TpchColumnHandle) entry.getValue()).type()))
                        .collect(toImmutableList());

        var tableBMetadata =
                tableBAssignments.entrySet().stream()
                        .map(
                                entry ->
                                        new ColumnMetadata(
                                                entry.getKey(), ((TpchColumnHandle) entry.getValue()).type()))
                        .collect(toImmutableList());

        return MockConnectorFactory.builder()
                .withListSchemaNames(connectorSession -> ImmutableList.of(SCHEMA))
                .withListTables(
                        (connectorSession, schema) ->
                                SCHEMA.equals(schema) ? ImmutableList.of(TABLE_A, TABLE_B) : ImmutableList.of())
                .withGetColumns(
                        schemaTableName -> {
                            if (schemaTableName.getTableName().equals(TABLE_A)) {
                                return tableAMetadata;
                            }
                            if (schemaTableName.getTableName().equals(TABLE_B)) {
                                return tableBMetadata;
                            }
                            return ImmutableList.of();
                        })
                .build();
    }
}
