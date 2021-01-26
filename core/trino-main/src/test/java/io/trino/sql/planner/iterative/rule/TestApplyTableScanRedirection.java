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
import io.trino.connector.CatalogName;
import io.trino.connector.MockConnectorColumnHandle;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorTableHandle;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.TableHandle;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableScanRedirectApplicationResult;
import io.trino.spi.predicate.TupleDomain;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.RuleTester;
import io.trino.testing.LocalQueryRunner;
import io.trino.testing.TestingTransactionHandle;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Predicates.equalTo;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.connector.MockConnectorFactory.ApplyTableScanRedirect;
import static io.trino.spi.predicate.Domain.singleValue;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.sql.planner.iterative.rule.test.RuleTester.defaultRuleTester;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.transaction.TransactionBuilder.transaction;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestApplyTableScanRedirection
{
    private static final String MOCK_CATALOG = "mock_catalog";
    private static final String TEST_SCHEMA = "test_schema";
    private static final String TEST_TABLE = "test_table";
    private static final SchemaTableName sourceTable = new SchemaTableName(TEST_SCHEMA, TEST_TABLE);
    private static final TableHandle TEST_TABLE_HANDLE = createTableHandle(new MockConnectorTableHandle(sourceTable));

    private static final Session MOCK_SESSION = testSessionBuilder().setCatalog(MOCK_CATALOG).setSchema(TEST_SCHEMA).build();

    private static final String sourceColumnNameA = "source_col_a";
    private static final ColumnHandle sourceColumnHandleA = new MockConnectorColumnHandle(sourceColumnNameA, VARCHAR);
    private static final String sourceColumnNameB = "source_col_b";
    private static final ColumnHandle sourceColumnHandleB = new MockConnectorColumnHandle(sourceColumnNameB, VARCHAR);

    private static final SchemaTableName destinationTable = new SchemaTableName("target_schema", "target_table");
    private static final String destinationColumnNameA = "destination_col_a";
    private static final ColumnHandle destinationColumnHandleA = new MockConnectorColumnHandle(destinationColumnNameA, VARCHAR);
    private static final String destinationColumnNameB = "destination_col_b";
    private static final ColumnHandle destinationColumnHandleB = new MockConnectorColumnHandle(destinationColumnNameB, VARCHAR);
    private static final String destinationColumnNameC = "destination_col_c";

    private static TableHandle createTableHandle(ConnectorTableHandle tableHandle)
    {
        return new TableHandle(
                new CatalogName(MOCK_CATALOG),
                tableHandle,
                TestingTransactionHandle.create(),
                Optional.empty());
    }

    @Test
    public void testDoesNotFire()
    {
        try (RuleTester ruleTester = defaultRuleTester()) {
            MockConnectorFactory mockFactory = createMockFactory(Optional.empty());
            ruleTester.getQueryRunner().createCatalog(MOCK_CATALOG, mockFactory, ImmutableMap.of());

            ruleTester.assertThat(new ApplyTableScanRedirection(ruleTester.getMetadata()))
                    .on(p -> {
                        Symbol column = p.symbol(sourceColumnNameA, VARCHAR);
                        return p.tableScan(TEST_TABLE_HANDLE,
                                ImmutableList.of(column),
                                ImmutableMap.of(column, sourceColumnHandleA));
                    })
                    .withSession(MOCK_SESSION)
                    .doesNotFire();
        }
    }

    @Test
    public void doesNotFireIfNoTableScan()
    {
        try (RuleTester ruleTester = defaultRuleTester()) {
            ApplyTableScanRedirect applyTableScanRedirect = getMockApplyRedirect(
                    ImmutableMap.of(sourceColumnHandleA, destinationColumnNameA));
            MockConnectorFactory mockFactory = createMockFactory(Optional.of(applyTableScanRedirect));
            ruleTester.getQueryRunner().createCatalog(MOCK_CATALOG, mockFactory, ImmutableMap.of());

            ruleTester.assertThat(new ApplyTableScanRedirection(ruleTester.getMetadata()))
                    .on(p -> p.values(p.symbol("a", BIGINT)))
                    .withSession(MOCK_SESSION)
                    .doesNotFire();
        }
    }

    @Test
    public void testMismatchedTypes()
    {
        try (RuleTester ruleTester = defaultRuleTester()) {
            // make the mock connector return a table scan on different table
            ApplyTableScanRedirect applyTableScanRedirect = getMockApplyRedirect(
                    ImmutableMap.of(sourceColumnHandleA, destinationColumnNameC));
            MockConnectorFactory mockFactory = createMockFactory(Optional.of(applyTableScanRedirect));

            LocalQueryRunner runner = ruleTester.getQueryRunner();
            runner.createCatalog(MOCK_CATALOG, mockFactory, ImmutableMap.of());

            transaction(runner.getTransactionManager(), runner.getAccessControl())
                    .execute(MOCK_SESSION, session -> {
                        assertThatThrownBy(() -> runner.createPlan(session, "SELECT source_col_a FROM test_table", WarningCollector.NOOP))
                                .isInstanceOf(TrinoException.class)
                                .hasMessageMatching("Redirected column mock_catalog.target_schema.target_table.destination_col_c has type bigint, different from source column .*MockConnectorTableHandle.*source_col_a.* type: varchar");
                    });
        }
    }

    @Test
    public void testApplyTableScanRedirection()
    {
        try (RuleTester ruleTester = defaultRuleTester()) {
            // make the mock connector return a table scan on different table
            ApplyTableScanRedirect applyTableScanRedirect = getMockApplyRedirect(
                    ImmutableMap.of(sourceColumnHandleA, destinationColumnNameA));
            MockConnectorFactory mockFactory = createMockFactory(Optional.of(applyTableScanRedirect));

            ruleTester.getQueryRunner().createCatalog(MOCK_CATALOG, mockFactory, ImmutableMap.of());

            ruleTester.assertThat(new ApplyTableScanRedirection(ruleTester.getMetadata()))
                    .on(p -> {
                        Symbol column = p.symbol(sourceColumnNameA, VARCHAR);
                        return p.tableScan(TEST_TABLE_HANDLE,
                                ImmutableList.of(column),
                                ImmutableMap.of(column, sourceColumnHandleA));
                    })
                    .withSession(MOCK_SESSION)
                    .matches(
                            tableScan(
                                    equalTo(new MockConnectorTableHandle(destinationTable)),
                                    TupleDomain.all(),
                                    ImmutableMap.of("DEST_COL", equalTo(destinationColumnHandleA))));
        }
    }

    @Test
    public void testApplyTableScanRedirectionWithFilter()
    {
        try (RuleTester ruleTester = defaultRuleTester()) {
            // make the mock connector return a table scan on different table
            // source table handle has a pushed down predicate
            ApplyTableScanRedirect applyTableScanRedirect = getMockApplyRedirect(
                    ImmutableMap.of(
                            sourceColumnHandleA, destinationColumnNameA,
                            sourceColumnHandleB, destinationColumnNameB));
            MockConnectorFactory mockFactory = createMockFactory(Optional.of(applyTableScanRedirect));

            ruleTester.getQueryRunner().createCatalog(MOCK_CATALOG, mockFactory, ImmutableMap.of());

            ApplyTableScanRedirection applyTableScanRedirection = new ApplyTableScanRedirection(ruleTester.getMetadata());
            TupleDomain<ColumnHandle> constraint = TupleDomain.withColumnDomains(
                    ImmutableMap.of(sourceColumnHandleA, singleValue(VARCHAR, utf8Slice("foo"))));
            ruleTester.assertThat(applyTableScanRedirection)
                    .on(p -> {
                        Symbol column = p.symbol(sourceColumnNameA, VARCHAR);
                        return p.tableScan(
                                createTableHandle(new MockConnectorTableHandle(sourceTable, constraint, Optional.empty())),
                                ImmutableList.of(column),
                                ImmutableMap.of(column, sourceColumnHandleA),
                                constraint);
                    })
                    .withSession(MOCK_SESSION)
                    .matches(
                            filter(
                                    "DEST_COL = CAST('foo' AS varchar)",
                                    tableScan(
                                            equalTo(new MockConnectorTableHandle(destinationTable)),
                                            TupleDomain.all(),
                                            ImmutableMap.of("DEST_COL", equalTo(destinationColumnHandleA)))));

            ruleTester.assertThat(applyTableScanRedirection)
                    .on(p -> {
                        Symbol column = p.symbol(sourceColumnNameB, VARCHAR);
                        return p.tableScan(
                                createTableHandle(new MockConnectorTableHandle(sourceTable, constraint, Optional.empty())),
                                ImmutableList.of(column),
                                ImmutableMap.of(column, sourceColumnHandleB), // predicate on non-projected column
                                constraint);
                    })
                    .withSession(MOCK_SESSION)
                    .matches(
                            project(
                                    ImmutableMap.of("expr", expression("DEST_COL_B")),
                                    filter(
                                            "DEST_COL_A = CAST('foo' AS varchar)",
                                            tableScan(
                                                    equalTo(new MockConnectorTableHandle(destinationTable)),
                                                    TupleDomain.all(),
                                                    ImmutableMap.of(
                                                            "DEST_COL_A", equalTo(destinationColumnHandleA),
                                                            "DEST_COL_B", equalTo(destinationColumnHandleB))))));
        }
    }

    private ApplyTableScanRedirect getMockApplyRedirect(Map<ColumnHandle, String> redirectionMapping)
    {
        return (ConnectorSession session, ConnectorTableHandle handle) -> Optional.of(
                new TableScanRedirectApplicationResult(
                        new CatalogSchemaTableName(MOCK_CATALOG, destinationTable),
                        redirectionMapping,
                        ((MockConnectorTableHandle) handle).getConstraint()
                                .transform(MockConnectorColumnHandle.class::cast)
                                .transform(redirectionMapping::get)));
    }

    private MockConnectorFactory createMockFactory(Optional<MockConnectorFactory.ApplyTableScanRedirect> applyTableScanRedirect)
    {
        MockConnectorFactory.Builder builder = MockConnectorFactory.builder()
                .withGetColumns(schemaTableName -> {
                    if (schemaTableName.equals(sourceTable)) {
                        return ImmutableList.of(
                                new ColumnMetadata(sourceColumnNameA, VARCHAR),
                                new ColumnMetadata(sourceColumnNameB, VARCHAR));
                    }
                    else if (schemaTableName.equals(destinationTable)) {
                        return ImmutableList.of(
                                new ColumnMetadata(destinationColumnNameA, VARCHAR),
                                new ColumnMetadata(destinationColumnNameB, VARCHAR),
                                new ColumnMetadata(destinationColumnNameC, BIGINT));
                    }
                    throw new IllegalArgumentException();
                });
        applyTableScanRedirect.ifPresent(builder::withApplyTableScanRedirect);

        return builder.build();
    }
}
