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
import static io.trino.tests.BogusType.BOGUS;
import static io.trino.transaction.TransactionBuilder.transaction;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestApplyTableScanRedirection
{
    private static final String MOCK_CATALOG = "mock_catalog";
    private static final String TEST_SCHEMA = "test_schema";
    private static final String TEST_TABLE = "test_table";
    private static final SchemaTableName SOURCE_TABLE = new SchemaTableName(TEST_SCHEMA, TEST_TABLE);
    private static final TableHandle TEST_TABLE_HANDLE = createTableHandle(new MockConnectorTableHandle(SOURCE_TABLE));

    private static final Session MOCK_SESSION = testSessionBuilder().setCatalog(MOCK_CATALOG).setSchema(TEST_SCHEMA).build();

    private static final String SOURCE_COLUMN_NAME_A = "source_col_a";
    private static final ColumnHandle SOURCE_COLUMN_HANDLE_A = new MockConnectorColumnHandle(SOURCE_COLUMN_NAME_A, VARCHAR);
    private static final String SOURCE_COLUMN_NAME_B = "source_col_b";
    private static final ColumnHandle SOURCE_COLUMN_HANDLE_B = new MockConnectorColumnHandle(SOURCE_COLUMN_NAME_B, VARCHAR);

    private static final SchemaTableName DESTINATION_TABLE = new SchemaTableName("target_schema", "target_table");
    private static final String DESTINATION_COLUMN_NAME_A = "destination_col_a";
    private static final ColumnHandle DESTINATION_COLUMN_HANDLE_A = new MockConnectorColumnHandle(DESTINATION_COLUMN_NAME_A, VARCHAR);
    private static final String DESTINATION_COLUMN_NAME_B = "destination_col_b";
    private static final ColumnHandle DESTINATION_COLUMN_HANDLE_B = new MockConnectorColumnHandle(DESTINATION_COLUMN_NAME_B, VARCHAR);
    private static final String DESTINATION_COLUMN_NAME_C = "destination_col_c";
    private static final ColumnHandle DESTINATION_COLUMN_HANDLE_C = new MockConnectorColumnHandle(DESTINATION_COLUMN_NAME_C, BIGINT);
    private static final String DESTINATION_COLUMN_NAME_D = "destination_col_d";

    private static TableHandle createTableHandle(ConnectorTableHandle tableHandle)
    {
        return new TableHandle(
                new CatalogName(MOCK_CATALOG),
                tableHandle,
                TestingTransactionHandle.create());
    }

    @Test
    public void testDoesNotFire()
    {
        try (RuleTester ruleTester = defaultRuleTester()) {
            MockConnectorFactory mockFactory = createMockFactory(Optional.empty());
            ruleTester.getQueryRunner().createCatalog(MOCK_CATALOG, mockFactory, ImmutableMap.of());

            ruleTester.assertThat(new ApplyTableScanRedirection(ruleTester.getPlannerContext()))
                    .on(p -> {
                        Symbol column = p.symbol(SOURCE_COLUMN_NAME_A, VARCHAR);
                        return p.tableScan(TEST_TABLE_HANDLE,
                                ImmutableList.of(column),
                                ImmutableMap.of(column, SOURCE_COLUMN_HANDLE_A));
                    })
                    .withSession(MOCK_SESSION)
                    .doesNotFire();
        }
    }

    @Test
    public void testDoesNotFireForDeleteTableScan()
    {
        try (RuleTester ruleTester = defaultRuleTester()) {
            // make the mock connector return a table scan on different table
            ApplyTableScanRedirect applyTableScanRedirect = getMockApplyRedirect(
                    ImmutableMap.of(SOURCE_COLUMN_HANDLE_A, DESTINATION_COLUMN_NAME_A));
            MockConnectorFactory mockFactory = createMockFactory(Optional.of(applyTableScanRedirect));

            ruleTester.getQueryRunner().createCatalog(MOCK_CATALOG, mockFactory, ImmutableMap.of());

            ruleTester.assertThat(new ApplyTableScanRedirection(ruleTester.getPlannerContext()))
                    .on(p -> {
                        Symbol column = p.symbol(SOURCE_COLUMN_NAME_A, VARCHAR);
                        return p.tableScan(TEST_TABLE_HANDLE,
                                ImmutableList.of(column),
                                ImmutableMap.of(column, SOURCE_COLUMN_HANDLE_A),
                                true);
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
                    ImmutableMap.of(SOURCE_COLUMN_HANDLE_A, DESTINATION_COLUMN_NAME_A));
            MockConnectorFactory mockFactory = createMockFactory(Optional.of(applyTableScanRedirect));
            ruleTester.getQueryRunner().createCatalog(MOCK_CATALOG, mockFactory, ImmutableMap.of());

            ruleTester.assertThat(new ApplyTableScanRedirection(ruleTester.getPlannerContext()))
                    .on(p -> p.values(p.symbol("a", BIGINT)))
                    .withSession(MOCK_SESSION)
                    .doesNotFire();
        }
    }

    @Test
    public void testMismatchedTypesWithCoercion()
    {
        try (RuleTester ruleTester = defaultRuleTester()) {
            // make the mock connector return a table scan on different table
            ApplyTableScanRedirect applyTableScanRedirect = getMockApplyRedirect(
                    ImmutableMap.of(SOURCE_COLUMN_HANDLE_A, DESTINATION_COLUMN_NAME_C));
            MockConnectorFactory mockFactory = createMockFactory(Optional.of(applyTableScanRedirect));

            LocalQueryRunner runner = ruleTester.getQueryRunner();
            runner.createCatalog(MOCK_CATALOG, mockFactory, ImmutableMap.of());

            ruleTester.assertThat(new ApplyTableScanRedirection(ruleTester.getPlannerContext()))
                    .on(p -> {
                        Symbol column = p.symbol(SOURCE_COLUMN_NAME_A, VARCHAR);
                        return p.tableScan(TEST_TABLE_HANDLE,
                                ImmutableList.of(column),
                                ImmutableMap.of(column, SOURCE_COLUMN_HANDLE_A));
                    })
                    .withSession(MOCK_SESSION)
                    .matches(
                            project(ImmutableMap.of("COL", expression("CAST(DEST_COL AS VARCHAR)")),
                                    tableScan(
                                            new MockConnectorTableHandle(DESTINATION_TABLE)::equals,
                                            TupleDomain.all(),
                                            ImmutableMap.of("DEST_COL", DESTINATION_COLUMN_HANDLE_C::equals))));
        }
    }

    @Test
    public void testMismatchedTypesWithMissingCoercion()
    {
        try (RuleTester ruleTester = defaultRuleTester()) {
            // make the mock connector return a table scan on different table
            ApplyTableScanRedirect applyTableScanRedirect = getMockApplyRedirect(
                    ImmutableMap.of(SOURCE_COLUMN_HANDLE_A, DESTINATION_COLUMN_NAME_D));
            MockConnectorFactory mockFactory = createMockFactory(Optional.of(applyTableScanRedirect));

            LocalQueryRunner runner = ruleTester.getQueryRunner();
            runner.createCatalog(MOCK_CATALOG, mockFactory, ImmutableMap.of());

            transaction(runner.getTransactionManager(), runner.getAccessControl())
                    .execute(MOCK_SESSION, session -> {
                        assertThatThrownBy(() -> runner.createPlan(session, "SELECT source_col_a FROM test_table", WarningCollector.NOOP))
                                .isInstanceOf(TrinoException.class)
                                .hasMessageMatching("Cast not possible from redirected column mock_catalog.target_schema.target_table.destination_col_d with type Bogus to source column .*mock_catalog.test_schema.test_table.*source_col_a.* with type: varchar");
                    });
        }
    }

    @Test
    public void testApplyTableScanRedirection()
    {
        try (RuleTester ruleTester = defaultRuleTester()) {
            // make the mock connector return a table scan on different table
            ApplyTableScanRedirect applyTableScanRedirect = getMockApplyRedirect(
                    ImmutableMap.of(SOURCE_COLUMN_HANDLE_A, DESTINATION_COLUMN_NAME_A));
            MockConnectorFactory mockFactory = createMockFactory(Optional.of(applyTableScanRedirect));

            ruleTester.getQueryRunner().createCatalog(MOCK_CATALOG, mockFactory, ImmutableMap.of());

            ruleTester.assertThat(new ApplyTableScanRedirection(ruleTester.getPlannerContext()))
                    .on(p -> {
                        Symbol column = p.symbol(SOURCE_COLUMN_NAME_A, VARCHAR);
                        return p.tableScan(TEST_TABLE_HANDLE,
                                ImmutableList.of(column),
                                ImmutableMap.of(column, SOURCE_COLUMN_HANDLE_A));
                    })
                    .withSession(MOCK_SESSION)
                    .matches(
                            tableScan(
                                    new MockConnectorTableHandle(DESTINATION_TABLE)::equals,
                                    TupleDomain.all(),
                                    ImmutableMap.of("DEST_COL", DESTINATION_COLUMN_HANDLE_A::equals)));
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
                            SOURCE_COLUMN_HANDLE_A, DESTINATION_COLUMN_NAME_A,
                            SOURCE_COLUMN_HANDLE_B, DESTINATION_COLUMN_NAME_B));
            MockConnectorFactory mockFactory = createMockFactory(Optional.of(applyTableScanRedirect));

            ruleTester.getQueryRunner().createCatalog(MOCK_CATALOG, mockFactory, ImmutableMap.of());

            ApplyTableScanRedirection applyTableScanRedirection = new ApplyTableScanRedirection(ruleTester.getPlannerContext());
            TupleDomain<ColumnHandle> constraint = TupleDomain.withColumnDomains(
                    ImmutableMap.of(SOURCE_COLUMN_HANDLE_A, singleValue(VARCHAR, utf8Slice("foo"))));
            ruleTester.assertThat(applyTableScanRedirection)
                    .on(p -> {
                        Symbol column = p.symbol(SOURCE_COLUMN_NAME_A, VARCHAR);
                        return p.tableScan(
                                createTableHandle(new MockConnectorTableHandle(SOURCE_TABLE, constraint, Optional.empty())),
                                ImmutableList.of(column),
                                ImmutableMap.of(column, SOURCE_COLUMN_HANDLE_A),
                                constraint);
                    })
                    .withSession(MOCK_SESSION)
                    .matches(
                            filter(
                                    "DEST_COL = VARCHAR 'foo'",
                                    tableScan(
                                            new MockConnectorTableHandle(DESTINATION_TABLE)::equals,
                                            TupleDomain.all(),
                                            ImmutableMap.of("DEST_COL", DESTINATION_COLUMN_HANDLE_A::equals))));

            ruleTester.assertThat(applyTableScanRedirection)
                    .on(p -> {
                        Symbol column = p.symbol(SOURCE_COLUMN_NAME_B, VARCHAR);
                        return p.tableScan(
                                createTableHandle(new MockConnectorTableHandle(SOURCE_TABLE, constraint, Optional.empty())),
                                ImmutableList.of(column),
                                ImmutableMap.of(column, SOURCE_COLUMN_HANDLE_B), // predicate on non-projected column
                                TupleDomain.all());
                    })
                    .withSession(MOCK_SESSION)
                    .matches(
                            project(
                                    ImmutableMap.of("expr", expression("DEST_COL_B")),
                                    filter(
                                            "DEST_COL_A = VARCHAR 'foo'",
                                            tableScan(
                                                    new MockConnectorTableHandle(DESTINATION_TABLE)::equals,
                                                    TupleDomain.all(),
                                                    ImmutableMap.of(
                                                            "DEST_COL_A", DESTINATION_COLUMN_HANDLE_A::equals,
                                                            "DEST_COL_B", DESTINATION_COLUMN_HANDLE_B::equals)))));
        }
    }

    private ApplyTableScanRedirect getMockApplyRedirect(Map<ColumnHandle, String> redirectionMapping)
    {
        return (ConnectorSession session, ConnectorTableHandle handle) -> Optional.of(
                new TableScanRedirectApplicationResult(
                        new CatalogSchemaTableName(MOCK_CATALOG, DESTINATION_TABLE),
                        redirectionMapping,
                        ((MockConnectorTableHandle) handle).getConstraint()
                                .transformKeys(MockConnectorColumnHandle.class::cast)
                                .transformKeys(redirectionMapping::get)));
    }

    private MockConnectorFactory createMockFactory(Optional<MockConnectorFactory.ApplyTableScanRedirect> applyTableScanRedirect)
    {
        MockConnectorFactory.Builder builder = MockConnectorFactory.builder()
                .withGetColumns(schemaTableName -> {
                    if (schemaTableName.equals(SOURCE_TABLE)) {
                        return ImmutableList.of(
                                new ColumnMetadata(SOURCE_COLUMN_NAME_A, VARCHAR),
                                new ColumnMetadata(SOURCE_COLUMN_NAME_B, VARCHAR));
                    }
                    else if (schemaTableName.equals(DESTINATION_TABLE)) {
                        return ImmutableList.of(
                                new ColumnMetadata(DESTINATION_COLUMN_NAME_A, VARCHAR),
                                new ColumnMetadata(DESTINATION_COLUMN_NAME_B, VARCHAR),
                                new ColumnMetadata(DESTINATION_COLUMN_NAME_C, BIGINT),
                                new ColumnMetadata(DESTINATION_COLUMN_NAME_D, BOGUS));
                    }
                    throw new IllegalArgumentException();
                });
        applyTableScanRedirect.ifPresent(builder::withApplyTableScanRedirect);

        return builder.build();
    }
}
