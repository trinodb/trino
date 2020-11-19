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
package io.prestosql.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.Session;
import io.prestosql.connector.CatalogName;
import io.prestosql.connector.MockConnectorColumnHandle;
import io.prestosql.connector.MockConnectorFactory;
import io.prestosql.connector.MockConnectorTableHandle;
import io.prestosql.metadata.TableHandle;
import io.prestosql.spi.connector.CatalogSchemaTableName;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.TableScanRedirectApplicationResult;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.iterative.rule.test.RuleTester;
import io.prestosql.testing.TestingTransactionHandle;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Predicates.equalTo;
import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.connector.MockConnectorFactory.ApplyTableScanRedirect;
import static io.prestosql.spi.predicate.Domain.singleValue;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.expression;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.filter;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.project;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.prestosql.sql.planner.iterative.rule.test.RuleTester.defaultRuleTester;
import static io.prestosql.testing.TestingSession.testSessionBuilder;

public class TestApplyTableScanRedirection
{
    private static final String MOCK_CATALOG = "mock_catalog";
    private static final String TEST_SCHEMA = "test_schema";
    private static final String TEST_TABLE = "test_table";
    private static final SchemaTableName sourceTable = new SchemaTableName(TEST_SCHEMA, TEST_TABLE);
    private static final TableHandle TEST_TABLE_HANDLE = createTableHandle(new MockConnectorTableHandle(sourceTable));

    private static final Session MOCK_SESSION = testSessionBuilder().setCatalog(MOCK_CATALOG).setSchema(TEST_SCHEMA).build();

    private static final String sourceColumnNameA = "source_col_a";
    private static final ColumnHandle sourceColumnHandleA = new MockConnectorColumnHandle(sourceColumnNameA, INTEGER);
    private static final String sourceColumnNameB = "source_col_b";
    private static final ColumnHandle sourceColumnHandleB = new MockConnectorColumnHandle(sourceColumnNameB, INTEGER);

    private static final SchemaTableName destinationTable = new SchemaTableName("target_schema", "target_table");
    private static final String destinationColumnNameA = "destination_col_a";
    private static final ColumnHandle destinationColumnHandleA = new MockConnectorColumnHandle(destinationColumnNameA, INTEGER);
    private static final String destinationColumnNameB = "destination_col_b";
    private static final ColumnHandle destinationColumnHandleB = new MockConnectorColumnHandle(destinationColumnNameB, INTEGER);

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
                                new ColumnMetadata(destinationColumnNameB, VARCHAR));
                    }
                    throw new IllegalArgumentException();
                });
        applyTableScanRedirect.ifPresent(builder::withApplyTableScanRedirect);

        return builder.build();
    }
}
