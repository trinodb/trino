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
import io.trino.connector.MockConnectorFactory.ApplyTableScanRedirect;
import io.trino.connector.MockConnectorTableHandle;
import io.trino.metadata.TableHandle;
import io.trino.metadata.TableHandle.ResolvingIdentity;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableScanRedirectApplicationResult;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.Identity;
import io.trino.spi.security.SelectedRole;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.assertions.PredicateMatcher;
import io.trino.sql.planner.iterative.rule.test.RuleTester;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.testing.PlanTester;
import io.trino.testing.TestingTransactionHandle;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.predicate.Domain.singleValue;
import static io.trino.spi.session.PropertyMetadata.stringProperty;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.ir.ComparisonOperator.EQUAL;
import static io.trino.sql.ir.TestingIr.comparison;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.testing.TestingHandles.TEST_CATALOG_NAME;
import static io.trino.testing.TestingHandles.TEST_RESOLVING_IDENTITY;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.tests.BogusType.BOGUS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class TestApplyTableScanRedirection
{
    private static final String TEST_SCHEMA = "test_schema";
    private static final String TEST_TABLE = "test_table";
    private static final SchemaTableName SOURCE_TABLE = new SchemaTableName(TEST_SCHEMA, TEST_TABLE);

    private static final Session MOCK_SESSION = testSessionBuilder().setCatalog(TEST_CATALOG_NAME).setSchema(TEST_SCHEMA).build();

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

    private static TableHandle createTableHandle(RuleTester ruleTester, ConnectorTableHandle tableHandle)
    {
        return new TableHandle(
                ruleTester.getCurrentCatalogHandle(),
                tableHandle,
                TestingTransactionHandle.create(),
                TEST_RESOLVING_IDENTITY);
    }

    @Test
    public void testDoesNotFire()
    {
        MockConnectorFactory mockFactory = createMockFactory(Optional.empty());
        try (RuleTester ruleTester = RuleTester.builder().withDefaultCatalogConnectorFactory(mockFactory).build()) {
            ruleTester.assertThat(new ApplyTableScanRedirection(ruleTester.getPlannerContext()))
                    .withSession(MOCK_SESSION)
                    .on(p -> {
                        Symbol column = p.symbol(SOURCE_COLUMN_NAME_A, VARCHAR);
                        return p.tableScan(
                                createTableHandle(ruleTester, new MockConnectorTableHandle(SOURCE_TABLE)),
                                ImmutableList.of(column),
                                ImmutableMap.of(column, SOURCE_COLUMN_HANDLE_A));
                    })
                    .doesNotFire();
        }
    }

    @Test
    public void testDoesNotFireForDeleteTableScan()
    {
        // make the mock connector return a table scan on different table
        ApplyTableScanRedirect applyTableScanRedirect = getMockApplyRedirect(
                ImmutableMap.of(SOURCE_COLUMN_HANDLE_A, DESTINATION_COLUMN_NAME_A));
        MockConnectorFactory mockFactory = createMockFactory(Optional.of(applyTableScanRedirect));
        try (RuleTester ruleTester = RuleTester.builder().withDefaultCatalogConnectorFactory(mockFactory).build()) {
            ruleTester.assertThat(new ApplyTableScanRedirection(ruleTester.getPlannerContext()))
                    .withSession(MOCK_SESSION)
                    .on(p -> {
                        Symbol column = p.symbol(SOURCE_COLUMN_NAME_A, VARCHAR);
                        return p.tableScan(
                                createTableHandle(ruleTester, new MockConnectorTableHandle(SOURCE_TABLE)),
                                ImmutableList.of(column),
                                ImmutableMap.of(column, SOURCE_COLUMN_HANDLE_A),
                                true);
                    })
                    .doesNotFire();
        }
    }

    @Test
    public void doesNotFireIfNoTableScan()
    {
        ApplyTableScanRedirect applyTableScanRedirect = getMockApplyRedirect(
                ImmutableMap.of(SOURCE_COLUMN_HANDLE_A, DESTINATION_COLUMN_NAME_A));
        MockConnectorFactory mockFactory = createMockFactory(Optional.of(applyTableScanRedirect));
        try (RuleTester ruleTester = RuleTester.builder().withDefaultCatalogConnectorFactory(mockFactory).build()) {
            ruleTester.assertThat(new ApplyTableScanRedirection(ruleTester.getPlannerContext()))
                    .withSession(MOCK_SESSION)
                    .on(p -> p.values(p.symbol("a", BIGINT)))
                    .doesNotFire();
        }
    }

    @Test
    public void testMismatchedTypesWithCoercion()
    {
        // make the mock connector return a table scan on different table
        ApplyTableScanRedirect applyTableScanRedirect = getMockApplyRedirect(
                ImmutableMap.of(SOURCE_COLUMN_HANDLE_A, DESTINATION_COLUMN_NAME_C));
        MockConnectorFactory mockFactory = createMockFactory(Optional.of(applyTableScanRedirect));
        try (RuleTester ruleTester = RuleTester.builder().withDefaultCatalogConnectorFactory(mockFactory).build()) {
            ruleTester.assertThat(new ApplyTableScanRedirection(ruleTester.getPlannerContext()))
                    .withSession(MOCK_SESSION)
                    .on(p -> {
                        Symbol column = p.symbol(SOURCE_COLUMN_NAME_A, VARCHAR);
                        return p.tableScan(
                                createTableHandle(ruleTester, new MockConnectorTableHandle(SOURCE_TABLE)),
                                ImmutableList.of(column),
                                ImmutableMap.of(column, SOURCE_COLUMN_HANDLE_A));
                    })
                    .matches(
                            project(ImmutableMap.of("COL", expression(new Cast(new Reference(BIGINT, "DEST_COL"), VARCHAR))),
                                    tableScan(
                                            new MockConnectorTableHandle(DESTINATION_TABLE)::equals,
                                            TupleDomain.all(),
                                            ImmutableMap.of("DEST_COL", DESTINATION_COLUMN_HANDLE_C::equals))));
        }
    }

    @Test
    public void testMismatchedTypesWithMissingCoercion()
    {
        // make the mock connector return a table scan on different table
        ApplyTableScanRedirect applyTableScanRedirect = getMockApplyRedirect(
                ImmutableMap.of(SOURCE_COLUMN_HANDLE_A, DESTINATION_COLUMN_NAME_D));
        MockConnectorFactory mockFactory = createMockFactory(Optional.of(applyTableScanRedirect));
        try (RuleTester ruleTester = RuleTester.builder().withDefaultCatalogConnectorFactory(mockFactory).build()) {
            PlanTester runner = ruleTester.getPlanTester();
            runner.inTransaction(MOCK_SESSION, transactionSession -> {
                assertThatThrownBy(() ->
                        runner.createPlan(transactionSession, "SELECT source_col_a FROM test_table"))
                        .isInstanceOf(TrinoException.class)
                        .hasMessageMatching("Cast not possible from redirected column test_catalog.target_schema.target_table.destination_col_d with type Bogus to source column .*test_catalog.test_schema.test_table.*source_col_a.* with type: varchar");
                return null;
            });
        }
    }

    @Test
    public void testApplyTableScanRedirection()
    {
        // make the mock connector return a table scan on different table
        ApplyTableScanRedirect applyTableScanRedirect = getMockApplyRedirect(
                ImmutableMap.of(SOURCE_COLUMN_HANDLE_A, DESTINATION_COLUMN_NAME_A));
        MockConnectorFactory mockFactory = createMockFactory(Optional.of(applyTableScanRedirect));
        try (RuleTester ruleTester = RuleTester.builder().withDefaultCatalogConnectorFactory(mockFactory).build()) {
            ruleTester.assertThat(new ApplyTableScanRedirection(ruleTester.getPlannerContext()))
                    .withSession(MOCK_SESSION)
                    .on(p -> {
                        Symbol column = p.symbol(SOURCE_COLUMN_NAME_A, VARCHAR);
                        return p.tableScan(
                                createTableHandle(ruleTester, new MockConnectorTableHandle(SOURCE_TABLE)),
                                ImmutableList.of(column),
                                ImmutableMap.of(column, SOURCE_COLUMN_HANDLE_A));
                    })
                    .matches(
                            tableScan(
                                    new MockConnectorTableHandle(DESTINATION_TABLE)::equals,
                                    TupleDomain.all(),
                                    ImmutableMap.of("DEST_COL", DESTINATION_COLUMN_HANDLE_A::equals))
                                    .with(new PredicateMatcher<TableScanNode>(node -> node.getTable().resolvingIdentity().user().equals(MOCK_SESSION.getUser()))));
        }
    }

    @ParameterizedTest
    @MethodSource("redirectionIdentityCases")
    public void testRedirectionPreservesResolvingIdentity(boolean crossCatalog, boolean withRequiredFilter, boolean resolvedAsOwner)
    {
        SelectedRole ownerRole = new SelectedRole(SelectedRole.Type.ROLE, Optional.of("owner_role"));
        SelectedRole sessionRole = new SelectedRole(SelectedRole.Type.ROLE, Optional.of("session_role"));
        String destinationCatalog = crossCatalog ? "destination_catalog" : TEST_CATALOG_NAME;
        Identity owner = Identity.forUser("view_owner")
                .withGroups(Set.of("view_owners"))
                .withConnectorRole(TEST_CATALOG_NAME, ownerRole)
                .build();
        Identity sessionIdentity = Identity.forUser("user")
                .withConnectorRole(destinationCatalog, sessionRole)
                .build();
        Identity tableIdentity = resolvedAsOwner ? owner : sessionIdentity;
        // a connector role applies only in the catalog it was set for
        Optional<SelectedRole> expectedDestinationRole = tableIdentity.toConnectorIdentity(destinationCatalog).getConnectorRole();
        List<String> destinationCalls = new ArrayList<>();
        AtomicReference<ConnectorSession> sourceSession = new AtomicReference<>();
        UnaryOperator<MockConnectorFactory.Builder> withIdentityChecks = builder -> builder
                .withSessionProperty(stringProperty("redirection_context", "Test redirection context", "default", false))
                .withRedirectTable((connectorSession, table) -> {
                    if (table.equals(DESTINATION_TABLE)) {
                        assertDestinationSession(connectorSession, sourceSession.get(), tableIdentity, expectedDestinationRole);
                        destinationCalls.add("redirectTable");
                    }
                    return Optional.empty();
                })
                .withGetTableHandle((connectorSession, table) -> {
                    if (table.equals(DESTINATION_TABLE)) {
                        assertDestinationSession(connectorSession, sourceSession.get(), tableIdentity, expectedDestinationRole);
                        destinationCalls.add("getTableHandle");
                    }
                    return new MockConnectorTableHandle(table);
                });
        MockConnectorFactory sourceFactory = withIdentityChecks.apply(createMockFactoryBuilder())
                .withApplyTableScanRedirect((connectorSession, _) -> {
                    sourceSession.set(connectorSession);
                    assertThat(connectorSession.getIdentity().getUser()).isEqualTo(tableIdentity.getUser());
                    return Optional.of(new TableScanRedirectApplicationResult(
                            new CatalogSchemaTableName(destinationCatalog, DESTINATION_TABLE),
                            ImmutableMap.of(
                                    SOURCE_COLUMN_HANDLE_A, DESTINATION_COLUMN_NAME_C,
                                    SOURCE_COLUMN_HANDLE_B, DESTINATION_COLUMN_NAME_B),
                            withRequiredFilter
                                    ? TupleDomain.withColumnDomains(ImmutableMap.of(DESTINATION_COLUMN_NAME_C, singleValue(VARCHAR, utf8Slice("7"))))
                                    : TupleDomain.all()));
                })
                .build();

        try (RuleTester ruleTester = RuleTester.builder().withDefaultCatalogConnectorFactory(sourceFactory).build()) {
            if (crossCatalog) {
                ruleTester.getPlanTester().createCatalog(
                        destinationCatalog,
                        withIdentityChecks.apply(createMockFactoryBuilder()).withName("destination_mock").build(),
                        ImmutableMap.of());
            }
            Session session = Session.builder(ruleTester.getPlanTester().getDefaultSession())
                    .setIdentity(sessionIdentity)
                    .setCatalogSessionProperty(destinationCatalog, "redirection_context", "preserved")
                    .build();

            PlanMatchPattern destinationScan = tableScan(
                    new MockConnectorTableHandle(DESTINATION_TABLE)::equals,
                    TupleDomain.all(),
                    withRequiredFilter
                            ? ImmutableMap.of("OUT", DESTINATION_COLUMN_HANDLE_B::equals, "FILTER", DESTINATION_COLUMN_HANDLE_C::equals)
                            : ImmutableMap.of("OUT", DESTINATION_COLUMN_HANDLE_B::equals))
                    .with(new PredicateMatcher<TableScanNode>(node -> node.getTable().resolvingIdentity().user().equals(tableIdentity.getUser()) &&
                            node.getTable().resolvingIdentity().groups().equals(tableIdentity.getGroups())));

            ruleTester.assertThat(new ApplyTableScanRedirection(ruleTester.getPlannerContext()))
                    .withSession(session)
                    .on(p -> {
                        Symbol column = p.symbol(SOURCE_COLUMN_NAME_B, VARCHAR);
                        return p.tableScan(
                                new TableHandle(
                                        ruleTester.getCurrentCatalogHandle(),
                                        new MockConnectorTableHandle(SOURCE_TABLE),
                                        TestingTransactionHandle.create(),
                                        ResolvingIdentity.from(tableIdentity.toConnectorIdentity(TEST_CATALOG_NAME))),
                                ImmutableList.of(column),
                                ImmutableMap.of(column, SOURCE_COLUMN_HANDLE_B));
                    })
                    .matches(withRequiredFilter ? anyTree(destinationScan) : destinationScan);

            assertThat(destinationCalls).contains("redirectTable", "getTableHandle");
        }
    }

    private static Stream<Arguments> redirectionIdentityCases()
    {
        return Stream.of(
                arguments(false, false, true),
                arguments(false, true, true),
                arguments(true, false, true),
                arguments(true, true, true),
                arguments(false, false, false),
                arguments(false, true, false),
                arguments(true, false, false),
                arguments(true, true, false));
    }

    private static void assertDestinationSession(ConnectorSession actual, ConnectorSession sourceSession, Identity expectedIdentity, Optional<SelectedRole> expectedRole)
    {
        assertThat(actual.getIdentity().getUser()).isEqualTo(expectedIdentity.getUser());
        assertThat(actual.getIdentity().getGroups()).isEqualTo(expectedIdentity.getGroups());
        assertThat(actual.getIdentity().getConnectorRole()).isEqualTo(expectedRole);
        assertThat(actual.getProperty("redirection_context", String.class)).isEqualTo("preserved");
        assertThat(actual.getQueryId()).isEqualTo(sourceSession.getQueryId());
        assertThat(actual.getStart()).isEqualTo(sourceSession.getStart());
    }

    @Test
    public void testApplyTableScanRedirectionWithFilter()
    {
        // make the mock connector return a table scan on different table
        // source table handle has a pushed down predicate
        ApplyTableScanRedirect applyTableScanRedirect = getMockApplyRedirect(
                ImmutableMap.of(
                        SOURCE_COLUMN_HANDLE_A, DESTINATION_COLUMN_NAME_A,
                        SOURCE_COLUMN_HANDLE_B, DESTINATION_COLUMN_NAME_B));
        MockConnectorFactory mockFactory = createMockFactory(Optional.of(applyTableScanRedirect));
        try (RuleTester ruleTester = RuleTester.builder().withDefaultCatalogConnectorFactory(mockFactory).build()) {
            ApplyTableScanRedirection applyTableScanRedirection = new ApplyTableScanRedirection(ruleTester.getPlannerContext());
            TupleDomain<ColumnHandle> constraint = TupleDomain.withColumnDomains(
                    ImmutableMap.of(SOURCE_COLUMN_HANDLE_A, singleValue(VARCHAR, utf8Slice("foo"))));
            ruleTester.assertThat(applyTableScanRedirection)
                    .withSession(MOCK_SESSION)
                    .on(p -> {
                        Symbol column = p.symbol(SOURCE_COLUMN_NAME_A, VARCHAR);
                        return p.tableScan(
                                createTableHandle(ruleTester, new MockConnectorTableHandle(SOURCE_TABLE, constraint, Optional.empty())),
                                ImmutableList.of(column),
                                ImmutableMap.of(column, SOURCE_COLUMN_HANDLE_A),
                                constraint);
                    })
                    .matches(
                            filter(
                                    comparison(EQUAL, new Reference(VARCHAR, "DEST_COL"), new Constant(VARCHAR, utf8Slice("foo"))),
                                    tableScan(
                                            new MockConnectorTableHandle(DESTINATION_TABLE)::equals,
                                            TupleDomain.all(),
                                            ImmutableMap.of("DEST_COL", DESTINATION_COLUMN_HANDLE_A::equals))));

            ruleTester.assertThat(applyTableScanRedirection)
                    .withSession(MOCK_SESSION)
                    .on(p -> {
                        Symbol column = p.symbol(SOURCE_COLUMN_NAME_B, VARCHAR);
                        return p.tableScan(
                                createTableHandle(ruleTester, new MockConnectorTableHandle(SOURCE_TABLE, constraint, Optional.empty())),
                                ImmutableList.of(column),
                                ImmutableMap.of(column, SOURCE_COLUMN_HANDLE_B), // predicate on non-projected column
                                TupleDomain.all());
                    })
                    .matches(
                            project(
                                    ImmutableMap.of("expr", expression(new Reference(BIGINT, "DEST_COL_B"))),
                                    filter(
                                            comparison(EQUAL, new Reference(VARCHAR, "DEST_COL_A"), new Constant(VARCHAR, utf8Slice("foo"))),
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
        return (ConnectorSession _, ConnectorTableHandle handle) -> Optional.of(
                new TableScanRedirectApplicationResult(
                        new CatalogSchemaTableName(TEST_CATALOG_NAME, DESTINATION_TABLE),
                        redirectionMapping,
                        ((MockConnectorTableHandle) handle).getConstraint()
                                .transformKeys(MockConnectorColumnHandle.class::cast)
                                .transformKeys(redirectionMapping::get)));
    }

    private MockConnectorFactory createMockFactory(Optional<MockConnectorFactory.ApplyTableScanRedirect> applyTableScanRedirect)
    {
        MockConnectorFactory.Builder builder = createMockFactoryBuilder();
        applyTableScanRedirect.ifPresent(builder::withApplyTableScanRedirect);
        return builder.build();
    }

    private MockConnectorFactory.Builder createMockFactoryBuilder()
    {
        return MockConnectorFactory.builder()
                .withGetColumns(schemaTableName -> {
                    if (schemaTableName.equals(SOURCE_TABLE)) {
                        return ImmutableList.of(
                                new ColumnMetadata(SOURCE_COLUMN_NAME_A, VARCHAR),
                                new ColumnMetadata(SOURCE_COLUMN_NAME_B, VARCHAR));
                    }
                    if (schemaTableName.equals(DESTINATION_TABLE)) {
                        return ImmutableList.of(
                                new ColumnMetadata(DESTINATION_COLUMN_NAME_A, VARCHAR),
                                new ColumnMetadata(DESTINATION_COLUMN_NAME_B, VARCHAR),
                                new ColumnMetadata(DESTINATION_COLUMN_NAME_C, BIGINT),
                                new ColumnMetadata(DESTINATION_COLUMN_NAME_D, BOGUS));
                    }
                    throw new IllegalArgumentException();
                });
    }
}
