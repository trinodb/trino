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
package io.trino.sql.rewrite;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.connector.CatalogName;
import io.trino.metadata.AbstractMockMetadata;
import io.trino.metadata.FunctionMetadata;
import io.trino.metadata.Metadata;
import io.trino.metadata.SessionPropertyManager;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.security.AccessControlManager;
import io.trino.spi.security.RoleGrant;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.sql.SqlFormatterUtil;
import io.trino.sql.analyzer.QueryExplainer;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.rewrite.StatementRewrite.Rewrite;
import io.trino.sql.tree.DescribeInput;
import io.trino.sql.tree.DescribeOutput;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.ShowCatalogs;
import io.trino.sql.tree.ShowFunctions;
import io.trino.sql.tree.ShowRoleGrants;
import io.trino.sql.tree.ShowSession;
import io.trino.sql.tree.ShowStats;
import io.trino.sql.tree.Statement;
import io.trino.sql.tree.Table;
import io.trino.testing.LocalQueryRunner;
import io.trino.transaction.TransactionManager;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.trino.cost.StatsCalculator.noopStatsCalculator;
import static io.trino.execution.warnings.WarningCollector.NOOP;
import static io.trino.spi.security.PrincipalType.USER;
import static io.trino.spi.session.PropertyMetadata.integerProperty;
import static io.trino.sql.QueryUtil.identifier;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.transaction.TransactionBuilder.transaction;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;

public class TestStatementRewrite
{
    private static final String TPCH_CATALOG = "tpch";
    private static final CatalogName TPCH_CATALOG_NAME = new CatalogName(TPCH_CATALOG);
    private static final Session.SessionBuilder CLIENT_SESSION_BUILDER = testSessionBuilder()
            .setCatalog(TPCH_CATALOG)
            .setSchema("tiny")
            .addPreparedStatement("q1", "SELECT orderkey, ? as col2 FROM tpch.tiny.orders");
    private static final Session SETUP_SESSION = testSessionBuilder()
            .setCatalog(TPCH_CATALOG)
            .setSchema("tiny")
            .build();

    private static final SqlParser SQL_PARSER = new SqlParser();
    private static final ShowQueriesRewrite SHOW_QUERIES_REWRITE = new ShowQueriesRewrite();

    private TransactionManager transactionManager;
    private AccessControlManager accessControl;
    private Metadata metadata;
    private Metadata mockMetadata;
    private QueryExplainer queryExplainer;

    @Test
    public void testDescribeOutputFormatSql()
    {
        assertFormatSql(
                new DescribeOutput(identifier("q1")),
                new DescribeOutputRewrite(),
                "SELECT\n" +
                        "  \"Column Name\"\n" +
                        ", \"Catalog\"\n" +
                        ", \"Schema\"\n" +
                        ", \"Table\"\n" +
                        ", \"Type\"\n" +
                        ", \"Type Size\"\n" +
                        ", \"Aliased\"\n" +
                        "FROM\n" +
                        "  (\n" +
                        " VALUES \n" +
                        "     ROW ('orderkey', 'tpch', 'tiny', 'orders', 'bigint', 8, false)\n" +
                        "   , ROW ('col2', '', '', '', 'unknown', 1, true)\n" +
                        ")  \"Statement Output\" (\"Column Name\", \"Catalog\", \"Schema\", \"Table\", \"Type\", \"Type Size\", \"Aliased\")\n");
    }

    @Test
    public void testDescribeInputFormatSql()
    {
        assertFormatSql(
                new DescribeInput(identifier("q1")),
                new DescribeInputRewrite(),
                "SELECT\n" +
                        "  \"Position\"\n" +
                        ", \"Type\"\n" +
                        "FROM\n" +
                        "  (\n" +
                        " VALUES \n" +
                        "     ROW (0, 'unknown')\n" +
                        ")  \"Parameter Input\" (\"Position\", \"Type\")\n" +
                        "ORDER BY \"Position\" ASC\n");
    }

    @Test
    public void testShowSessionFormatSql()
    {
        assertFormatSqlWithMockMetadata(
                new ShowSession(Optional.of("%"), Optional.of("$")),
                SHOW_QUERIES_REWRITE,
                "SELECT\n" +
                        "  \"name\" \"Name\"\n" +
                        ", \"value\" \"Value\"\n" +
                        ", \"default\" \"Default\"\n" +
                        ", \"type\" \"Type\"\n" +
                        ", \"description\" \"Description\"\n" +
                        "FROM\n" +
                        "  (\n" +
                        " VALUES \n" +
                        "     ROW ('prop1', '1', '1', 'integer', 'des1', true)\n" +
                        "   , ROW ('prop2', '2', '2', 'integer', 'des2', true)\n" +
                        "   , ROW ('', '', '', '', '', false)\n" +
                        ")  \"session\" (\"name\", \"value\", \"default\", \"type\", \"description\", \"include\")\n" +
                        "WHERE (include AND (name LIKE '%' ESCAPE '$'))\n");
    }

    @Test
    public void testShowFunctionsFormatSql()
    {
        assertFormatSqlWithMockMetadata(
                new ShowFunctions(Optional.of("%"), Optional.of("$")),
                SHOW_QUERIES_REWRITE,
                "SELECT\n" +
                        "  \"function_name\" \"Function\"\n" +
                        ", \"return_type\" \"Return Type\"\n" +
                        ", \"argument_types\" \"Argument Types\"\n" +
                        ", \"function_type\" \"Function Type\"\n" +
                        ", \"deterministic\" \"Deterministic\"\n" +
                        ", \"description\" \"Description\"\n" +
                        "FROM\n" +
                        "  (\n" +
                        " VALUES \n" +
                        "     ROW ('row_number', 'bigint', '', 'window', true, '')\n" +
                        "   , ROW ('rank', 'bigint', '', 'window', true, '')\n" +
                        ")  \"functions\" (\"function_name\", \"return_type\", \"argument_types\", \"function_type\", \"deterministic\", \"description\")\n" +
                        "WHERE (function_name LIKE '%' ESCAPE '$')\n" +
                        "ORDER BY lower(function_name) ASC, \"return_type\" ASC, \"argument_types\" ASC, \"function_type\" ASC\n");
    }

    @Test
    public void testShowCatalogsFormatSql()
    {
        assertFormatSql(
                new ShowCatalogs(Optional.of("%"), Optional.of("$")),
                SHOW_QUERIES_REWRITE,
                "SELECT *\n" +
                        "FROM\n" +
                        "  (\n" +
                        " VALUES \n" +
                        "     ROW ('system')\n" +
                        "   , ROW ('tpch')\n" +
                        ")  \"catalogs\" (\"Catalog\")\n" +
                        "WHERE (\"catalog\" LIKE '%' ESCAPE '$')\n" +
                        "ORDER BY \"Catalog\" ASC\n");
    }

    @Test
    public void testShowRoleGrantsFormatSql()
    {
        assertFormatSqlWithMockMetadata(
                new ShowRoleGrants(Optional.empty()),
                SHOW_QUERIES_REWRITE,
                "SELECT *\n" +
                        "FROM\n" +
                        "  (\n" +
                        " VALUES \n" +
                        "     ROW ('b1')\n" +
                        "   , ROW ('b2')\n" +
                        ")  \"role_grants\" (\"Role Grants\")\n" +
                        "ORDER BY \"Role Grants\" ASC\n");
    }

    @Test
    public void testShowStatesFormatSql()
    {
        assertFormatSql(
                new ShowStats(new Table(QualifiedName.of("nation"))),
                new ShowStatsRewrite(),
                "SELECT\n" +
                        "  \"column_name\"\n" +
                        ", \"data_size\"\n" +
                        ", \"distinct_values_count\"\n" +
                        ", \"nulls_fraction\"\n" +
                        ", \"row_count\"\n" +
                        ", \"low_value\"\n" +
                        ", \"high_value\"\n" +
                        "FROM\n" +
                        "  (\n" +
                        " VALUES \n" +
                        "     ROW ('nationkey', CAST(null AS double), CAST(null AS double), CAST(null AS double), CAST(null AS double), CAST(null AS varchar), CAST(null AS varchar))\n" +
                        "   , ROW ('name', CAST(null AS double), CAST(null AS double), CAST(null AS double), CAST(null AS double), CAST(null AS varchar), CAST(null AS varchar))\n" +
                        "   , ROW ('regionkey', CAST(null AS double), CAST(null AS double), CAST(null AS double), CAST(null AS double), CAST(null AS varchar), CAST(null AS varchar))\n" +
                        "   , ROW ('comment', CAST(null AS double), CAST(null AS double), CAST(null AS double), CAST(null AS double), CAST(null AS varchar), CAST(null AS varchar))\n" +
                        "   , ROW (CAST(null AS varchar), CAST(null AS double), CAST(null AS double), CAST(null AS double), CAST(null AS double), CAST(null AS varchar), CAST(null AS varchar))\n" +
                        ")  \"table_stats\" (\"column_name\", \"data_size\", \"distinct_values_count\", \"nulls_fraction\", \"row_count\", \"low_value\", \"high_value\")\n");
    }

    private void assertFormatSql(Statement node, Rewrite rewriteProvider, String expected)
    {
        transaction(transactionManager, accessControl)
                .readUncommitted()
                .execute(beginTransaction(CLIENT_SESSION_BUILDER), session -> {
                    Statement rewrittenNode = rewrite(session, metadata, node, rewriteProvider);
                    String actual = SqlFormatterUtil.getFormattedSql(rewrittenNode, SQL_PARSER);
                    assertThat(actual).isEqualTo(expected);
                });
    }

    private void assertFormatSqlWithMockMetadata(Statement node, Rewrite rewriteProvider, String expected)
    {
        transaction(transactionManager, accessControl)
                .readUncommitted()
                .execute(beginTransaction(CLIENT_SESSION_BUILDER), session -> {
                    Statement rewrittenNode = rewrite(session, mockMetadata, node, rewriteProvider);
                    String actual = SqlFormatterUtil.getFormattedSql(rewrittenNode, SQL_PARSER);
                    assertThat(actual).isEqualTo(expected);
                });
    }

    private Statement rewrite(Session session, Metadata metadata, Statement node, Rewrite rewriteProvider)
    {
        return rewriteProvider.rewrite(
                session,
                metadata,
                SQL_PARSER,
                Optional.of(queryExplainer),
                node,
                emptyList(),
                emptyMap(),
                user -> ImmutableSet.of(),
                accessControl,
                NOOP,
                noopStatsCalculator());
    }

    private Session beginTransaction(Session.SessionBuilder sessionBuilder)
    {
        return sessionBuilder.setTransactionId(transactionManager.beginTransaction(false)).build();
    }

    @BeforeClass
    public void setup()
    {
        LocalQueryRunner runner = LocalQueryRunner.builder(SETUP_SESSION).build();
        transactionManager = runner.getTransactionManager();
        accessControl = runner.getAccessControl();
        metadata = runner.getMetadata();
        queryExplainer = runner.getQueryExplainer(true);

        // Because the default system property and function could be changed if we implement new feature.
        // Create a mock metadata to offer testing data for rewrite classes.
        mockMetadata = new MockMetadata(metadata);

        runner.installPlugin(new TpchPlugin());
        runner.createCatalog(TPCH_CATALOG, TPCH_CATALOG, ImmutableMap.of());

        // initial the connector session properties for mockMetadata
        mockMetadata.getSessionPropertyManager().addConnectorSessionProperties(TPCH_CATALOG_NAME, ImmutableList.of());
        mockMetadata.getSessionPropertyManager().addConnectorSessionProperties(new CatalogName("system"), ImmutableList.of());
    }

    private static class MockMetadata
            extends AbstractMockMetadata
    {
        private final Metadata delegate;
        private final SessionPropertyManager sessionPropertyManager;

        public MockMetadata(Metadata delegate)
        {
            this.delegate = delegate;
            sessionPropertyManager = new SessionPropertyManager(
                    ImmutableList.of(
                            integerProperty("prop1", "des1", 1, false),
                            integerProperty("prop2", "des2", 2, false),
                            integerProperty("hidden", "hidden", 3, true)));
        }

        @Override
        public SessionPropertyManager getSessionPropertyManager()
        {
            return sessionPropertyManager;
        }

        @Override
        public Map<String, CatalogName> getCatalogNames(Session session)
        {
            return delegate.getCatalogNames(session);
        }

        @Override
        public List<FunctionMetadata> listFunctions()
        {
            return ImmutableList.of(
                    getFunctionMetadata("row_number"),
                    getFunctionMetadata("rank"));
        }

        @Override
        public Set<RoleGrant> listRoleGrants(Session session, String catalog, TrinoPrincipal principal)
        {
            TrinoPrincipal admin = new TrinoPrincipal(USER, "admin");
            return ImmutableSet.of(
                    new RoleGrant(admin, "b1", false),
                    new RoleGrant(admin, "b2", true));
        }

        private FunctionMetadata getFunctionMetadata(String name)
        {
            return delegate.getFunctionMetadata(delegate.resolveFunction(QualifiedName.of(name), ImmutableList.of()));
        }
    }
}
