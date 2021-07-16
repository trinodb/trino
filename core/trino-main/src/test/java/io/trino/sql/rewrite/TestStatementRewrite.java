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
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.connector.CatalogName;
import io.trino.connector.informationschema.InformationSchemaConnector;
import io.trino.connector.system.SystemConnector;
import io.trino.metadata.AbstractMockMetadata;
import io.trino.metadata.Catalog;
import io.trino.metadata.CatalogManager;
import io.trino.metadata.FunctionMetadata;
import io.trino.metadata.InMemoryNodeManager;
import io.trino.metadata.InternalNodeManager;
import io.trino.metadata.Metadata;
import io.trino.metadata.SessionPropertyManager;
import io.trino.security.AccessControlConfig;
import io.trino.security.AccessControlManager;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.transaction.IsolationLevel;
import io.trino.sql.SqlFormatterUtil;
import io.trino.sql.analyzer.FeaturesConfig;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.rewrite.StatementRewrite.Rewrite;
import io.trino.sql.tree.DescribeInput;
import io.trino.sql.tree.DescribeOutput;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.ShowFunctions;
import io.trino.sql.tree.ShowSession;
import io.trino.sql.tree.Statement;
import io.trino.testing.TestingMetadata;
import io.trino.transaction.TransactionManager;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import static io.trino.connector.CatalogName.createInformationSchemaCatalogName;
import static io.trino.connector.CatalogName.createSystemTablesCatalogName;
import static io.trino.cost.StatsCalculator.noopStatsCalculator;
import static io.trino.execution.warnings.WarningCollector.NOOP;
import static io.trino.metadata.MetadataManager.createTestMetadataManager;
import static io.trino.operator.scalar.ApplyFunction.APPLY_FUNCTION;
import static io.trino.spi.session.PropertyMetadata.integerProperty;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.QueryUtil.identifier;
import static io.trino.testing.TestingEventListenerManager.emptyEventListenerManager;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.transaction.InMemoryTransactionManager.createTestTransactionManager;
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
            .setSchema("s1")
            .addPreparedStatement("q1", "SELECT a, ? as col2 FROM tpch.s1.t1");
    private static final Session SETUP_SESSION = testSessionBuilder()
            .setCatalog("c1")
            .setSchema("s1")
            .build();

    private static final SqlParser SQL_PARSER = new SqlParser();
    private static final ShowQueriesRewrite SHOW_QUERIES_REWRITE = new ShowQueriesRewrite();

    private TransactionManager transactionManager;
    private AccessControlManager accessControl;
    private Metadata metadata;
    private Metadata mockMetadata;

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
                        "     ROW ('a', 'tpch', 's1', 't1', 'bigint', 8, false)\n" +
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
    public void testShowFunctions()
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
                Optional.empty(),
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
        CatalogManager catalogManager = new CatalogManager();
        transactionManager = createTestTransactionManager(catalogManager);
        accessControl = new AccessControlManager(transactionManager, emptyEventListenerManager(), new AccessControlConfig());
        accessControl.loadSystemAccessControl();

        metadata = createTestMetadataManager(transactionManager, new FeaturesConfig());
        metadata.addFunctions(ImmutableList.of(APPLY_FUNCTION));

        // Because the default system property and function could be changed if we implement new feature.
        // Create a mock metadata to offer testing data for rewrite classes.
        mockMetadata = new MockMetadata(metadata);

        Catalog tpchTestCatalog = createTestingCatalog(TPCH_CATALOG, TPCH_CATALOG_NAME);
        catalogManager.registerCatalog(tpchTestCatalog);
        metadata.getTablePropertyManager().addProperties(TPCH_CATALOG_NAME, tpchTestCatalog.getConnector(TPCH_CATALOG_NAME).getTableProperties());
        metadata.getAnalyzePropertyManager().addProperties(TPCH_CATALOG_NAME, tpchTestCatalog.getConnector(TPCH_CATALOG_NAME).getAnalyzeProperties());

        SchemaTableName table1 = new SchemaTableName("s1", "t1");
        inSetupTransaction(session -> metadata.createTable(session, TPCH_CATALOG,
                new ConnectorTableMetadata(table1, ImmutableList.of(new ColumnMetadata("a", BIGINT))),
                false));
    }

    private Catalog createTestingCatalog(String catalogName, CatalogName catalog)
    {
        CatalogName systemId = createSystemTablesCatalogName(catalog);
        Connector connector = createTestingConnector();
        metadata.getSessionPropertyManager().addConnectorSessionProperties(new CatalogName(catalogName), connector.getSessionProperties());
        mockMetadata.getSessionPropertyManager().addConnectorSessionProperties(new CatalogName(catalogName), connector.getSessionProperties());
        InternalNodeManager nodeManager = new InMemoryNodeManager();
        return new Catalog(
                catalogName,
                catalog,
                connector,
                createInformationSchemaCatalogName(catalog),
                new InformationSchemaConnector(catalogName, nodeManager, metadata, accessControl),
                systemId,
                new SystemConnector(
                        nodeManager,
                        connector.getSystemTables(),
                        transactionId -> transactionManager.getConnectorTransaction(transactionId, catalog)));
    }

    private void inSetupTransaction(Consumer<Session> consumer)
    {
        transaction(transactionManager, accessControl)
                .singleStatement()
                .readUncommitted()
                .execute(SETUP_SESSION, consumer);
    }

    private static Connector createTestingConnector()
    {
        return new Connector()
        {
            private final ConnectorMetadata metadata = new TestingMetadata();

            @Override
            public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
            {
                return new ConnectorTransactionHandle() {};
            }

            @Override
            public ConnectorMetadata getMetadata(ConnectorTransactionHandle transaction)
            {
                return metadata;
            }

            @Override
            public List<PropertyMetadata<?>> getAnalyzeProperties()
            {
                return ImmutableList.of();
            }
        };
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

        private FunctionMetadata getFunctionMetadata(String name)
        {
            return delegate.getFunctionMetadata(delegate.resolveFunction(QualifiedName.of(name), ImmutableList.of()));
        }
    }
}
