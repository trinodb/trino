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
package io.trino.tests;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Closer;
import com.starburstdata.dataframe.expression.Attribute;
import com.starburstdata.dataframe.type.LongType;
import com.starburstdata.dataframe.type.StringType;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.Session;
import io.trino.client.NodeVersion;
import io.trino.connector.CatalogServiceProvider;
import io.trino.connector.TestingTableFunctions;
import io.trino.metadata.AnalyzePropertyManager;
import io.trino.metadata.CatalogTableFunctions;
import io.trino.metadata.ColumnPropertyManager;
import io.trino.metadata.InternalFunctionBundle;
import io.trino.metadata.MaterializedViewPropertyManager;
import io.trino.metadata.SchemaPropertyManager;
import io.trino.metadata.SessionPropertyManager;
import io.trino.metadata.TableFunctionRegistry;
import io.trino.metadata.TableProceduresPropertyManager;
import io.trino.metadata.TableProceduresRegistry;
import io.trino.metadata.TablePropertyManager;
import io.trino.plugin.base.security.AllowAllSystemAccessControl;
import io.trino.plugin.base.security.DefaultSystemAccessControl;
import io.trino.plugin.tpch.TpchConnectorFactory;
import io.trino.security.AccessControl;
import io.trino.security.AccessControlConfig;
import io.trino.security.AccessControlManager;
import io.trino.server.dataframe.DataTypeMapper;
import io.trino.server.dataframe.DataframeMetadataProvider;
import io.trino.sql.PlannerContext;
import io.trino.sql.analyzer.AnalyzerFactory;
import io.trino.sql.analyzer.SessionTimeProvider;
import io.trino.sql.analyzer.StatementAnalyzerFactory;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.rewrite.ShowQueriesRewrite;
import io.trino.sql.rewrite.StatementRewrite;
import io.trino.testing.PlanTester;
import io.trino.transaction.TransactionId;
import io.trino.transaction.TransactionManager;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.operator.scalar.ApplyFunction.APPLY_FUNCTION;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.TestingEventListenerManager.emptyEventListenerManager;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestDataframeMetadataProvider
{
    private static final SqlParser SQL_PARSER = new SqlParser();
    private DataframeMetadataProvider dataframeMetadataProvider;
    private Closer closer;
    private TransactionManager transactionManager;
    private AccessControl accessControl;
    private PlannerContext plannerContext;
    private TablePropertyManager tablePropertyManager;
    private AnalyzePropertyManager analyzePropertyManager;

    @BeforeAll
    public void setup()
    {
        closer = Closer.create();
        PlanTester planTester = closer.register(PlanTester.create(TEST_SESSION));
        planTester.createCatalog(TEST_SESSION.getCatalog().get(), new TpchConnectorFactory(), ImmutableMap.of());
        transactionManager = planTester.getTransactionManager();

        AccessControlManager accessControlManager = new AccessControlManager(
                NodeVersion.UNKNOWN,
                transactionManager,
                emptyEventListenerManager(),
                new AccessControlConfig(),
                OpenTelemetry.noop(),
                DefaultSystemAccessControl.NAME);
        accessControlManager.setSystemAccessControls(List.of(AllowAllSystemAccessControl.INSTANCE));
        this.accessControl = accessControlManager;

        planTester.addFunctions(InternalFunctionBundle.builder().functions(APPLY_FUNCTION).build());
        plannerContext = planTester.getPlannerContext();
        tablePropertyManager = planTester.getTablePropertyManager();
        analyzePropertyManager = planTester.getAnalyzePropertyManager();

        StatementRewrite statementRewrite = new StatementRewrite(ImmutableSet.of(new ShowQueriesRewrite(
                plannerContext.getMetadata(),
                SQL_PARSER,
                accessControl,
                new SessionPropertyManager(),
                new SchemaPropertyManager(CatalogServiceProvider.fail()),
                new ColumnPropertyManager(CatalogServiceProvider.fail()),
                tablePropertyManager,
                new MaterializedViewPropertyManager(catalogName -> ImmutableMap.of()))));
        StatementAnalyzerFactory statementAnalyzerFactory = new StatementAnalyzerFactory(
                plannerContext,
                SQL_PARSER,
                SessionTimeProvider.DEFAULT,
                accessControl,
                transactionManager,
                user -> ImmutableSet.of(),
                new TableProceduresRegistry(CatalogServiceProvider.fail("procedures are not supported in testing analyzer")),
                new TableFunctionRegistry(catalogName -> new CatalogTableFunctions(ImmutableList.of(
                        new TestingTableFunctions.TwoScalarArgumentsFunction(),
                        new TestingTableFunctions.TableArgumentFunction(),
                        new TestingTableFunctions.TableArgumentRowSemanticsFunction(),
                        new TestingTableFunctions.DescriptorArgumentFunction(),
                        new TestingTableFunctions.TwoTableArgumentsFunction(),
                        new TestingTableFunctions.OnlyPassThroughFunction(),
                        new TestingTableFunctions.MonomorphicStaticReturnTypeFunction(),
                        new TestingTableFunctions.PolymorphicStaticReturnTypeFunction(),
                        new TestingTableFunctions.PassThroughFunction(),
                        new TestingTableFunctions.RequiredColumnsFunction()))),
                tablePropertyManager,
                analyzePropertyManager,
                new TableProceduresPropertyManager(CatalogServiceProvider.fail("procedures are not supported in testing analyzer")));
        AnalyzerFactory analyzerFactory = new AnalyzerFactory(statementAnalyzerFactory, statementRewrite, plannerContext.getTracer());

        DataTypeMapper dataTypeMapper = new DataTypeMapper();
        TransactionId transactionId = transactionManager.beginTransaction(true);
        Session session = createSession(transactionId);
        planTester.getPlannerContext().getLanguageFunctionManager().registerQuery(session);
        dataframeMetadataProvider = new DataframeMetadataProvider(session, analyzerFactory, SQL_PARSER, dataTypeMapper);
    }

    @AfterAll
    public void tearDown()
            throws Exception
    {
        closer.close();
        closer = null;
        transactionManager = null;
        accessControl = null;
        plannerContext = null;
        tablePropertyManager = null;
        analyzePropertyManager = null;
    }

    @Test
    public void testTableExists()
    {
        assertTrue(dataframeMetadataProvider.tableExists("nation"));
        assertFalse(dataframeMetadataProvider.tableExists("not_exists"));
    }

    @Test
    public void testResolveOutput()
    {
        Optional<List<Attribute>> attributesQuery = dataframeMetadataProvider.resolveOutput("SELECT * FROM tpch.tiny.nation");

        Optional<List<Attribute>> attributeList = Optional.of(ImmutableList.of(
                new Attribute(attributesQuery.get().get(0).getId(), "\"nationkey\"", new LongType(), true),
                new Attribute(attributesQuery.get().get(1).getId(), "\"name\"", new StringType(Optional.of(25)), true),
                new Attribute(attributesQuery.get().get(2).getId(), "\"regionkey\"", new LongType(), true),
                new Attribute(attributesQuery.get().get(3).getId(), "\"comment\"", new StringType(Optional.of(152)), true)));
        assertEquals(attributeList, attributesQuery);

        assertThatThrownBy(() -> dataframeMetadataProvider.resolveOutput("SELECT * FROM (SELECT avg(1))"))
                .hasMessageContaining("Dataframe columns should always be named")
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void testAliasOutput()
    {
        assertEquals("""
                SELECT nationkey key
                FROM
                 \stpch.tiny.nation
                    """, dataframeMetadataProvider.aliasOutput("SELECT nationkey as key FROM tpch.tiny.nation"));
        assertEquals("SELECT avg(1) \"avg(1)\"\n\n", dataframeMetadataProvider.aliasOutput("SELECT avg(1)"));
        assertEquals("""
                SELECT * FROM (SELECT *
                FROM
                  (
                   SELECT avg(1)

                )\s
                ) t("_1")""", dataframeMetadataProvider.aliasOutput("SELECT * FROM (SELECT avg(1))"));
    }

    @Test
    public void testLimit()
    {
        assertEquals("""
                SELECT *
                FROM
                  tpch.tiny.nation
                OFFSET 5 ROWS
                LIMIT 5
                """, dataframeMetadataProvider.limit("SELECT * FROM tpch.tiny.nation", "5", "5"));
        assertEquals(" SELECT  *  FROM (SELECT * FROM tpch.tiny.nation OFFSET 10 rows LIMIT 10) OFFSET 1 LIMIT 1", dataframeMetadataProvider.limit("SELECT * FROM tpch.tiny.nation OFFSET 10 rows LIMIT 10", "1", "1"));
    }

    @Test
    public void testSort()
    {
        String query = """
                SELECT *
                FROM
                  tpch.tiny.nation
                ORDER BY regionkey ASC, nationkey ASC
                """;
        assertEquals(query, dataframeMetadataProvider.sort("SELECT * FROM tpch.tiny.nation", Arrays.asList("regionkey", "nationkey")));
        assertEquals(query, dataframeMetadataProvider.sort("SELECT * FROM tpch.tiny.nation ORDER BY regionkey, nationkey", Arrays.asList("regionkey", "nationkey")));
        assertEquals("""
                SELECT *
                FROM
                  tpch.tiny.nation
                ORDER BY regionkey ASC, nationkey DESC
                """, dataframeMetadataProvider.sort("SELECT * FROM tpch.tiny.nation ORDER BY regionkey DESC, nationkey DESC", Arrays.asList("regionkey ASC", "nationkey DESC")));
        assertEquals("""
                SELECT *
                FROM
                  tpch.tiny.nation
                ORDER BY nationkey DESC
                """, dataframeMetadataProvider.sort("SELECT * FROM tpch.tiny.nation ORDER BY regionkey DESC, nationkey DESC", Arrays.asList("nationkey DESC")));
    }

    private static Session createSession(TransactionId transactionId)
    {
        return testSessionBuilder()
                .setCatalog("tpch")
                .setSchema(TINY_SCHEMA_NAME)
                .setTransactionId(transactionId)
                .build();
    }
}
