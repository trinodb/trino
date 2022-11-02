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
package io.trino.execution;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import io.trino.connector.CatalogServiceProvider;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.AnalyzePropertyManager;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.TablePropertyManager;
import io.trino.security.AllowAllAccessControl;
import io.trino.sql.analyzer.AnalyzerFactory;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.rewrite.StatementRewrite;
import io.trino.sql.tree.AllColumns;
import io.trino.sql.tree.CreateView;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.Query;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.trino.spi.StandardErrorCode.TABLE_ALREADY_EXISTS;
import static io.trino.sql.QueryUtil.selectList;
import static io.trino.sql.QueryUtil.simpleQuery;
import static io.trino.sql.QueryUtil.table;
import static io.trino.sql.analyzer.StatementAnalyzerFactory.createTestingStatementAnalyzerFactory;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;

@Test(singleThreaded = true)
public class TestCreateViewTask
        extends BaseDataDefinitionTaskTest
{
    private static final String CATALOG_NAME = "catalog";
    private SqlParser parser;
    private AnalyzerFactory analyzerFactory;

    @Override
    @BeforeMethod
    public void setUp()
    {
        super.setUp();
        parser = new SqlParser();
        analyzerFactory = new AnalyzerFactory(
                createTestingStatementAnalyzerFactory(
                        plannerContext,
                        new AllowAllAccessControl(),
                        new TablePropertyManager(CatalogServiceProvider.fail()),
                        new AnalyzePropertyManager(CatalogServiceProvider.fail())),
                new StatementRewrite(ImmutableSet.of()));
        QualifiedObjectName tableName = qualifiedObjectName("mock_table");
        metadata.createTable(testSession, CATALOG_NAME, someTable(tableName), false);
    }

    @Test
    public void testCreateViewOnViewIfNotExists()
    {
        QualifiedObjectName viewName = qualifiedObjectName("new_view");
        getFutureValue(executeCreateView(asQualifiedName(viewName), false));
        assertThat(metadata.isView(testSession, viewName)).isTrue();
    }

    @Test
    public void testCreateViewOnViewIfExists()
    {
        QualifiedObjectName viewName = qualifiedObjectName("existing_view");
        metadata.createView(testSession, viewName, someView(), false);

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeCreateView(asQualifiedName(viewName), false)))
                .hasErrorCode(TABLE_ALREADY_EXISTS)
                .hasMessage("View already exists: '%s'", viewName);
    }

    @Test
    public void testReplaceViewOnViewIfExists()
    {
        QualifiedObjectName viewName = qualifiedObjectName("existing_view");
        metadata.createView(testSession, viewName, someView(), false);

        getFutureValue(executeCreateView(asQualifiedName(viewName), true));
        assertThat(metadata.isView(testSession, viewName)).isTrue();
    }

    @Test
    public void testCreateViewOnTableIfExists()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        metadata.createTable(testSession, CATALOG_NAME, someTable(tableName), false);

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeCreateView(asQualifiedName(tableName), false)))
                .hasErrorCode(TABLE_ALREADY_EXISTS)
                .hasMessage("Table already exists: '%s'", tableName, tableName);
    }

    @Test
    public void testReplaceViewOnTableIfExists()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        metadata.createTable(testSession, CATALOG_NAME, someTable(tableName), false);

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeCreateView(asQualifiedName(tableName), true)))
                .hasErrorCode(TABLE_ALREADY_EXISTS)
                .hasMessage("Table already exists: '%s'", tableName, tableName);
    }

    @Test
    public void testCreateViewOnMaterializedView()
    {
        QualifiedObjectName viewName = qualifiedObjectName("existing_materialized_view");
        metadata.createMaterializedView(testSession, viewName, someMaterializedView(), false, false);

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeCreateView(asQualifiedName(viewName), false)))
                .hasErrorCode(TABLE_ALREADY_EXISTS)
                .hasMessage("Materialized view already exists: '%s'", viewName);
    }

    private ListenableFuture<Void> executeCreateView(QualifiedName viewName, boolean replace)
    {
        Query query = simpleQuery(selectList(new AllColumns()), table(QualifiedName.of("mock_table")));
        CreateView statement = new CreateView(
                viewName,
                query,
                replace,
                Optional.empty(),
                Optional.empty());
        return new CreateViewTask(metadata, new AllowAllAccessControl(), parser, analyzerFactory).execute(statement, queryStateMachine, ImmutableList.of(), WarningCollector.NOOP);
    }
}
