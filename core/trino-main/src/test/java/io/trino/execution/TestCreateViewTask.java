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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import io.trino.connector.CatalogServiceProvider;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.AnalyzePropertyManager;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.TablePropertyManager;
import io.trino.metadata.ViewPropertyManager;
import io.trino.security.AllowAllAccessControl;
import io.trino.sql.analyzer.AnalyzerFactory;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.rewrite.StatementRewrite;
import io.trino.sql.tree.AllColumns;
import io.trino.sql.tree.CreateView;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.Property;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.StringLiteral;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;
import java.util.Optional;

import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.trino.spi.StandardErrorCode.INVALID_VIEW_PROPERTY;
import static io.trino.spi.StandardErrorCode.TABLE_ALREADY_EXISTS;
import static io.trino.spi.connector.SaveMode.FAIL;
import static io.trino.spi.session.PropertyMetadata.booleanProperty;
import static io.trino.sql.QueryUtil.selectList;
import static io.trino.sql.QueryUtil.simpleQuery;
import static io.trino.sql.QueryUtil.table;
import static io.trino.sql.analyzer.StatementAnalyzerFactory.createTestingStatementAnalyzerFactory;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;

@TestInstance(PER_METHOD)
public class TestCreateViewTask
        extends BaseDataDefinitionTaskTest
{
    private static final String CATALOG_NAME = "catalog";
    private SqlParser parser;
    private AnalyzerFactory analyzerFactory;

    @Override
    @BeforeEach
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
                new StatementRewrite(ImmutableSet.of()),
                plannerContext.getTracer());
        QualifiedObjectName tableName = qualifiedObjectName("mock_table");
        metadata.createTable(testSession, CATALOG_NAME, someTable(tableName), FAIL);
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
        metadata.createView(testSession, viewName, someView(), ImmutableMap.of(), false);

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeCreateView(asQualifiedName(viewName), false)))
                .hasErrorCode(TABLE_ALREADY_EXISTS)
                .hasMessage("View already exists: '%s'", viewName);
    }

    @Test
    public void testReplaceViewOnViewIfExists()
    {
        QualifiedObjectName viewName = qualifiedObjectName("existing_view");
        metadata.createView(testSession, viewName, someView(), ImmutableMap.of(), false);

        getFutureValue(executeCreateView(asQualifiedName(viewName), true));
        assertThat(metadata.isView(testSession, viewName)).isTrue();
    }

    @Test
    public void testCreateViewOnTableIfExists()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        metadata.createTable(testSession, CATALOG_NAME, someTable(tableName), FAIL);

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeCreateView(asQualifiedName(tableName), false)))
                .hasErrorCode(TABLE_ALREADY_EXISTS)
                .hasMessage("Table already exists: '%s'", tableName, tableName);
    }

    @Test
    public void testReplaceViewOnTableIfExists()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        metadata.createTable(testSession, CATALOG_NAME, someTable(tableName), FAIL);

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeCreateView(asQualifiedName(tableName), true)))
                .hasErrorCode(TABLE_ALREADY_EXISTS)
                .hasMessage("Table already exists: '%s'", tableName, tableName);
    }

    @Test
    public void testCreateViewOnMaterializedView()
    {
        QualifiedObjectName viewName = qualifiedObjectName("existing_materialized_view");
        metadata.createMaterializedView(testSession, viewName, someMaterializedView(), MATERIALIZED_VIEW_PROPERTIES, false, false);

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeCreateView(asQualifiedName(viewName), false)))
                .hasErrorCode(TABLE_ALREADY_EXISTS)
                .hasMessage("Materialized view already exists: '%s'", viewName);
    }

    @Test
    public void testCreateViewWithUnknownProperty()
    {
        QualifiedObjectName viewName = qualifiedObjectName("view_with_unknown_property");

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeCreateView(
                asQualifiedName(viewName),
                ImmutableList.of(new Property(new Identifier("unknown_property"), new StringLiteral("unknown"))),
                false)))
                .hasErrorCode(INVALID_VIEW_PROPERTY)
                .hasMessage("Catalog 'test_catalog' view property 'unknown_property' does not exist");
    }

    @Test
    public void testCreateViewWithInvalidProperty()
    {
        QualifiedObjectName viewName = qualifiedObjectName("view_with_unknown_property");

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeCreateView(
                asQualifiedName(viewName),
                ImmutableList.of(new Property(new Identifier("boolean_property"), new StringLiteral("unknown"))),
                false)))
                .hasErrorCode(INVALID_VIEW_PROPERTY)
                .hasMessage("Invalid value for catalog 'test_catalog' view property 'boolean_property': Cannot convert ['unknown'] to boolean");
    }

    private ListenableFuture<Void> executeCreateView(QualifiedName viewName, boolean replace)
    {
        return executeCreateView(viewName, ImmutableList.of(), replace);
    }

    private ListenableFuture<Void> executeCreateView(QualifiedName viewName, List<Property> viewProperties, boolean replace)
    {
        Query query = simpleQuery(selectList(new AllColumns()), table(QualifiedName.of("mock_table")));
        CreateView statement = new CreateView(
                viewName,
                query,
                replace,
                Optional.empty(),
                Optional.empty(),
                viewProperties);
        return new CreateViewTask(
                plannerContext,
                new AllowAllAccessControl(),
                parser,
                analyzerFactory,
                new ViewPropertyManager(catalogHandle -> ImmutableMap.of("boolean_property", booleanProperty("boolean_property", "Mock description", false, false))))
                .execute(statement, queryStateMachine, ImmutableList.of(), WarningCollector.NOOP);
    }
}
