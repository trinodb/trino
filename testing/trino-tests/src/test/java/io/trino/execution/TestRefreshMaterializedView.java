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
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.SettableFuture;
import io.trino.Session;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorPlugin;
import io.trino.connector.MockConnectorTableHandle;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.SchemaTableName;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static io.airlift.concurrent.MoreFutures.addExceptionCallback;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.MoreFutures.toCompletableFuture;
import static io.trino.execution.QueryState.RUNNING;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.assertions.Assert.assertEventually;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Test(singleThreaded = true)
public class TestRefreshMaterializedView
        extends AbstractTestQueryFramework
{
    private ListeningExecutorService executorService;
    private SettableFuture<Void> startRefreshMaterializedView;
    private SettableFuture<Void> finishRefreshMaterializedView;
    private SettableFuture<Void> refreshInterrupted;

    @BeforeClass
    public void setUp()
    {
        executorService = listeningDecorator(newCachedThreadPool());
    }

    @AfterClass(alwaysRun = true)
    public void shutdown()
    {
        executorService.shutdownNow();
    }

    @BeforeMethod
    public void resetState()
    {
        startRefreshMaterializedView = SettableFuture.create();
        finishRefreshMaterializedView = SettableFuture.create();
        refreshInterrupted = SettableFuture.create();
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog("mock")
                .setSchema("default")
                .build();
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session)
                .build();
        queryRunner.installPlugin(
                new MockConnectorPlugin(
                        MockConnectorFactory.builder()
                                .withListSchemaNames(connectionSession -> ImmutableList.of("default"))
                                .withGetColumns(schemaTableName -> ImmutableList.of(new ColumnMetadata("nationkey", BIGINT)))
                                .withGetTableHandle((connectorSession, tableName) -> new MockConnectorTableHandle(tableName))
                                .withGetMaterializedViews((connectorSession, schemaTablePrefix) -> ImmutableMap.of(
                                        new SchemaTableName("default", "delegate_refresh_to_connector"),
                                        new ConnectorMaterializedViewDefinition(
                                                "SELECT nationkey FROM mock.default.test_table",
                                                Optional.of(new CatalogSchemaTableName("mock", "default", "test_storage")),
                                                Optional.of("mock"),
                                                Optional.of("default"),
                                                ImmutableList.of(new ConnectorMaterializedViewDefinition.Column("nationkey", BIGINT.getTypeId())),
                                                Optional.empty(),
                                                Optional.of("alice"),
                                                ImmutableMap.of())))
                                .withDelegateMaterializedViewRefreshToConnector((connectorSession, schemaTableName) -> true)
                                .withRefreshMaterializedView(((connectorSession, schemaTableName) -> {
                                    startRefreshMaterializedView.set(null);
                                    SettableFuture<Void> refreshMaterializedView = SettableFuture.create();
                                    finishRefreshMaterializedView.addListener(() -> refreshMaterializedView.set(null), directExecutor());
                                    addExceptionCallback(refreshMaterializedView, () -> refreshInterrupted.set(null));
                                    return toCompletableFuture(refreshMaterializedView);
                                }))
                                .build()));
        queryRunner.createCatalog("mock", "mock");
        return queryRunner;
    }

    @Test(timeOut = 30_000)
    public void testDelegateRefreshMaterializedViewToConnector()
    {
        ListenableFuture<Void> queryFuture = assertUpdateAsync("REFRESH MATERIALIZED VIEW mock.default.delegate_refresh_to_connector");

        // wait for connector to start refreshing MV
        getFutureValue(startRefreshMaterializedView);

        // verify that the query eventually transitions to the RUNNING state
        QueryManager queryManager = getDistributedQueryRunner().getCoordinator().getQueryManager();
        assertEventually(() ->
                assertThat(
                        queryManager.getQueries().stream()
                                .allMatch(basicQueryInfo -> basicQueryInfo.getState() == RUNNING))
                        .isTrue());

        // finish MV refresh
        finishRefreshMaterializedView.set(null);

        getFutureValue(queryFuture);
    }

    @Test(timeOut = 30_000)
    public void testDelegateRefreshMaterializedViewToConnectorWithCancellation()
    {
        ListenableFuture<Void> queryFuture = assertUpdateAsync("REFRESH MATERIALIZED VIEW mock.default.delegate_refresh_to_connector");

        // wait for connector to start refreshing MV
        getFutureValue(startRefreshMaterializedView);

        // cancel refresh query
        QueryManager queryManager = getDistributedQueryRunner().getCoordinator().getQueryManager();
        queryManager.getQueries().forEach(query -> queryManager.cancelQuery(query.getQueryId()));

        assertThatThrownBy(() -> getFutureValue(queryFuture))
                .hasMessage("Query was canceled");
        getFutureValue(refreshInterrupted);
    }

    private ListenableFuture<Void> assertUpdateAsync(@Language("SQL") String sql)
    {
        return Futures.submit(() -> assertUpdate(sql), executorService);
    }
}
