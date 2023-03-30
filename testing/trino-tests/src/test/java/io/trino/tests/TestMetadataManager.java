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
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorTableHandle;
import io.trino.dispatcher.DispatchManager;
import io.trino.metadata.MetadataManager;
import io.trino.metadata.QualifiedTablePrefix;
import io.trino.server.BasicQueryInfo;
import io.trino.server.protocol.Slug;
import io.trino.spi.Plugin;
import io.trino.spi.QueryId;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.SchemaTableName;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.TestingSessionContext;
import io.trino.tests.tpch.TpchQueryRunnerBuilder;
import io.trino.transaction.TransactionBuilder;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.execution.QueryState.FAILED;
import static io.trino.execution.QueryState.RUNNING;
import static io.trino.spi.type.BigintType.BIGINT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

/**
 * This is integration / unit test suite.
 * The reason for having it here is to ensure that we won't leak memory in MetadataManager
 * while registering catalog -> query Id mapping.
 * This mapping has to be manually cleaned when query finishes execution (Metadata#cleanupQuery method).
 */
@Test(singleThreaded = true)
public class TestMetadataManager
{
    private DistributedQueryRunner queryRunner;
    private MetadataManager metadataManager;

    @BeforeClass
    public void setUp()
            throws Exception
    {
        queryRunner = TpchQueryRunnerBuilder.builder().build();
        queryRunner.installPlugin(new Plugin()
        {
            @Override
            public Iterable<ConnectorFactory> getConnectorFactories()
            {
                SchemaTableName viewTableName = new SchemaTableName("UPPER_CASE_SCHEMA", "test_view");

                MockConnectorFactory connectorFactory = MockConnectorFactory.builder()
                        .withListSchemaNames(session -> ImmutableList.of("UPPER_CASE_SCHEMA"))
                        .withGetTableHandle((session, schemaTableName) -> {
                            if (schemaTableName.equals(viewTableName)) {
                                return null;
                            }

                            return new MockConnectorTableHandle(schemaTableName);
                        })
                        .withListTables((session, schemaName) -> ImmutableList.of("UPPER_CASE_TABLE"))
                        .withGetViews((session, prefix) -> ImmutableMap.of(viewTableName, getConnectorViewDefinition()))
                        .build();
                return ImmutableList.of(connectorFactory);
            }
        });
        queryRunner.createCatalog("upper_case_schema_catalog", "mock");
        metadataManager = (MetadataManager) queryRunner.getMetadata();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        queryRunner.close();
        queryRunner = null;
        metadataManager = null;
    }

    @Test
    public void testMetadataIsClearedAfterQueryFinished()
    {
        @Language("SQL") String sql = "SELECT * FROM nation";
        queryRunner.execute(sql);

        assertEquals(metadataManager.getActiveQueryIds().size(), 0);
    }

    @Test
    public void testMetadataIsClearedAfterQueryFailed()
    {
        @Language("SQL") String sql = "SELECT nationkey/0 FROM nation"; // will raise division by zero exception
        assertThatThrownBy(() -> queryRunner.execute(sql))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Division by zero");

        assertEquals(metadataManager.getActiveQueryIds().size(), 0);
    }

    @Test
    public void testMetadataListTablesReturnsQualifiedView()
    {
        TransactionBuilder.transaction(queryRunner.getTransactionManager(), queryRunner.getAccessControl())
                .execute(
                        TEST_SESSION,
                        transactionSession -> {
                            QualifiedTablePrefix viewName = new QualifiedTablePrefix("upper_case_schema_catalog", "upper_case_schema", "test_view");
                            assertThat(metadataManager.listTables(transactionSession, viewName)).containsExactly(viewName.asQualifiedObjectName().get());
                        });
    }

    @Test
    public void testMetadataIsClearedAfterQueryCanceled()
            throws Exception
    {
        DispatchManager dispatchManager = queryRunner.getCoordinator().getDispatchManager();
        QueryId queryId = dispatchManager.createQueryId();
        dispatchManager.createQuery(
                queryId,
                Slug.createNew(),
                TestingSessionContext.fromSession(TEST_SESSION),
                "SELECT * FROM lineitem")
                .get();

        // wait until query starts running
        while (true) {
            BasicQueryInfo queryInfo = dispatchManager.getQueryInfo(queryId);
            if (queryInfo.getState().isDone()) {
                assertEquals(queryInfo.getState(), FAILED);
                throw dispatchManager.getDispatchInfo(queryId).get().getFailureInfo().get().toException();
            }
            if (queryInfo.getState() == RUNNING) {
                break;
            }
            Thread.sleep(100);
        }

        // cancel query
        dispatchManager.cancelQuery(queryId);
        assertEquals(metadataManager.getActiveQueryIds().size(), 0);
    }

    @Test
    public void testUpperCaseSchemaIsChangedToLowerCase()
    {
        TransactionBuilder.transaction(queryRunner.getTransactionManager(), queryRunner.getAccessControl())
                .execute(
                        TEST_SESSION,
                        transactionSession -> {
                            List<String> expectedSchemas = ImmutableList.of("information_schema", "upper_case_schema");
                            assertEquals(queryRunner.getMetadata().listSchemaNames(transactionSession, "upper_case_schema_catalog"), expectedSchemas);
                            return null;
                        });
    }

    @Test
    public void testUpperCaseListTablesFilter()
    {
        // TODO (https://github.com/trinodb/trino/issues/17) this should return no rows
        assertThat(queryRunner.execute("SELECT * FROM system.jdbc.tables WHERE TABLE_SCHEM = 'upper_case_schema' AND TABLE_NAME = 'upper_case_table'"))
                .hasSize(1);
        // TODO (https://github.com/trinodb/trino/issues/17) this should return 1 row
        assertThat(queryRunner.execute("SELECT * FROM system.jdbc.tables WHERE TABLE_SCHEM = 'UPPER_CASE_SCHEMA'"))
                .isEmpty();
        // TODO (https://github.com/trinodb/trino/issues/17) this should return 1 row
        assertThat(queryRunner.execute("SELECT * FROM system.jdbc.tables WHERE TABLE_NAME = 'UPPER_CASE_TABLE'"))
                .isEmpty();
    }

    @Test
    public void testColumnsQueryWithUpperCaseFilter()
    {
        // TODO (https://github.com/trinodb/trino/issues/17) this should return no rows
        assertThat(queryRunner.execute("SELECT * FROM system.jdbc.columns WHERE table_schem = 'upper_case_schema' AND table_name = 'upper_case_table'"))
                .hasSize(100);
        // TODO (https://github.com/trinodb/trino/issues/17) this should return 100 rows
        assertThat(queryRunner.execute("SELECT * FROM system.jdbc.columns WHERE table_schem = 'UPPER_CASE_SCHEMA'"))
                .isEmpty();
        // TODO (https://github.com/trinodb/trino/issues/17) this should return 100 rows
        assertThat(queryRunner.execute("SELECT * FROM system.jdbc.columns WHERE table_name = 'UPPER_CASE_TABLE'"))
                .isEmpty();
        // TODO (https://github.com/trinodb/trino/issues/17) this should return 100 rows
        assertThat(queryRunner.execute("SELECT * FROM system.jdbc.columns WHERE table_schem = 'UPPER_CASE_TABLE' AND table_name = 'UPPER_CASE_TABLE'"))
                .isEmpty();
    }

    private static ConnectorViewDefinition getConnectorViewDefinition()
    {
        return new ConnectorViewDefinition(
                "test view SQL",
                Optional.of("upper_case_schema_catalog"),
                Optional.of("upper_case_schema"),
                ImmutableList.of(new ConnectorViewDefinition.ViewColumn("col", BIGINT.getTypeId(), Optional.empty())),
                Optional.of("comment"),
                Optional.of("test_owner"),
                false);
    }
}
