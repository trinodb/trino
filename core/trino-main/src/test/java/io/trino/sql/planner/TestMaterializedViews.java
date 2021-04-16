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
package io.trino.sql.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.connector.CatalogName;
import io.trino.connector.informationschema.InformationSchemaConnector;
import io.trino.connector.system.SystemConnector;
import io.trino.metadata.Catalog;
import io.trino.metadata.InMemoryNodeManager;
import io.trino.metadata.InternalNodeManager;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition.Column;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.transaction.IsolationLevel;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.testing.LocalQueryRunner;
import io.trino.testing.TestingMetadata;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.trino.connector.CatalogName.createInformationSchemaCatalogName;
import static io.trino.connector.CatalogName.createSystemTablesCatalogName;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.testing.TestingSession.testSessionBuilder;

public class TestMaterializedViews
        extends BasePlanTest
{
    @Override
    protected LocalQueryRunner createLocalQueryRunner()
    {
        String catalog = "local";
        String schema = "tiny";
        Session.SessionBuilder sessionBuilder = testSessionBuilder()
                .setCatalog(catalog)
                .setSchema(schema)
                .setSystemProperty("task_concurrency", "1"); // these tests don't handle exchanges from local parallel

        LocalQueryRunner queryRunner = LocalQueryRunner.create(sessionBuilder.build());

        Catalog testCatalog = createTestingCatalog(catalog, new CatalogName(catalog), queryRunner);
        queryRunner.getCatalogManager().registerCatalog(testCatalog);
        TestingMetadata testingConnectorMetadata = (TestingMetadata) testCatalog.getConnector(new CatalogName(catalog)).getMetadata(null);

        Metadata metadata = queryRunner.getMetadata();
        SchemaTableName testTable = new SchemaTableName(schema, "test_table");
        queryRunner.inTransaction(session -> {
            metadata.createTable(
                    session,
                    catalog,
                    new ConnectorTableMetadata(testTable, ImmutableList.of(new ColumnMetadata("a", BIGINT))),
                    false);
            return null;
        });

        SchemaTableName storageTable = new SchemaTableName(schema, "storage_table");
        queryRunner.inTransaction(session -> {
            metadata.createTable(
                    session,
                    catalog,
                    new ConnectorTableMetadata(storageTable, ImmutableList.of(new ColumnMetadata("a", BIGINT))),
                    false);
            return null;
        });

        QualifiedObjectName freshMaterializedView = new QualifiedObjectName(catalog, schema, "fresh_materialized_view");
        ConnectorMaterializedViewDefinition materializedViewDefinition = new ConnectorMaterializedViewDefinition(
                "SELECT a FROM test_table",
                Optional.of(new CatalogSchemaTableName(catalog, schema, "storage_table")),
                Optional.of(catalog),
                Optional.of(schema),
                ImmutableList.of(new Column("a", BIGINT.getTypeId())),
                Optional.empty(),
                "some user",
                ImmutableMap.of());
        queryRunner.inTransaction(session -> {
            metadata.createMaterializedView(
                    session,
                    freshMaterializedView,
                    materializedViewDefinition,
                    false,
                    false);
            return null;
        });
        testingConnectorMetadata.markMaterializedViewIsFresh(freshMaterializedView.asSchemaTableName());

        QualifiedObjectName notFreshMaterializedView = new QualifiedObjectName(catalog, schema, "not_fresh_materialized_view");
        queryRunner.inTransaction(session -> {
            metadata.createMaterializedView(
                    session,
                    notFreshMaterializedView,
                    materializedViewDefinition,
                    false,
                    false);
            return null;
        });
        testingConnectorMetadata.markMaterializedViewIsFresh(freshMaterializedView.asSchemaTableName());

        return queryRunner;
    }

    @Test
    public void testFreshMaterializedView()
    {
        assertPlan("SELECT * FROM fresh_materialized_view",
                anyTree(
                        tableScan("storage_table")));
    }

    @Test
    public void testNotFreshMaterializedView()
    {
        assertPlan("SELECT * FROM not_fresh_materialized_view",
                anyTree(
                        tableScan("test_table")));
    }

    private Catalog createTestingCatalog(String catalogName, CatalogName catalog, LocalQueryRunner queryRunner)
    {
        CatalogName systemId = createSystemTablesCatalogName(catalog);
        Connector connector = createTestingConnector();
        InternalNodeManager nodeManager = new InMemoryNodeManager();
        return new Catalog(
                catalogName,
                catalog,
                connector,
                createInformationSchemaCatalogName(catalog),
                new InformationSchemaConnector(catalogName, nodeManager, queryRunner.getMetadata(), queryRunner.getAccessControl()),
                systemId,
                new SystemConnector(
                        nodeManager,
                        connector.getSystemTables(),
                        transactionId -> queryRunner.getTransactionManager().getConnectorTransaction(transactionId, catalog)));
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
        };
    }
}
