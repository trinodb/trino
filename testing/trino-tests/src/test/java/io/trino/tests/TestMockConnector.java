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
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorPlugin;
import io.trino.connector.MockConnectorTableHandle;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.procedure.TestProcedure;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition.Column;
import io.trino.spi.connector.SchemaTableName;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.trino.connector.MockConnectorEntities.TPCH_NATION_DATA;
import static io.trino.connector.MockConnectorEntities.TPCH_NATION_SCHEMA;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestMockConnector
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(testSessionBuilder().build()).build();

        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");
        queryRunner.installPlugin(
                new MockConnectorPlugin(
                        MockConnectorFactory.builder()
                                .withListSchemaNames(connectionSession -> ImmutableList.of("default"))
                                .withGetColumns(schemaTableName -> {
                                    if (schemaTableName.equals(new SchemaTableName("default", "nation"))) {
                                        return TPCH_NATION_SCHEMA;
                                    }
                                    return ImmutableList.of(new ColumnMetadata("nationkey", BIGINT));
                                })
                                .withGetTableHandle((session, tableName) -> new MockConnectorTableHandle(tableName))
                                .withGetMaterializedViews((session, schemaTablePrefix) -> ImmutableMap.of(
                                        new SchemaTableName("default", "test_materialized_view"),
                                        new ConnectorMaterializedViewDefinition(
                                                "SELECT nationkey FROM mock.default.test_table",
                                                Optional.of(new CatalogSchemaTableName("mock", "default", "test_storage")),
                                                Optional.of("mock"),
                                                Optional.of("default"),
                                                ImmutableList.of(new Column("nationkey", BIGINT.getTypeId())),
                                                Optional.empty(),
                                                "alice",
                                                ImmutableMap.of())))
                                .withData(schemaTableName -> {
                                    if (schemaTableName.equals(new SchemaTableName("default", "nation"))) {
                                        return TPCH_NATION_DATA;
                                    }
                                    throw new UnsupportedOperationException();
                                })
                                .withProcedures(ImmutableSet.of(new TestProcedure().get()))
                                .build()));
        queryRunner.createCatalog("mock", "mock");
        return queryRunner;
    }

    @Test
    public void testCreateSchema()
    {
        assertUpdate("CREATE SCHEMA mock.new_schema");
    }

    @Test
    public void testDropSchema()
    {
        assertUpdate("DROP SCHEMA mock.default");
    }

    @Test
    public void testRenameSchema()
    {
        assertUpdate("ALTER SCHEMA mock.default RENAME to renamed");
    }

    @Test
    public void testCreateMaterializedView()
    {
        assertUpdate("CREATE MATERIALIZED VIEW mock.default.materialized_view AS SELECT * FROM tpch.tiny.nation");
    }

    @Test
    public void testRefreshMaterializedView()
    {
        assertUpdate("REFRESH MATERIALIZED VIEW mock.default.test_materialized_view", 0);
    }

    @Test
    public void testDropMaterializedView()
    {
        assertUpdate("DROP MATERIALIZED VIEW mock.default.test_materialized_view");
    }

    @Test
    public void testDataGeneration()
    {
        assertQuery(
                "SELECT * FROM mock.default.nation",
                "SELECT * FROM nation");
        assertQuery(
                "SELECT nationkey FROM mock.default.nation",
                "SELECT nationkey FROM nation");
        assertQuery(
                "SELECT nationkey, regionkey FROM mock.default.nation",
                "SELECT nationkey, regionkey FROM nation");
        assertQuery(
                "SELECT regionkey, nationkey FROM mock.default.nation",
                "SELECT regionkey, nationkey FROM nation");
        assertQuery(
                "SELECT regionkey FROM mock.default.nation",
                "SELECT regionkey FROM nation");
    }

    @Test
    public void testInsert()
    {
        assertQuery("SELECT count(*) FROM mock.default.nation", "SELECT 25");
        assertUpdate("INSERT INTO mock.default.nation VALUES (101, 'POLAND', 0, 'No comment')", 1);
        // Mock connector only pretends support for INSERT, it does not manipulate any data
        assertQuery("SELECT count(*) FROM mock.default.nation", "SELECT 25");
    }

    @Test
    public void testDelete()
    {
        assertQuery("SELECT count(*) FROM mock.default.nation", "SELECT 25");
        assertUpdate("DELETE FROM mock.default.nation", 25);
        assertUpdate("DELETE FROM mock.default.nation WHERE nationkey = 1", 1);
        assertUpdate("DELETE FROM mock.default.nation WHERE false", 0);
        // Mock connector only pretends support for DELETE, it does not manipulate any data
        assertQuery("SELECT count(*) FROM mock.default.nation", "SELECT 25");
    }

    @Test
    public void testUpdate()
    {
        assertQuery("SELECT count(*) FROM mock.default.nation WHERE name = 'ALGERIA'", "SELECT 1");
        assertUpdate("UPDATE mock.default.nation SET name = 'ALGERIA'", 25);
        assertUpdate("UPDATE mock.default.nation SET name = 'ALGERIA' WHERE nationkey = 1", 1);
        assertThatThrownBy(() -> assertUpdate("UPDATE mock.default.nation SET name = 'x' WHERE false", 0))
                // TODO https://github.com/trinodb/trino/issues/8855 - UPDATE with WHERE false currently is not supported
                .hasMessage("Invalid descendant for DeleteNode or UpdateNode: io.trino.sql.planner.plan.ExchangeNode");
        // Mock connector only pretends support for UPDATE, it does not manipulate any data
        assertQuery("SELECT count(*) FROM mock.default.nation WHERE name = 'ALGERIA'", "SELECT 1");
    }

    @Test
    public void testProcedure()
    {
        assertUpdate("CALL mock.default.test_procedure()");
        assertThatThrownBy(() -> assertUpdate("CALL mock.default.non_exist_procedure()"))
                .hasMessage("Procedure not registered: default.non_exist_procedure");
    }
}
