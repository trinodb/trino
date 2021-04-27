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
import io.trino.connector.MockConnectorPlugin;
import io.trino.connector.MockConnectorTableHandle;
import io.trino.plugin.tpch.TpchPlugin;
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

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.TestingSession.testSessionBuilder;

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
                                .withGetColumns(schemaTableName -> ImmutableList.of(new ColumnMetadata("nationkey", BIGINT)))
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
}
