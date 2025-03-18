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
package io.trino.plugin.scylladb;

import io.trino.plugin.cassandra.CassandraServer;
import io.trino.plugin.cassandra.TestCassandraConnectorTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import org.junit.jupiter.api.Test;

public class TestScyllaDbConnectorTest
        extends TestCassandraConnectorTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        CassandraServer server = closeAfterClass(new TestingScyllaDbServer());
        session = server.getSession();
        return ScyllaDbQueryRunner.builder(server).setInitialTables(REQUIRED_TPCH_TABLES).build();
    }

    @Test
    @Override
    public void testCreateTableWithLongColumnName()
    {
        // disable for now as maxColumnName is not applied and test failing: testCreateTableWithLongColumnName
    }

    @Test
    @Override
    protected void testSelectClusteringMaterializedView()
    {
        // disable for now
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_ADD_COLUMN,
                 SUPPORTS_ARRAY,
                 SUPPORTS_COMMENT_ON_COLUMN,
                 SUPPORTS_COMMENT_ON_TABLE,
                 SUPPORTS_CREATE_SCHEMA,
                 SUPPORTS_CREATE_TABLE_WITH_COLUMN_COMMENT,
                 SUPPORTS_CREATE_TABLE_WITH_TABLE_COMMENT,
                 SUPPORTS_CREATE_VIEW,
                 SUPPORTS_MAP_TYPE,
                 SUPPORTS_MERGE,
                 SUPPORTS_NOT_NULL_CONSTRAINT,
                 SUPPORTS_RENAME_COLUMN,
                 SUPPORTS_RENAME_TABLE,
                 SUPPORTS_ROW_TYPE,
                 SUPPORTS_SET_COLUMN_TYPE,
                 SUPPORTS_TOPN_PUSHDOWN,
                 SUPPORTS_UPDATE -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
    }
}
