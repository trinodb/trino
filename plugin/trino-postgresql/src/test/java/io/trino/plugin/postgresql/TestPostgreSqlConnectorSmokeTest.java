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
package io.trino.plugin.postgresql;

import io.trino.plugin.jdbc.BaseJdbcConnectorSmokeTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.TestTable;

import java.util.Map;

import static com.google.common.base.Verify.verify;

/**
 * Duplicates {@link TestPostgreSqlConnectorTest} but also verifies {@link BaseJdbcConnectorSmokeTest}
 * with a JDBC connector that is feature-reach.
 */
public class TestPostgreSqlConnectorSmokeTest
        extends BaseJdbcConnectorSmokeTest
{
    private TestingPostgreSqlServer postgreSqlServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        postgreSqlServer = closeAfterClass(new TestingPostgreSqlServer());
        return PostgreSqlQueryRunner.builder(postgreSqlServer)
                .addConnectorProperties(Map.of("postgresql.array-mapping", "AS_ARRAY"))
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .build();
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_PREDICATE_EXPRESSION_PUSHDOWN -> {
                // TODO remove once super has this set to true
                verify(!super.hasBehavior(connectorBehavior));
                yield true;
            }
            case SUPPORTS_CANCELLATION,
                 SUPPORTS_JOIN_PUSHDOWN,
                 SUPPORTS_TOPN_PUSHDOWN_WITH_VARCHAR -> true;
            case SUPPORTS_ADD_COLUMN_WITH_COMMENT,
                 SUPPORTS_CREATE_TABLE_WITH_COLUMN_COMMENT,
                 SUPPORTS_JOIN_PUSHDOWN_WITH_FULL_JOIN,
                 SUPPORTS_MAP_TYPE,
                 SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_INEQUALITY,
                 SUPPORTS_RENAME_TABLE_ACROSS_SCHEMAS,
                 SUPPORTS_ROW_TYPE -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Override
    protected TestTable createTestTableForWrites(String tablePrefix)
    {
        return new TestTable(
                postgreSqlServer::execute,
                tablePrefix,
                "(a bigint NOT NULL PRIMARY KEY, b double precision)");
    }
}
