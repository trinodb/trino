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
package io.trino.plugin.sybase;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.jdbc.BaseJdbcConnectorTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.SqlExecutor;
import org.testng.annotations.AfterClass;

import static io.trino.plugin.sybase.SybaseQueryRunner.createSybaseQueryRunner;

public class TestSybaseConnectorTest
        extends BaseJdbcConnectorTest
{
    protected TestingSybaseServer sybaseServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        sybaseServer = new TestingSybaseServer();
        return createSybaseQueryRunner(sybaseServer, ImmutableMap.of(), ImmutableMap.of(), REQUIRED_TPCH_TABLES);
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        sybaseServer.close();
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_CREATE_TABLE:
            case SUPPORTS_JOIN_PUSHDOWN_WITH_FULL_JOIN:
            case SUPPORTS_JOIN_PUSHDOWN_WITH_DISTINCT_FROM:
            case SUPPORTS_JOIN_PUSHDOWN:
            case SUPPORTS_COMMENT_ON_TABLE:
            case SUPPORTS_COMMENT_ON_COLUMN:
            case SUPPORTS_ARRAY:
            case SUPPORTS_RENAME_TABLE_ACROSS_SCHEMAS:
            case SUPPORTS_CREATE_TABLE_WITH_DATA:
            case SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_INEQUALITY:
            case SUPPORTS_INSERT:
            case SUPPORTS_CREATE_SCHEMA:
            case SUPPORTS_AGGREGATION_PUSHDOWN:
            case SUPPORTS_TOPN_PUSHDOWN:
                return false;

            default:
                return super.hasBehavior(connectorBehavior);
        }
    }

    private void execute(String sql)
    {
        sybaseServer.execute(sql);
    }

    @Override
    protected SqlExecutor onRemoteDatabase()
    {
        return sybaseServer::execute;
    }
}
