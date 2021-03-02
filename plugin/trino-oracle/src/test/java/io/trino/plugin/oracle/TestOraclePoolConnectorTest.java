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
package io.trino.plugin.oracle;

import com.google.common.collect.ImmutableMap;
import io.airlift.testing.Closeables;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.SqlExecutor;
import io.trino.tpch.TpchTable;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.IOException;

import static io.trino.plugin.oracle.TestingOracleServer.TEST_PASS;
import static io.trino.plugin.oracle.TestingOracleServer.TEST_USER;
import static io.trino.testing.sql.TestTable.randomTableSuffix;
import static org.assertj.core.api.Assertions.assertThat;

public class TestOraclePoolConnectorTest
        extends BaseOracleConnectorTest
{
    private TestingOracleServer oracleServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        oracleServer = new TestingOracleServer();
        return OracleQueryRunner.createOracleQueryRunner(
                oracleServer,
                ImmutableMap.of(),
                ImmutableMap.<String, String>builder()
                        .put("connection-url", oracleServer.getJdbcUrl())
                        .put("connection-user", TEST_USER)
                        .put("connection-password", TEST_PASS)
                        .put("allow-drop-table", "true")
                        .put("oracle.connection-pool.enabled", "true")
                        .put("oracle.remarks-reporting.enabled", "false")
                        .build(),
                TpchTable.getTables());
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
            throws IOException
    {
        Closeables.closeAll(oracleServer);
        oracleServer = null;
    }

    @Test
    @Override
    public void testCommentColumn()
    {
        String tableName = "test_comment_column_" + randomTableSuffix();

        assertUpdate("CREATE TABLE " + tableName + "(a integer)");

        // comment set
        assertUpdate("COMMENT ON COLUMN " + tableName + ".a IS 'new comment'");
        // without remarksReporting Oracle does not return comments set
        assertThat((String) computeActual("SHOW CREATE TABLE " + tableName).getOnlyValue()).doesNotContain("COMMENT 'new comment'");
    }

    @Override
    protected SqlExecutor onOracle()
    {
        return oracleServer::execute;
    }
}
