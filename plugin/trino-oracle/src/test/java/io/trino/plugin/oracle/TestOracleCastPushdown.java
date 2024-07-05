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
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.SqlExecutor;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestOracleCastPushdown
        extends BaseOracleCastPushdownTest
{
    private TestingOracleServer oracleServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        oracleServer = closeAfterClass(new TestingOracleServer());
        return OracleQueryRunner.builder(oracleServer)
                .addConnectorProperties(ImmutableMap.<String, String>builder()
                        .put("jdbc-types-mapped-to-varchar", "interval year(2) to month, timestamp(6) with local time zone")
                        .put("join-pushdown.enabled", "true")
                        .buildOrThrow())
                .build();
    }

    @Override
    protected SqlExecutor onRemoteDatabase()
    {
        return new SqlExecutor()
        {
            @Override
            public boolean supportsMultiRowInsert()
            {
                return false;
            }

            @Override
            public void execute(String sql)
            {
                oracleServer.execute(sql);
            }
        };
    }
}
