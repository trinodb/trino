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
import io.trino.operator.RetryPolicy;
import io.trino.plugin.exchange.filesystem.FileSystemExchangePlugin;
import io.trino.plugin.jdbc.BaseJdbcFailureRecoveryTest;
import io.trino.testing.QueryRunner;
import io.trino.testng.services.Flaky;
import io.trino.tpch.TpchTable;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;

import static io.trino.plugin.oracle.OracleQueryRunner.createOracleQueryRunner;
import static io.trino.plugin.oracle.TestingOracleServer.TEST_PASS;
import static io.trino.plugin.oracle.TestingOracleServer.TEST_USER;

public abstract class BaseOracleFailureRecoveryTest
        extends BaseJdbcFailureRecoveryTest
{
    public BaseOracleFailureRecoveryTest(RetryPolicy retryPolicy)
    {
        super(retryPolicy);
    }

    @Override
    protected QueryRunner createQueryRunner(
            List<TpchTable<?>> requiredTpchTables,
            Map<String, String> configProperties,
            Map<String, String> coordinatorProperties)
            throws Exception
    {
        TestingOracleServer oracleServer = new TestingOracleServer();
        return createOracleQueryRunner(
                closeAfterClass(oracleServer),
                configProperties,
                coordinatorProperties,
                ImmutableMap.<String, String>builder()
                        .put("connection-url", oracleServer.getJdbcUrl())
                        .put("connection-user", TEST_USER)
                        .put("connection-password", TEST_PASS)
                        .buildOrThrow(),
                requiredTpchTables,
                runner -> {
                    runner.installPlugin(new FileSystemExchangePlugin());
                    runner.loadExchangeManager("filesystem", ImmutableMap.of(
                            "exchange.base-directories", System.getProperty("java.io.tmpdir") + "/trino-local-file-system-exchange-manager"));
                });
    }

    @Override
    @Flaky(issue = "https://github.com/trinodb/trino/issues/16277", match = "There should be no remaining tmp_trino tables that are queryable")
    @Test(dataProvider = "parallelTests")
    public void testParallel(Runnable runnable)
    {
        super.testParallel(runnable);
    }
}
