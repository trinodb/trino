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
import org.testng.annotations.AfterClass;

import static io.trino.plugin.oracle.TestingOracleServer.TEST_PASS;
import static io.trino.plugin.oracle.TestingOracleServer.TEST_USER;

public class TestOraclePoolConnectorSmokeTest
        extends BaseOracleConnectorSmokeTest
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
                        .put("oracle.connection-pool.enabled", "true")
                        .buildOrThrow(),
                REQUIRED_TPCH_TABLES);
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
            throws Exception
    {
        Closeables.closeAll(oracleServer);
        oracleServer = null;
    }
}
