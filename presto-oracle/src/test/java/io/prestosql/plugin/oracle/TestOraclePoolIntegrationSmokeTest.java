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
package io.prestosql.plugin.oracle;

import com.google.common.collect.ImmutableList;
import io.airlift.testing.Closeables;
import io.prestosql.testing.QueryRunner;
import io.prestosql.testing.sql.SqlExecutor;
import org.testng.annotations.AfterClass;

import java.io.IOException;

import static io.prestosql.tpch.TpchTable.CUSTOMER;
import static io.prestosql.tpch.TpchTable.NATION;
import static io.prestosql.tpch.TpchTable.ORDERS;
import static io.prestosql.tpch.TpchTable.REGION;

public class TestOraclePoolIntegrationSmokeTest
        extends BaseOracleIntegrationSmokeTest
{
    private TestingOracleServer oracleServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        oracleServer = new TestingOracleServer();
        return OracleQueryRunner.createOraclePoolQueryRunner(oracleServer, ImmutableList.of(CUSTOMER, NATION, ORDERS, REGION));
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
            throws IOException
    {
        Closeables.closeAll(oracleServer);
        oracleServer = null;
    }

    @Override
    protected SqlExecutor onOracle()
    {
        return oracleServer::execute;
    }
}
