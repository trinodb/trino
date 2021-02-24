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
package io.trino.plugin.mysql;

import com.google.common.collect.ImmutableMap;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.SqlExecutor;

import static io.trino.plugin.mysql.MySqlQueryRunner.createMySqlQueryRunner;

public class TestGlobalTransactionMySqlConnectorTest
        // TODO(https://github.com/trinodb/trino/issues/7019) define shorter tests set that exercises various read and write scenarios (a.k.a. "a smoke test")
        extends BaseMySqlConnectorTest
{
    private TestingMySqlServer mysqlServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        mysqlServer = closeAfterClass(new TestingMySqlServer(true));
        return createMySqlQueryRunner(mysqlServer, ImmutableMap.of(), ImmutableMap.of(), REQUIRED_TPCH_TABLES);
    }

    @Override
    protected SqlExecutor getMySqlExecutor()
    {
        return mysqlServer::execute;
    }
}
