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

import io.trino.plugin.jdbc.BaseAutomaticJoinPushdownTest;
import io.trino.testing.QueryRunner;

import static io.trino.plugin.oracle.TestingOracleServer.TEST_USER;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;

public class TestOracleAutomaticJoinPushdown
        extends BaseAutomaticJoinPushdownTest
{
    private TestingOracleServer oracleServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.oracleServer = closeAfterClass(new TestingOracleServer());
        return OracleQueryRunner.builder(oracleServer)
                .build();
    }

    @Override
    protected void gatherStats(String tableName)
    {
        oracleServer.execute(format(
                "BEGIN DBMS_STATS.GATHER_TABLE_STATS(ownname => '%s', tabname => '%s', estimate_percent => 100, method_opt => 'FOR ALL COLUMNS SIZE 1'); END;",
                TEST_USER.toUpperCase(ENGLISH),
                tableName.replace("\"", "").toUpperCase(ENGLISH)));
    }
}
