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
import org.testng.annotations.Test;

import static io.trino.plugin.mysql.MySqlQueryRunner.createMySqlQueryRunner;
import static java.util.stream.Collectors.joining;
import static java.util.stream.IntStream.range;

public class TestMySqlConnectorTest
        extends BaseMySqlConnectorTest
{
    private TestingMySqlServer mySqlServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        mySqlServer = closeAfterClass(new TestingMySqlServer(false));
        return createMySqlQueryRunner(mySqlServer, ImmutableMap.of(), ImmutableMap.of(), REQUIRED_TPCH_TABLES);
    }

    @Override
    protected SqlExecutor onRemoteDatabase()
    {
        return mySqlServer::execute;
    }

    /**
     * This test helps to tune TupleDomain simplification threshold.
     */
    @Test
    public void testNativeLargeIn()
    {
        // Using IN list of size 140_000 as bigger list causes error:
        // "com.mysql.jdbc.PacketTooBigException: Packet for query is too large (XXX > 1048576).
        //  You can change this value on the server by setting the max_allowed_packet' variable."
        mySqlServer.execute("SELECT count(*) FROM tpch.orders WHERE " + getLongInClause(0, 140_000));
    }

    /**
     * This test helps to tune TupleDomain simplification threshold.
     */
    @Test
    public void testNativeMultipleInClauses()
    {
        String longInClauses = range(0, 14)
                .mapToObj(value -> getLongInClause(value * 10_000, 10_000))
                .collect(joining(" OR "));
        mySqlServer.execute("SELECT count(*) FROM tpch.orders WHERE " + longInClauses);
    }

    private String getLongInClause(int start, int length)
    {
        String longValues = range(start, start + length)
                .mapToObj(Integer::toString)
                .collect(joining(", "));
        return "orderkey IN (" + longValues + ")";
    }
}
