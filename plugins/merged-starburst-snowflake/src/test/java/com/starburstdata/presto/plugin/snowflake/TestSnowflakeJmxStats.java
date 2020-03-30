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
package com.starburstdata.presto.plugin.snowflake;

import io.prestosql.testing.MaterializedResult;
import io.prestosql.testing.MaterializedRow;
import io.prestosql.tests.AbstractTestQueryFramework;
import org.testng.annotations.Test;

import static com.starburstdata.presto.plugin.snowflake.SnowflakeQueryRunner.distributedBuilder;
import static com.starburstdata.presto.plugin.snowflake.SnowflakeQueryRunner.oktaImpersonationEnabled;
import static org.testng.Assert.assertEquals;

public class TestSnowflakeJmxStats
        extends AbstractTestQueryFramework
{
    protected TestSnowflakeJmxStats()
    {
        super(() -> distributedBuilder()
                // using single worker instance, because workers overwrites their JMX stats
                .withAdditionalProperties(oktaImpersonationEnabled(false))
                .withNodeCount(1)
                .build());
    }

    @Test
    public void testAuthenticationJmxStats()
    {
        getQueryRunner().execute(getSession(), "SELECT * FROM orders LIMIT 1");
        getQueryRunner().execute(getSession(), "SELECT * FROM nation LIMIT 1");
        MaterializedResult rows = getQueryRunner().execute(
                getSession(),
                "SELECT \"authenticate.time.alltime.count\", \"obtainsamlassertion.time.alltime.count\" " +
                        "FROM jmx.current.\"presto.plugin.snowflake.auth:name=snowflake,type=statscollectingoktaauthclient\"");
        // we are running with 2 nodes
        assertEquals(rows.getRowCount(), 1);
        MaterializedRow oktaStatsRow = rows.getMaterializedRows().get(0);
        // We should have made each call to Okta only once; after that, the cache should have been used
        assertEquals(oktaStatsRow.getField(0), 1.0);
        assertEquals(oktaStatsRow.getField(1), 1.0);

        rows = getQueryRunner().execute(
                getSession(),
                "SELECT \"generatesamlrequest.time.alltime.count\", \"requestoauthtoken.time.alltime.count\" " +
                        "FROM jmx.current.\"presto.plugin.snowflake.auth:name=snowflake,type=statscollectingsnowflakeauthclient\"");
        assertEquals(rows.getRowCount(), 1);
        MaterializedRow snowflakeStatsRow = rows.getMaterializedRows().get(0);
        // We should have made each call to Snowflake only once; after that, the cache should have been used
        assertEquals(snowflakeStatsRow.getField(0), 1.0);
        assertEquals(snowflakeStatsRow.getField(1), 1.0);
    }
}
