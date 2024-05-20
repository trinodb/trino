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
package io.trino.plugin.postgresql;

import io.trino.plugin.jdbc.BaseAutomaticJoinPushdownTest;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class TestPostgreSqlAutomaticJoinPushdown
        extends BaseAutomaticJoinPushdownTest
{
    private TestingPostgreSqlServer postgreSqlServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.postgreSqlServer = closeAfterClass(new TestingPostgreSqlServer());
        return PostgreSqlQueryRunner.builder(postgreSqlServer)
                .build();
    }

    @Test
    @Override
    @Disabled
    public void testJoinPushdownWithEmptyStatsInitially()
    {
        // PostgreSQL automatically collects stats for newly created tables via the autovacuum daemon and this cannot be disabled reliably
    }

    @Override
    protected void gatherStats(String tableName)
    {
        postgreSqlServer.execute("ANALYZE tpch." + tableName);
    }
}
