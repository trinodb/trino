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
package io.trino.tests;

import io.trino.Session;
import io.trino.spi.type.TimeZoneKey;
import io.trino.testing.AbstractTestAggregations;
import io.trino.testing.QueryRunner;
import io.trino.tests.tpch.TpchQueryRunnerBuilder;
import org.testng.annotations.Test;

public class TestAggregations
        extends AbstractTestAggregations
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return TpchQueryRunnerBuilder.builder().build();
    }

    @Test
    public void testGroupByWithNullsSimple()
    {
        assertQuery("SELECT key, COUNT(*) FROM (" +
                "VALUES NULL, 1, NULL, 1) t(key) GROUP BY key");
    }

    @Test
    public void testGroupBySumSimple()
    {
        assertQuery("SELECT key, SUM(CAST(key AS BIGINT)) FROM (VALUES NULL, 1, NULL, 1) t(key) GROUP BY key");
    }

    @Test
    public void testGroupBySumTpchSimple()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty("task_concurrency", "1")
                .build();
        assertQuery(session, "SELECT suppkey, SUM(CAST(quantity AS BIGINT)) FROM lineitem WHERE suppkey = 2 GROUP BY suppkey");
    }
}
