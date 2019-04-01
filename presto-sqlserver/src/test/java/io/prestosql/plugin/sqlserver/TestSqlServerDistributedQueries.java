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
package io.prestosql.plugin.sqlserver;

import com.google.common.collect.ImmutableMap;
import io.airlift.tpch.TpchTable;
import io.prestosql.tests.AbstractTestDistributedQueries;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import static io.prestosql.plugin.sqlserver.SqlServerQueryRunner.createSqlServerQueryRunner;

@Test
public class TestSqlServerDistributedQueries
        extends AbstractTestDistributedQueries
{
    private final TestingSqlServer sqlServer;

    public TestSqlServerDistributedQueries()
    {
        this(new TestingSqlServer());
    }

    public TestSqlServerDistributedQueries(TestingSqlServer testingSqlServer)
    {
        super(() -> createSqlServerQueryRunner(testingSqlServer, ImmutableMap.of(), TpchTable.getTables()));
        this.sqlServer = testingSqlServer;
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        sqlServer.close();
    }

    @Override
    protected boolean supportsViews()
    {
        return false;
    }

    @Override
    protected boolean supportsArrays()
    {
        return false;
    }

    @Override
    public void testCommentTable()
    {
        // SQLServer connector currently does not support comment on table
        assertQueryFails("COMMENT ON TABLE orders IS 'hello'", "This connector does not support setting table comments");
    }

    @Override
    public void testDelete()
    {
        // delete is not supported
    }

    // SQLServer specific tests should normally go in TestSqlServerIntegrationSmokeTest
}
