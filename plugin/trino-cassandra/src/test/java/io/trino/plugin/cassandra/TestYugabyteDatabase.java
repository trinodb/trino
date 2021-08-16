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
package io.trino.plugin.cassandra;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import static io.trino.plugin.cassandra.CassandraTestingUtils.createKeyspace;
import static io.trino.plugin.cassandra.YugabyteQueryRunner.createYugabyteQueryRunner;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestYugabyteDatabase
        extends AbstractTestQueryFramework
{
    private static final String KEYSPACE = "smoke_test";

    private CassandraSession session;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        TestingYugabyteServer server = closeAfterClass(new TestingYugabyteServer());
        session = server.getSession();
        createKeyspace(session, KEYSPACE);
        return createYugabyteQueryRunner(
                server,
                ImmutableMap.of(),
                ImmutableMap.of("cassandra.session-refresh-interval", "5s"));
    }

    @Test
    public void testNewSchemaInYugabyte()
            throws Exception
    {
        String schemaName = "test_new_schema";

        onYugabyte("DROP KEYSPACE IF EXISTS " + schemaName);
        assertQueryReturnsEmptyResult(format("SHOW SCHEMAS FROM yugabyte LIKE '%s'", schemaName));

        onYugabyte("CREATE SCHEMA " + schemaName);

        assertQueryEventually(getSession(),
                format("SHOW SCHEMAS FROM yugabyte LIKE '%s'", schemaName),
                format("VALUES ('%s')", schemaName),
                new Duration(10, SECONDS));

        onYugabyte("DROP KEYSPACE " + schemaName);
    }

    @Test
    public void testNewTableInYugabyte()
            throws Exception
    {
        String tableName = "test_new_table";

        onYugabyte("DROP TABLE IF EXISTS smoke_test." + tableName);
        assertQueryReturnsEmptyResult(format("SHOW TABLES FROM smoke_test LIKE '%s'", tableName));

        onYugabyte(format("CREATE TABLE smoke_test.%s (id INT PRIMARY KEY)", tableName));

        assertQueryEventually(getSession(),
                format("SHOW TABLES FROM smoke_test LIKE '%s'", tableName),
                format("VALUES ('%s')", tableName),
                new Duration(10, SECONDS));

        onYugabyte("DROP TABLE smoke_test." + tableName);
    }

    private void onYugabyte(@Language("SQL") String sql)
    {
        session.execute(sql);
    }
}
