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
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.testng.annotations.Test;

import java.sql.Timestamp;

import static io.trino.plugin.cassandra.CassandraQueryRunner.createCassandraQueryRunner;
import static io.trino.plugin.cassandra.CassandraTestingUtils.createTestTables;
import static org.assertj.core.api.Assertions.assertThat;

public class TestCassandraProtocolVersionV3ConnectorSmokeTest
        extends BaseCassandraConnectorSmokeTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        CassandraServer server = closeAfterClass(new CassandraServer());
        CassandraSession session = server.getSession();
        createTestTables(session, KEYSPACE, Timestamp.from(TIMESTAMP_VALUE.toInstant()));
        return createCassandraQueryRunner(server, ImmutableMap.of(), ImmutableMap.of("cassandra.protocol-version", "V3"), REQUIRED_TPCH_TABLES);
    }

    @Test
    @Override
    public void testInsertDate()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_insert_", "(a_date date)")) {
            assertUpdate("INSERT INTO " + table.getName() + " (a_date) VALUES ('2020-05-11')", 1);
            assertThat(query("SELECT a_date FROM " + table.getName())).matches("VALUES (CAST('2020-05-11' AS varchar))");
        }
    }
}
