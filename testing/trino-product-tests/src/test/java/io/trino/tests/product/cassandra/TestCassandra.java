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
package io.trino.tests.product.cassandra;

import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.QueryResult;
import io.trino.testing.containers.environment.QueryResultAssert;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.testing.containers.environment.TpchTableResults;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Cassandra CREATE TABLE AS SELECT test.
 */
@ProductTest
@RequiresEnvironment(CassandraEnvironment.class)
@TestGroup.Cassandra
@TestGroup.ProfileSpecificTests
class TestCassandra
{
    @Test
    void testCreateTableAsSelect(CassandraEnvironment env)
            throws Exception
    {
        try (Connection conn = env.createTrinoConnection();
                Statement stmt = conn.createStatement()) {
            // Keyspace 'test' is created by CassandraEnvironment during startup
            int count = stmt.executeUpdate("CREATE TABLE cassandra.test.nation AS SELECT * FROM tpch.tiny.nation");
            try {
                assertThat(count).isEqualTo(TpchTableResults.NATION_ROW_COUNT);

                // Verify the data was created with full row validation
                try (ResultSet rs = stmt.executeQuery("SELECT * FROM cassandra.test.nation")) {
                    QueryResult result = QueryResult.forResultSet(rs);
                    QueryResultAssert.assertThat(result).containsOnly(TpchTableResults.NATION_ROWS);
                }
            }
            finally {
                stmt.execute("DROP TABLE cassandra.test.nation");
            }
        }
    }
}
