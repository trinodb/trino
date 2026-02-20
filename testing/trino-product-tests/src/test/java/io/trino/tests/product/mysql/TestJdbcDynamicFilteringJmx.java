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
package io.trino.tests.product.mysql;

import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests JDBC dynamic filtering with JMX metrics verification.
 */
@ProductTest
@RequiresEnvironment(MySqlEnvironment.class)
@TestGroup.Mysql
@TestGroup.ProfileSpecificTests
class TestJdbcDynamicFilteringJmx
{
    private static final String TABLE_NAME = "nation_df_tmp";

    @Test
    void testDynamicFilteringStats(MySqlEnvironment environment)
            throws Exception
    {
        // Use a single connection for all queries so SET SESSION persists
        try (Connection conn = environment.createTrinoConnection();
                Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS mysql.test." + TABLE_NAME);
            // Create test table
            int count = stmt.executeUpdate(
                    "CREATE TABLE mysql.test." + TABLE_NAME + " AS SELECT * FROM tpch.tiny.nation");
            assertThat(count).isEqualTo(25);

            // Configure session for dynamic filtering test
            stmt.execute("SET SESSION mysql.dynamic_filtering_wait_timeout = '1h'");
            stmt.execute("SET SESSION join_reordering_strategy = 'NONE'");
            stmt.execute("SET SESSION join_distribution_type = 'BROADCAST'");

            // Execute join query that triggers dynamic filtering
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) FROM mysql.test." + TABLE_NAME + " a " +
                    "JOIN tpch.tiny.nation b ON a.nationkey = b.nationkey AND b.name = 'INDIA'")) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getLong(1)).isEqualTo(1);
            }

            // Verify dynamic filtering stats via JMX
            long completedCount = getJmxCount(stmt, "completeddynamicfilters.totalcount");
            assertThat(completedCount).isEqualTo(1);

            long totalCount = getJmxCount(stmt, "totaldynamicfilters.totalcount");
            assertThat(totalCount).isEqualTo(1);
        }
    }

    private long getJmxCount(Statement stmt, String metricName)
            throws Exception
    {
        try (ResultSet rs = stmt.executeQuery(
                "SELECT \"" + metricName + "\" " +
                "FROM jmx.current.\"io.trino.plugin.jdbc:name=mysql,type=dynamicfilteringstats\"")) {
            assertThat(rs.next()).isTrue();
            return rs.getLong(1);
        }
    }
}
