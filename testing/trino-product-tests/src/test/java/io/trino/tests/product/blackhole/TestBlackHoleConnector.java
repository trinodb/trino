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
package io.trino.tests.product.blackhole;

import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the BlackHole connector.
 * <p>
 * The BlackHole connector accepts writes but discards them,
 * and reads always return zero rows.
 */
@ProductTest
@RequiresEnvironment(BlackHoleEnvironment.class)
@TestGroup.Blackhole
@TestGroup.ProfileSpecificTests
class TestBlackHoleConnector
{
    @Test
    void testBlackHoleConnector(BlackHoleEnvironment env)
            throws Exception
    {
        String tableName = "blackhole.default.nation_" + UUID.randomUUID().toString().replace("-", "");

        try (Connection conn = env.createTrinoConnection();
                Statement stmt = conn.createStatement()) {
            // Verify source table has data
            try (ResultSet rs = stmt.executeQuery("SELECT count(*) FROM tpch.tiny.nation")) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getLong(1)).isEqualTo(25);
            }

            // Create table in blackhole (accepts the write)
            int createCount = stmt.executeUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM tpch.tiny.nation");
            assertThat(createCount).isEqualTo(25);

            try {
                // Insert more data (accepts the write)
                int insertCount = stmt.executeUpdate("INSERT INTO " + tableName + " SELECT * FROM tpch.tiny.nation");
                assertThat(insertCount).isEqualTo(25);

                // Read returns zero rows (blackhole discards everything)
                try (ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName)) {
                    assertThat(rs.next()).isFalse();
                }
            }
            finally {
                stmt.execute("DROP TABLE " + tableName);
            }
        }
    }
}
