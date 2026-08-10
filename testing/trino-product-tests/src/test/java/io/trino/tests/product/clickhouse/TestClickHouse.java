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
package io.trino.tests.product.clickhouse;

import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * ClickHouse CREATE TABLE AS SELECT test.
 */
@ProductTest
@RequiresEnvironment(ClickHouseEnvironment.class)
@TestGroup.Clickhouse
@TestGroup.ProfileSpecificTests
class TestClickHouse
{
    @Test
    void testCreateTableAsSelect(ClickHouseEnvironment env)
            throws Exception
    {
        try (Connection conn = env.createTrinoConnection();
                Statement stmt = conn.createStatement()) {
            int count = stmt.executeUpdate("CREATE TABLE clickhouse.default.nation AS SELECT * FROM tpch.tiny.nation");
            try {
                assertThat(count).isEqualTo(25);

                try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM clickhouse.default.nation")) {
                    assertThat(rs.next()).isTrue();
                    assertThat(rs.getLong(1)).isEqualTo(25);
                }
            }
            finally {
                stmt.execute("DROP TABLE clickhouse.default.nation");
            }
        }
    }
}
