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
package io.trino.tests.product.sqlserver;

import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * SQL Server CREATE TABLE AS SELECT test.
 */
@ProductTest
@RequiresEnvironment(SqlServerEnvironment.class)
@TestGroup.Sqlserver
@TestGroup.ProfileSpecificTests
class TestSqlServer
{
    @Test
    void testCreateTableAsSelect(SqlServerEnvironment env)
            throws Exception
    {
        String tableName = "sqlserver.dbo.nation_" + randomNameSuffix();

        try (Connection conn = env.createTrinoConnection();
                Statement stmt = conn.createStatement()) {
            int count = stmt.executeUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM tpch.tiny.nation");
            try {
                assertThat(count).isEqualTo(25);

                try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tableName)) {
                    assertThat(rs.next()).isTrue();
                    assertThat(rs.getLong(1)).isEqualTo(25);
                }
            }
            finally {
                stmt.execute("DROP TABLE " + tableName);
            }
        }
    }
}
