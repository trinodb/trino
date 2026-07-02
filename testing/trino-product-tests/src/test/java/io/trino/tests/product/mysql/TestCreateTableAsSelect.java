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
import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests CREATE TABLE AS SELECT functionality with MySQL connector.
 */
@ProductTest
@RequiresEnvironment(MySqlEnvironment.class)
@TestGroup.Mysql
@TestGroup.ProfileSpecificTests
class TestCreateTableAsSelect
{
    private static final String TABLE_NAME = "nation_ctas_tmp";

    @Test
    void testCreateTableAsSelect(MySqlEnvironment environment)
            throws Exception
    {
        try (Connection conn = environment.createTrinoConnection();
                Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS mysql.test." + TABLE_NAME);
            int count = stmt.executeUpdate(
                    "CREATE TABLE mysql.test." + TABLE_NAME + " AS SELECT * FROM tpch.tiny.nation");
            assertThat(count).isEqualTo(25);
        }
    }
}
