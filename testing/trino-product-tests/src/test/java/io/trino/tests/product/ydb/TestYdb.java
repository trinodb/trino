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
package io.trino.tests.product.ydb;

import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * YDB CREATE TABLE AS SELECT test.
 */
@ProductTest
@RequiresEnvironment(YdbEnvironment.class)
@TestGroup.Ydb
@TestGroup.ProfileSpecificTests
class TestYdb
{
    @Test
    void testCreateTableAsSelect(YdbEnvironment env)
            throws Exception
    {
        try (Connection conn = env.createTrinoConnection();
                Statement stmt = conn.createStatement()) {
            int count = stmt.executeUpdate("CREATE TABLE ydb.default.nation " +
                    "WITH (primary_key = ARRAY['nationkey']) AS SELECT * FROM tpch.tiny.nation");
            try {
                assertThat(count).isEqualTo(25);

                try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM ydb.default.nation")) {
                    assertThat(rs.next()).isTrue();
                    assertThat(rs.getLong(1)).isEqualTo(25);
                }
            }
            finally {
                stmt.execute("DROP TABLE ydb.default.nation");
            }
        }
    }
}
