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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThat;

@ProductTest
@RequiresEnvironment(YdbEnvironment.class)
@TestGroup.Ydb
@TestGroup.ProfileSpecificTests
class TestYdb
{
    @Test
    void testReadTable(YdbEnvironment env)
            throws Exception
    {
        try (Connection nativeConnection = env.createYdbConnection();
                Statement nativeStatement = nativeConnection.createStatement()) {
            nativeStatement.execute("CREATE TABLE nation (nationkey Int64, name Text, PRIMARY KEY (nationkey))");
            try {
                try (PreparedStatement insert = nativeConnection.prepareStatement("UPSERT INTO nation (nationkey, name) VALUES (?, ?)")) {
                    for (int key = 0; key < 25; key++) {
                        insert.setLong(1, key);
                        insert.setString(2, "nation_" + key);
                        insert.addBatch();
                    }
                    insert.executeBatch();
                }
                try (Connection connection = env.createTrinoConnection();
                        Statement statement = connection.createStatement();
                        ResultSet rows = statement.executeQuery("SELECT COUNT(*) FROM ydb.default.nation")) {
                    assertThat(rows.next()).isTrue();
                    assertThat(rows.getLong(1)).isEqualTo(25);
                    assertThat(rows.next()).isFalse();
                }
            }
            finally {
                nativeStatement.execute("DROP TABLE nation");
            }
        }
    }
}
