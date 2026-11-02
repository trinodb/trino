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
package io.trino.tests.product.hive;

import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Optional;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

@ProductTest
@RequiresEnvironment(HiveTransactionalEnvironment.class)
@TestGroup.HiveTransactional
class TestHiveTransactionalByDefault
{
    private static final String TRANSACTIONAL = "transactional";

    @Test
    void testVerifyEnvironmentHiveTransactionalByDefault(HiveTransactionalEnvironment env)
            throws SQLException
    {
        String tableName = "test_hive_transactional_by_default_" + randomNameSuffix();
        env.executeHiveUpdate("CREATE TABLE default." + tableName + "(a bigint) STORED AS ORC");
        try {
            assertThat(getTableProperty(env, "default." + tableName, TRANSACTIONAL))
                    .contains("true");
        }
        finally {
            env.executeHiveUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    private static Optional<String> getTableProperty(HiveTransactionalEnvironment env, String tableName, String propertyName)
            throws SQLException
    {
        requireNonNull(tableName, "tableName is null");
        requireNonNull(propertyName, "propertyName is null");

        try (Connection connection = env.createHiveConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery("SHOW TBLPROPERTIES " + tableName)) {
            while (resultSet.next()) {
                if (propertyName.equals(resultSet.getString("prpt_name"))) {
                    return Optional.of(resultSet.getString("prpt_value"));
                }
            }
        }
        return Optional.empty();
    }
}
