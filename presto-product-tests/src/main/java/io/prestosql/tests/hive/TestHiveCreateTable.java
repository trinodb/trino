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
package io.prestosql.tests.hive;

import io.prestosql.tempto.ProductTest;
import org.assertj.core.api.Assertions;
import org.testng.annotations.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Optional;

import static io.prestosql.tempto.assertions.QueryAssert.Row.row;
import static io.prestosql.tempto.assertions.QueryAssert.assertThat;
import static io.prestosql.tests.TestGroups.HDP3_ONLY;
import static io.prestosql.tests.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.prestosql.tests.TestGroups.STORAGE_FORMATS;
import static io.prestosql.tests.utils.QueryExecutors.onHive;
import static io.prestosql.tests.utils.QueryExecutors.onPresto;
import static java.util.Objects.requireNonNull;

public class TestHiveCreateTable
        extends ProductTest
{
    @Test(groups = STORAGE_FORMATS)
    public void testCreateTable()
            throws SQLException
    {
        onPresto().executeQuery("CREATE TABLE test_create_table(a bigint, b varchar, c smallint) WITH (format='ORC')");
        onPresto().executeQuery("INSERT INTO test_create_table(a, b, c) VALUES " +
                "(NULL, NULL, NULL), " +
                "(-42, 'abc', SMALLINT '-127'), " +
                "(9223372036854775807, 'abcdefghijklmnopqrstuvwxyz', SMALLINT '32767')");
        assertThat(onPresto().executeQuery("SELECT * FROM test_create_table"))
                .containsOnly(
                        row(null, null, null),
                        row(-42, "abc", -127),
                        row(9223372036854775807L, "abcdefghijklmnopqrstuvwxyz", 32767));
        Assertions.assertThat(getTableProperty("test_create_table", "transactional"))
                // Hive 3 removes "transactional" table property when it has value "false"
                .isIn(Optional.empty(), Optional.of("false"));
        onPresto().executeQuery("DROP TABLE test_create_table");
    }

    @Test(groups = STORAGE_FORMATS)
    public void testCreateTableAsSelect()
            throws SQLException
    {
        onPresto().executeQuery("" +
                "CREATE TABLE test_create_table_as_select WITH (format='ORC') AS " +
                "SELECT * FROM (VALUES " +
                "  (NULL, NULL, NULL), " +
                "  (-42, 'abc', SMALLINT '-127'), " +
                "  (9223372036854775807, 'abcdefghijklmnopqrstuvwxyz', SMALLINT '32767')" +
                ") t(a, b, c)");
        assertThat(onPresto().executeQuery("SELECT * FROM test_create_table_as_select"))
                .containsOnly(
                        row(null, null, null),
                        row(-42, "abc", -127),
                        row(9223372036854775807L, "abcdefghijklmnopqrstuvwxyz", 32767));
        Assertions.assertThat(getTableProperty("test_create_table_as_select", "transactional"))
                // Hive 3 removes "transactional" table property when it has value "false"
                .isIn(Optional.empty(), Optional.of("false"));
        onPresto().executeQuery("DROP TABLE test_create_table_as_select");
    }

    @Test(groups = {HDP3_ONLY, PROFILE_SPECIFIC_TESTS})
    public void testVerifyEnvironmentHiveTransactionalByDefault()
            throws SQLException
    {
        onHive().executeQuery("CREATE TABLE test_hive_transactional_by_default(a bigint) STORED AS ORC");
        Assertions.assertThat(getTableProperty("test_hive_transactional_by_default", "transactional"))
                .contains("true");
        onHive().executeQuery("DROP TABLE test_hive_transactional_by_default");
    }

    private static Optional<String> getTableProperty(String tableName, String propertyName)
            throws SQLException
    {
        requireNonNull(tableName, "tableName is null");
        requireNonNull(propertyName, "propertyName is null");

        try (Statement statement = onHive().getConnection().createStatement();
                ResultSet resultSet = statement.executeQuery("SHOW TBLPROPERTIES " + tableName)) {
            while (resultSet.next()) {
                if (propertyName.equals(resultSet.getString("prpt_name"))) {
                    // We need to distinguish between a property that is not set and a property that has NULL value.
                    return Optional.of(resultSet.getString("prpt_value"));
                }
            }
        }

        return Optional.empty();
    }
}
