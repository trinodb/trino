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
import io.prestosql.tempto.assertions.QueryAssert;
import io.prestosql.testing.AbstractTestDistributedQueries;
import org.assertj.core.api.Assertions;
import org.testng.annotations.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.tempto.assertions.QueryAssert.Row.row;
import static io.prestosql.tempto.assertions.QueryAssert.assertThat;
import static io.prestosql.tests.TestGroups.HDP3_ONLY;
import static io.prestosql.tests.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.prestosql.tests.TestGroups.STORAGE_FORMATS;
import static io.prestosql.tests.utils.QueryExecutors.onHive;
import static io.prestosql.tests.utils.QueryExecutors.onPresto;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

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

    @Test
    public void testColumnNames()
    {
        List<String> sqlColumnNames = AbstractTestDistributedQueries.FANCY_COLUMN_NAMES.stream()
                .map(columnName -> "\"" + columnName.replace("\"", "\"\"") + "\"")
                .collect(toImmutableList());
        String createTable = "CREATE TABLE test_column_names AS SELECT " +
                IntStream.range(0, sqlColumnNames.size())
                        .mapToObj(i -> format("'value %s %s' %s", i, sqlColumnNames.get(i).replace("'", "''"), sqlColumnNames.get(i)))
                        .collect(joining(", "));
        String select = "SELECT " + join(", ", sqlColumnNames) + " FROM test_column_names";
        QueryAssert.Row values = new QueryAssert.Row(
                IntStream.range(0, sqlColumnNames.size())
                        .mapToObj(i -> format("value %s %s", i, sqlColumnNames.get(i)))
                        .toArray());

        onPresto().executeQuery("DROP TABLE IF EXISTS test_column_names");
        onPresto().executeQuery(createTable);
        assertThat(onPresto().executeQuery(select)).contains(values);
        assertThat(onHive().executeQuery(select)).contains(values);
        onPresto().executeQuery("DROP TABLE test_column_names");
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
