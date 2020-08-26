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
package io.trino.tests.hive;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.assertj.core.api.Condition;
import org.testng.annotations.Test;

import java.sql.Date;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.trino.tempto.assertions.QueryAssert.Row;
import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.tests.TestGroups.HIVE_REDIRECTION_TO_ICEBERG;
import static io.trino.tests.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.utils.QueryExecutors.onPresto;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class TestHiveRedirectionToIceberg
        extends HiveProductTest
{
    private static final String CREATE_ICEBERG_TABLE_TEMPLATE = "CREATE TABLE %s (_string VARCHAR, _bigint BIGINT, _date DATE) WITH (partitioning = ARRAY['_bigint', '_date'])";
    private static final String CREATE_HIVE_TABLE_TEMPLATE = "CREATE TABLE %s (_string VARCHAR, _bigint BIGINT, _date DATE) WITH (partitioned_by = ARRAY['_bigint', '_date'])";
    private static final String INSERT_TEMPLATE = "INSERT INTO %s VALUES " +
            "(NULL, NULL, NULL), " +
            "('abc', 1, DATE '2020-08-04'), " +
            "('abcdefghijklmnopqrstuvwxyz', 2, DATE '2020-08-04')";
    private static final List<Row> EXPECTED_ROWS = ImmutableList.<Row>builder()
            .add(row(null, null, null))
            .add(row("abc", 1, Date.valueOf("2020-08-04")))
            .add(row("abcdefghijklmnopqrstuvwxyz", 2, Date.valueOf("2020-08-04")))
            .build();
    private static final Pattern TABLE_COMMENT_EXTRACTER = Pattern.compile(".*?COMMENT\\s*'(.*?)'.*$", Pattern.DOTALL);

    @Test(groups = {HIVE_REDIRECTION_TO_ICEBERG, PROFILE_SPECIFIC_TESTS})
    public void testSelectAndDrop()
    {
        onPresto().executeQuery(format(CREATE_ICEBERG_TABLE_TEMPLATE, "iceberg.default.test_select_drop"));
        onPresto().executeQuery(format(INSERT_TEMPLATE, "iceberg.default.test_select_drop"));

        assertThat(onPresto().executeQuery("SELECT * FROM iceberg.default.test_select_drop")).containsOnly(EXPECTED_ROWS);
        assertThat(onPresto().executeQuery("SELECT * FROM hive.default.test_select_drop")).containsOnly(EXPECTED_ROWS);
        assertThat(onPresto().executeQuery("SELECT _bigint, _date FROM hive.default.\"test_select_drop$partitions\"")).containsOnly(
                row(null, null),
                row(1, Date.valueOf("2020-08-04")),
                row(2, Date.valueOf("2020-08-04")));

        onPresto().executeQuery("DROP TABLE hive.default.test_select_drop");

        assertThat(() -> onPresto().executeQuery("SELECT * FROM hive.default.test_select_drop"))
                .failsWithMessage("Table 'hive.default.test_select_drop' does not exist");
        assertThat(() -> onPresto().executeQuery("SELECT * FROM iceberg.default.test_select_drop"))
                .failsWithMessage("Table 'iceberg.default.test_select_drop' does not exist");
    }

    @Test(groups = {HIVE_REDIRECTION_TO_ICEBERG, PROFILE_SPECIFIC_TESTS})
    public void testInsert()
    {
        onPresto().executeQuery(format(CREATE_ICEBERG_TABLE_TEMPLATE, "iceberg.default.test_insert"));
        onPresto().executeQuery(format(INSERT_TEMPLATE, "hive.default.test_insert"));

        assertThat(onPresto().executeQuery("SELECT * FROM iceberg.default.test_insert")).containsOnly(EXPECTED_ROWS);
        assertThat(onPresto().executeQuery("SELECT * FROM hive.default.test_insert")).containsOnly(EXPECTED_ROWS);

        onPresto().executeQuery("DROP TABLE iceberg.default.test_insert");
    }

    @Test(groups = {HIVE_REDIRECTION_TO_ICEBERG, PROFILE_SPECIFIC_TESTS})
    public void testDescribe()
    {
        onPresto().executeQuery(format(CREATE_ICEBERG_TABLE_TEMPLATE, "iceberg.default.test_describe"));

        assertThat(onPresto().executeQuery("DESCRIBE hive.default.test_describe"))
                .satisfies(new Condition<>(queryResult -> {
                    Set<String> actualColumns = ImmutableSet.copyOf(queryResult.column(1));
                    Set<String> expectedColumns = ImmutableSet.of("_string", "_bigint", "_date");
                    return actualColumns.equals(expectedColumns);
                }, "equals"));

        onPresto().executeQuery("DROP TABLE iceberg.default.test_describe");
    }

    @Test(groups = {HIVE_REDIRECTION_TO_ICEBERG, PROFILE_SPECIFIC_TESTS})
    public void testShowCreateTable()
    {
        onPresto().executeQuery(format(CREATE_ICEBERG_TABLE_TEMPLATE, "iceberg.default.test_show_create"));

        assertThat(onPresto().executeQuery("SHOW CREATE TABLE hive.default.test_show_create")).hasRowsCount(1);

        onPresto().executeQuery("DROP TABLE iceberg.default.test_show_create");
    }

    @Test(groups = {HIVE_REDIRECTION_TO_ICEBERG, PROFILE_SPECIFIC_TESTS})
    public void testAlterTable()
    {
        onPresto().executeQuery(format(CREATE_ICEBERG_TABLE_TEMPLATE, "iceberg.default.test_alter_table"));
        onPresto().executeQuery(format(INSERT_TEMPLATE, "iceberg.default.test_alter_table"));

        onPresto().executeQuery("ALTER TABLE hive.default.test_alter_table RENAME TO default.test_alter_table_new");
        assertThat(onPresto().executeQuery("SELECT * FROM iceberg.default.test_alter_table_new")).containsOnly(EXPECTED_ROWS);
        assertThat(onPresto().executeQuery("SELECT * FROM hive.default.test_alter_table_new")).containsOnly(EXPECTED_ROWS);

        onPresto().executeQuery("ALTER TABLE hive.default.test_alter_table_new ADD COLUMN _double DOUBLE");
        onPresto().executeQuery("ALTER TABLE hive.default.test_alter_table_new DROP COLUMN _string");
        onPresto().executeQuery("ALTER TABLE hive.default.test_alter_table_new RENAME COLUMN _bigint TO _bi");
        assertThat(onPresto().executeQuery("DESCRIBE hive.default.test_alter_table_new"))
                .satisfies(new Condition<>(queryResult -> {
                    Set<String> actualColumns = ImmutableSet.copyOf(queryResult.column(1));
                    Set<String> expectedColumns = ImmutableSet.of("_bi", "_date", "_double");
                    return actualColumns.equals(expectedColumns);
                }, "equals"));

        onPresto().executeQuery("DROP TABLE iceberg.default.test_alter_table_new");
    }

    @Test(groups = {HIVE_REDIRECTION_TO_ICEBERG, PROFILE_SPECIFIC_TESTS})
    public void testCommentTable()
    {
        onPresto().executeQuery(format(CREATE_ICEBERG_TABLE_TEMPLATE, "iceberg.default.test_comment_table"));

        assertNull(extractTableComment((String) onPresto().executeQuery("SHOW CREATE TABLE hive.default.test_comment_table").row(0).get(0)));
        onPresto().executeQuery("COMMENT ON TABLE hive.default.test_comment_table IS 'this is an iceberg table'");
        assertEquals(extractTableComment((String) onPresto().executeQuery("SHOW CREATE TABLE hive.default.test_comment_table").row(0).get(0)), "this is an iceberg table");
        assertEquals(extractTableComment((String) onPresto().executeQuery("SHOW CREATE TABLE iceberg.default.test_comment_table").row(0).get(0)), "this is an iceberg table");

        onPresto().executeQuery("DROP TABLE iceberg.default.test_comment_table");
    }

    @Test(groups = {HIVE_REDIRECTION_TO_ICEBERG, PROFILE_SPECIFIC_TESTS})
    public void testCreateHiveTable()
    {
        onPresto().executeQuery(format(CREATE_HIVE_TABLE_TEMPLATE, "hive.default.test_create_hive_table"));
        onPresto().executeQuery(format(INSERT_TEMPLATE, "hive.default.test_create_hive_table"));

        assertThat(onPresto().executeQuery("SELECT * FROM hive.default.test_create_hive_table")).containsOnly(EXPECTED_ROWS);
        assertThat(() -> onPresto().executeQuery("SELECT * FROM iceberg.default.test_create_hive_table"))
                .failsWithMessage("Not an Iceberg table: default.test_create_hive_table");

        onPresto().executeQuery("DROP TABLE hive.default.test_create_hive_table");
    }

    @Test(groups = {HIVE_REDIRECTION_TO_ICEBERG, PROFILE_SPECIFIC_TESTS})
    public void testInformationSchemaColumns()
    {
        onPresto().executeQuery("CREATE SCHEMA iceberg.redirection_schema");
        onPresto().executeQuery(format(CREATE_ICEBERG_TABLE_TEMPLATE, "iceberg.redirection_schema.test_info_schema_table"));

        List<Row> expectedColumns = ImmutableList.<Row>builder()
                .add(row("hive", "redirection_schema", "test_info_schema_table", "_string", 1, null, "YES", "varchar"))
                .add(row("hive", "redirection_schema", "test_info_schema_table", "_bigint", 2, null, "YES", "bigint"))
                .add(row("hive", "redirection_schema", "test_info_schema_table", "_date", 3, null, "YES", "date"))
                .build();
        assertThat(onPresto().executeQuery("SELECT * FROM hive.information_schema.columns")).contains(expectedColumns);
        assertThat(onPresto().executeQuery("SELECT * FROM hive.information_schema.columns WHERE table_schema = 'redirection_schema'")).containsOnly(expectedColumns);
        assertThat(onPresto().executeQuery("SELECT * FROM hive.information_schema.columns WHERE table_schema = 'redirection_schema' AND table_name = 'test_info_schema_table'")).containsOnly(expectedColumns);

        onPresto().executeQuery("DROP TABLE iceberg.redirection_schema.test_info_schema_table");
        onPresto().executeQuery("DROP SCHEMA iceberg.redirection_schema");
    }

    private static String extractTableComment(String sql)
    {
        Matcher matcher = TABLE_COMMENT_EXTRACTER.matcher(sql);
        if (matcher.matches()) {
            return matcher.group(1);
        }
        return null;
    }
}
