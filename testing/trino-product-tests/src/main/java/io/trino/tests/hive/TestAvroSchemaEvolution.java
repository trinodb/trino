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

import io.trino.tempto.AfterTestWithContext;
import io.trino.tempto.BeforeTestWithContext;
import io.trino.tempto.ProductTest;
import io.trino.tempto.query.QueryExecutor;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.tempto.context.ThreadLocalTestContextHolder.testContext;
import static io.trino.tempto.query.QueryExecutor.query;
import static io.trino.tests.TestGroups.AVRO;
import static java.lang.String.format;

public class TestAvroSchemaEvolution
        extends ProductTest
{
    private static final String TABLE_WITH_SCHEMA_URL = "product_tests_avro_table_with_schema_url";
    private static final String TABLE_WITH_SCHEMA_LITERAL = "product_tests_avro_table_with_schema_literal";
    // TODO move Avro schema files to classpath and use tempto SshClient to upload them
    private static final String ORIGINAL_SCHEMA_URL = "file:///docker/presto-product-tests/avro/original_schema.avsc";
    private static final String RENAMED_COLUMN_SCHEMA_URL = "file:///docker/presto-product-tests/avro/rename_column_schema.avsc";
    private static final String REMOVED_COLUMN_SCHEMA_URL = "file:///docker/presto-product-tests/avro/remove_column_schema.avsc";
    private static final String ADDED_COLUMN_SCHEMA_URL = "file:///docker/presto-product-tests/avro/add_column_schema.avsc";
    private static final String CHANGE_COLUMN_TYPE_SCHEMA_URL = "file:///docker/presto-product-tests/avro/change_column_type_schema.avsc";
    private static final String INCOMPATIBLE_TYPE_SCHEMA_URL = "file:///docker/presto-product-tests/avro/incompatible_type_schema.avsc";

    @BeforeTestWithContext
    public void createAndLoadTable()
            throws IOException
    {
        query(format("" +
                        "CREATE TABLE %s (dummy_col VARCHAR)" +
                        "WITH (" +
                        "format='AVRO', " +
                        "avro_schema_url='%s'" +
                        ")",
                TABLE_WITH_SCHEMA_URL,
                ORIGINAL_SCHEMA_URL));
        query(format("" +
                        "CREATE TABLE %s (dummy_col VARCHAR)" +
                        "WITH (" +
                        "format='AVRO', " +
                        "avro_schema_literal='%s'" +
                        ")",
                TABLE_WITH_SCHEMA_LITERAL,
                readSchemaLiteralFromUrl(ORIGINAL_SCHEMA_URL)));
        query(format("INSERT INTO %s VALUES ('string0', 0)", TABLE_WITH_SCHEMA_URL));
        query(format("INSERT INTO %s VALUES ('string0', 0)", TABLE_WITH_SCHEMA_LITERAL));
    }

    @AfterTestWithContext
    public void dropTestTable()
    {
        query(format("DROP TABLE IF EXISTS %s", TABLE_WITH_SCHEMA_URL));
        query(format("DROP TABLE IF EXISTS %s", TABLE_WITH_SCHEMA_LITERAL));
    }

    @Test(groups = AVRO)
    public void testSelectTable()
    {
        assertUnmodified();
    }

    @Test(groups = AVRO)
    public void testInsertAfterSchemaEvolution()
            throws IOException
    {
        assertUnmodified();

        alterTableSchemaUrl(TABLE_WITH_SCHEMA_URL, ADDED_COLUMN_SCHEMA_URL);
        alterTableSchemaLiteral(TABLE_WITH_SCHEMA_LITERAL, readSchemaLiteralFromUrl(ADDED_COLUMN_SCHEMA_URL));

        query(format("INSERT INTO %s VALUES ('string1', 1, 101)", TABLE_WITH_SCHEMA_URL));
        query(format("INSERT INTO %s VALUES ('string1', 1, 101)", TABLE_WITH_SCHEMA_LITERAL));

        assertThat(query("SELECT * FROM " + TABLE_WITH_SCHEMA_URL))
                .containsOnly(
                        row("string0", 0, 100),
                        row("string1", 1, 101));
        assertThat(query("SELECT * FROM " + TABLE_WITH_SCHEMA_LITERAL))
                .containsOnly(
                        row("string0", 0, 100),
                        row("string1", 1, 101));
    }

    @Test(groups = AVRO)
    public void testSchemaEvolutionWithIncompatibleType()
            throws IOException
    {
        assertUnmodified();

        alterTableSchemaUrl(TABLE_WITH_SCHEMA_URL, INCOMPATIBLE_TYPE_SCHEMA_URL);
        alterTableSchemaLiteral(TABLE_WITH_SCHEMA_LITERAL, readSchemaLiteralFromUrl(INCOMPATIBLE_TYPE_SCHEMA_URL));

        assertThat(() -> query("SELECT * FROM " + TABLE_WITH_SCHEMA_URL))
                .failsWithMessage("Found int, expecting string");
        assertThat(() -> query("SELECT * FROM " + TABLE_WITH_SCHEMA_URL))
                .failsWithMessage("Found int, expecting string");
    }

    @Test(groups = AVRO)
    public void testSchemaEvolution()
            throws IOException
    {
        assertUnmodified();

        alterTableSchemaUrl(TABLE_WITH_SCHEMA_URL, CHANGE_COLUMN_TYPE_SCHEMA_URL);
        alterTableSchemaLiteral(TABLE_WITH_SCHEMA_LITERAL, readSchemaLiteralFromUrl(CHANGE_COLUMN_TYPE_SCHEMA_URL));

        assertThat(query("SHOW COLUMNS IN " + TABLE_WITH_SCHEMA_URL))
                .containsExactlyInOrder(
                        row("string_col", "varchar", "", ""),
                        row("int_col", "bigint", "", ""));
        assertThat(query("SHOW COLUMNS IN " + TABLE_WITH_SCHEMA_LITERAL))
                .containsExactlyInOrder(
                        row("string_col", "varchar", "", ""),
                        row("int_col", "bigint", "", ""));

        assertThat(query("SELECT * FROM " + TABLE_WITH_SCHEMA_URL))
                .containsOnly(row("string0", 0));
        assertThat(query("SELECT * FROM " + TABLE_WITH_SCHEMA_LITERAL))
                .containsOnly(row("string0", 0));

        alterTableSchemaUrl(TABLE_WITH_SCHEMA_URL, ADDED_COLUMN_SCHEMA_URL);
        alterTableSchemaLiteral(TABLE_WITH_SCHEMA_LITERAL, readSchemaLiteralFromUrl(ADDED_COLUMN_SCHEMA_URL));

        assertThat(query("SHOW COLUMNS IN " + TABLE_WITH_SCHEMA_URL))
                .containsExactlyInOrder(
                        row("string_col", "varchar", "", ""),
                        row("int_col", "integer", "", ""),
                        row("int_col_added", "integer", "", ""));
        assertThat(query("SHOW COLUMNS IN " + TABLE_WITH_SCHEMA_LITERAL))
                .containsExactlyInOrder(
                        row("string_col", "varchar", "", ""),
                        row("int_col", "integer", "", ""),
                        row("int_col_added", "integer", "", ""));

        assertThat(query("SELECT * FROM " + TABLE_WITH_SCHEMA_URL))
                .containsOnly(row("string0", 0, 100));
        assertThat(query("SELECT * FROM " + TABLE_WITH_SCHEMA_LITERAL))
                .containsOnly(row("string0", 0, 100));

        alterTableSchemaUrl(TABLE_WITH_SCHEMA_URL, REMOVED_COLUMN_SCHEMA_URL);
        alterTableSchemaLiteral(TABLE_WITH_SCHEMA_LITERAL, readSchemaLiteralFromUrl(REMOVED_COLUMN_SCHEMA_URL));

        assertThat(query("SHOW COLUMNS IN " + TABLE_WITH_SCHEMA_URL))
                .containsExactlyInOrder(
                        row("int_col", "integer", "", ""));
        assertThat(query("SHOW COLUMNS IN " + TABLE_WITH_SCHEMA_LITERAL))
                .containsExactlyInOrder(
                        row("int_col", "integer", "", ""));

        assertThat(query("SELECT * FROM " + TABLE_WITH_SCHEMA_URL))
                .containsOnly(row(0));
        assertThat(query("SELECT * FROM " + TABLE_WITH_SCHEMA_LITERAL))
                .containsOnly(row(0));

        alterTableSchemaUrl(TABLE_WITH_SCHEMA_URL, RENAMED_COLUMN_SCHEMA_URL);
        alterTableSchemaLiteral(TABLE_WITH_SCHEMA_LITERAL, readSchemaLiteralFromUrl(RENAMED_COLUMN_SCHEMA_URL));

        assertThat(query("SHOW COLUMNS IN " + TABLE_WITH_SCHEMA_URL))
                .containsExactlyInOrder(
                        row("string_col", "varchar", "", ""),
                        row("int_col_renamed", "integer", "", ""));
        assertThat(query("SHOW COLUMNS IN " + TABLE_WITH_SCHEMA_LITERAL))
                .containsExactlyInOrder(
                        row("string_col", "varchar", "", ""),
                        row("int_col_renamed", "integer", "", ""));

        assertThat(query("SELECT * FROM " + TABLE_WITH_SCHEMA_URL))
                .containsOnly(row("string0", null));
        assertThat(query("SELECT * FROM " + TABLE_WITH_SCHEMA_LITERAL))
                .containsOnly(row("string0", null));
    }

    @Test(groups = AVRO)
    public void testSchemaWhenUrlIsUnset()
    {
        assertUnmodified();

        executeHiveQuery(format("ALTER TABLE %s UNSET TBLPROPERTIES('avro.schema.url')", TABLE_WITH_SCHEMA_URL));
        executeHiveQuery(format("ALTER TABLE %s UNSET TBLPROPERTIES('avro.schema.literal')", TABLE_WITH_SCHEMA_LITERAL));

        assertThat(query("SHOW COLUMNS IN " + TABLE_WITH_SCHEMA_URL))
                .containsExactlyInOrder(
                        row("dummy_col", "varchar", "", ""));
        assertThat(query("SHOW COLUMNS IN " + TABLE_WITH_SCHEMA_LITERAL))
                .containsExactlyInOrder(
                        row("dummy_col", "varchar", "", ""));
    }

    @Test(groups = AVRO)
    public void testCreateTableLike()
    {
        String createTableLikeName = "test_avro_like";
        query(format(
                "CREATE TABLE %s (LIKE %s INCLUDING PROPERTIES)",
                createTableLikeName,
                TABLE_WITH_SCHEMA_URL));

        query(format("INSERT INTO %s VALUES ('string0', 0)", createTableLikeName));

        assertThat(query(format("SELECT string_col FROM %s", createTableLikeName)))
                .containsExactlyInOrder(row("string0"));
        query("DROP TABLE IF EXISTS " + createTableLikeName);

        query(format(
                "CREATE TABLE %s (LIKE %s INCLUDING PROPERTIES)",
                createTableLikeName,
                TABLE_WITH_SCHEMA_LITERAL));

        query(format("INSERT INTO %s VALUES ('string0', 0)", createTableLikeName));

        assertThat(query(format("SELECT string_col FROM %s", createTableLikeName)))
                .containsExactlyInOrder(row("string0"));
        query("DROP TABLE IF EXISTS " + createTableLikeName);
    }

    private void assertUnmodified()
    {
        assertThat(query("SHOW COLUMNS IN " + TABLE_WITH_SCHEMA_URL))
                .containsExactlyInOrder(
                        row("string_col", "varchar", "", ""),
                        row("int_col", "integer", "", ""));
        assertThat(query("SHOW COLUMNS IN " + TABLE_WITH_SCHEMA_LITERAL))
                .containsExactlyInOrder(
                        row("string_col", "varchar", "", ""),
                        row("int_col", "integer", "", ""));

        assertThat(query("SELECT * FROM " + TABLE_WITH_SCHEMA_URL))
                .containsOnly(row("string0", 0));
        assertThat(query("SELECT * FROM " + TABLE_WITH_SCHEMA_LITERAL))
                .containsOnly(row("string0", 0));
    }

    private static void alterTableSchemaUrl(String tableName, String newUrl)
    {
        executeHiveQuery(format("ALTER TABLE %s SET TBLPROPERTIES('avro.schema.url'='%s')", tableName, newUrl));
    }

    private static void alterTableSchemaLiteral(String tableName, String newLiteral)
    {
        executeHiveQuery(format("ALTER TABLE %s SET TBLPROPERTIES('avro.schema.literal'='%s')", tableName, newLiteral));
    }

    private static void executeHiveQuery(String query)
    {
        testContext().getDependency(QueryExecutor.class, "hive").executeQuery(query);
    }

    private static String readSchemaLiteralFromUrl(String url)
            throws IOException
    {
        return Files.readString(Path.of(URI.create(url)));
    }
}
