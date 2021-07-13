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

import com.google.common.collect.ImmutableList;
import io.trino.tempto.query.QueryResult;
import org.testng.annotations.Test;

import java.sql.Date;
import java.util.List;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.tempto.assertions.QueryAssert.Row;
import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.tests.product.TestGroups.HIVE_REDIRECTION_TO_ICEBERG;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.hive.util.TemporaryHiveTable.randomTableSuffix;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

public class TestHiveRedirectionToIceberg
        extends HiveProductTest
{
    private static final String DEFAULT_SCHEMA = "default";
    private static final String HIVE_CATALOG = "hive";
    private static final String ICEBERG_CATALOG = "iceberg";

    private static final String CREATE_TABLE_TEMPLATE_ICEBERG = "CREATE TABLE %s (_string VARCHAR, _bigint BIGINT, _date DATE) WITH (partitioning = ARRAY['_bigint', '_date'])";
    private static final String CREATE_TABLE_TEMPLATE_HIVE = "CREATE TABLE %s (_string VARCHAR, _bigint BIGINT, _date DATE) WITH (partitioned_by = ARRAY['_bigint', '_date'])";
    private static final String INSERT_TEMPLATE = "INSERT INTO %s VALUES " +
            "(NULL, NULL, NULL), " +
            "('abc', 1, DATE '2020-08-04'), " +
            "('abcdefghijklmnopqrstuvwxyz', 2, DATE '2020-08-04')";
    private static final List<Row> EXPECTED_ROWS = ImmutableList.<Row>builder()
            .add(row(null, null, null))
            .add(row("abc", 1, Date.valueOf("2020-08-04")))
            .add(row("abcdefghijklmnopqrstuvwxyz", 2, Date.valueOf("2020-08-04")))
            .build();

    @Test(groups = {HIVE_REDIRECTION_TO_ICEBERG, PROFILE_SPECIFIC_TESTS})
    public void testSelect()
    {
        String tableName = createOnTrinoIceberg("test_select");
        insertWithTrinoIceberg(tableName);

        assertThat(onTrinoIceberg("SELECT * FROM %s", tableName)).containsOnly(EXPECTED_ROWS);
        assertThat(onTrinoHive("SELECT * FROM %s", tableName)).containsOnly(EXPECTED_ROWS);
        assertThatThrownBy(() -> onTrinoHive("SELECT * FROM %s", format("\"%s$partitions\"", tableName)))
                .hasMessageContaining(
                        "Querying system table '%s$partitions' is not supported, because the source table is redirected to '%s'",
                        fullName(HIVE_CATALOG, DEFAULT_SCHEMA, tableName),
                        fullName(ICEBERG_CATALOG, DEFAULT_SCHEMA, tableName));

        dropWithTrinoIceberg(tableName);
    }

    @Test(groups = {HIVE_REDIRECTION_TO_ICEBERG, PROFILE_SPECIFIC_TESTS})
    public void testDescribe()
    {
        String tableName = createOnTrinoIceberg("test_describe");
        assertThat(onTrinoHive("DESCRIBE %s", tableName))
                .containsOnly(ImmutableList.of(
                        row("_string", "varchar", "", ""),
                        row("_bigint", "bigint", "", ""),
                        row("_date", "date", "", "")));
        dropWithTrinoIceberg(tableName);
    }

    @Test(groups = {HIVE_REDIRECTION_TO_ICEBERG, PROFILE_SPECIFIC_TESTS})
    public void testShowCreateTable()
    {
        String tableName = createOnTrinoIceberg("test_show_create");
        QueryResult icebergShowCreate = onTrinoIceberg("SHOW CREATE TABLE %s", tableName);
        QueryResult hiveShowCreate = onTrinoHive("SHOW CREATE TABLE %s", tableName);

        assertEquals(
                getOnlyElement(icebergShowCreate.column(1)),
                ((String) getOnlyElement(hiveShowCreate.column(1)))
                        // Everything except for "CREATE TABLE X" should be the same
                        .replace(
                                format("CREATE TABLE %s", fullName(HIVE_CATALOG, DEFAULT_SCHEMA, tableName)),
                                format("CREATE TABLE %s", fullName(ICEBERG_CATALOG, DEFAULT_SCHEMA, tableName))));

        dropWithTrinoIceberg(tableName);
    }

    @Test(groups = {HIVE_REDIRECTION_TO_ICEBERG, PROFILE_SPECIFIC_TESTS})
    public void testDrop()
    {
        String tableName = createOnTrinoIceberg("test_drop");
        assertThatThrownBy(() -> onTrinoHive("DROP TABLE %s", tableName))
                .hasMessageMatching(notSupportedError("DROP TABLE", tableName));
        dropWithTrinoIceberg(tableName);
    }

    @Test(groups = {HIVE_REDIRECTION_TO_ICEBERG, PROFILE_SPECIFIC_TESTS})
    public void testDelete()
    {
        String tableName = createOnTrinoIceberg("test_delete");
        assertThatThrownBy(() -> onTrinoHive("DELETE FROM %s WHERE _bigint = 2", tableName))
                .hasMessageMatching(notSupportedError("DELETE", tableName));
        dropWithTrinoIceberg(tableName);
    }

    @Test(groups = {HIVE_REDIRECTION_TO_ICEBERG, PROFILE_SPECIFIC_TESTS})
    public void testInsert()
    {
        String tableName = createOnTrinoIceberg("test_insert");
        assertThatThrownBy(() -> insertWithTrinoHive(tableName))
                .hasMessageMatching(notSupportedError("INSERT", tableName));
        dropWithTrinoIceberg(tableName);
    }

    @Test(groups = {HIVE_REDIRECTION_TO_ICEBERG, PROFILE_SPECIFIC_TESTS})
    public void testAlterTable()
    {
        String tableName = createOnTrinoIceberg("test_alter_table");
        insertWithTrinoIceberg(tableName);

        assertThatThrownBy(() -> onTrinoHive("ALTER TABLE %s RENAME TO random_schema.random_table", tableName))
                .hasMessageMatching(notSupportedError("RENAME TABLE", tableName));

        assertThatThrownBy(() -> onTrinoHive("ALTER TABLE %s ADD COLUMN _double DOUBLE", tableName))
                .hasMessageMatching(notSupportedError("ADD COLUMN", tableName));
        assertThatThrownBy(() -> onTrinoHive("ALTER TABLE %s DROP COLUMN _string", tableName))
                .hasMessageMatching(notSupportedError("DROP COLUMN", tableName));
        assertThatThrownBy(() -> onTrinoHive("ALTER TABLE %s RENAME COLUMN _bigint TO _bi", tableName))
                .hasMessageMatching(notSupportedError("RENAME COLUMN", tableName));

        assertThatThrownBy(() -> onTrinoHive("ALTER TABLE %s SET AUTHORIZATION user", tableName))
                .hasMessageMatching(notSupportedError("SET TABLE AUTHORIZATION", tableName));

        dropWithTrinoIceberg(tableName);
    }

    @Test(groups = {HIVE_REDIRECTION_TO_ICEBERG, PROFILE_SPECIFIC_TESTS})
    public void testUpdate()
    {
        String tableName = createOnTrinoIceberg("test_update");
        assertThatThrownBy(() -> onTrinoHive("UPDATE %s SET _string = 'big_number' WHERE _bigint > 3", tableName))
                .hasMessageMatching(notSupportedError("UPDATE", tableName));
        dropWithTrinoIceberg(tableName);
    }

    @Test(groups = {HIVE_REDIRECTION_TO_ICEBERG, PROFILE_SPECIFIC_TESTS})
    public void testPartitionUpdates()
    {
        String tableName = createOnTrinoIceberg("test_partition_updates");
        String errorFromHiveConnector = ".*This operation is not supported on '.*', because it is redirected to '.*'. "
                + "Modification operations \\(insert, alter, drop, delete, update, comment, grant, revoke\\) are not supported when redirection is enabled.";

        assertThatThrownBy(() -> onTrino().executeQuery(format(
                "CALL %s.system.create_empty_partition("
                        + " schema_name => '%s', table_name => '%s',"
                        + " partition_columns => ARRAY['_bigint', '_date'],"
                        + " partition_values => ARRAY['5', 'DATE 2020-08-04'])",
                HIVE_CATALOG,
                DEFAULT_SCHEMA,
                tableName)))
                .hasMessageMatching(errorFromHiveConnector);

        assertThatThrownBy(() -> onTrino().executeQuery(format(
                "CALL %s.system.drop_stats(schema_name => '%s', table_name => '%s', partition_values => ARRAY[ARRAY['5', 'DATE 2020-08-04']])",
                HIVE_CATALOG,
                DEFAULT_SCHEMA,
                tableName)))
                .hasMessageMatching(errorFromHiveConnector);

        assertThatThrownBy(() -> onTrinoHive("ANALYZE %s", tableName))
                .hasMessageMatching(".*Cannot collect statistics for table '.*', because it is redirected to '.*'");

        dropWithTrinoIceberg(tableName);
    }

    @Test(groups = {HIVE_REDIRECTION_TO_ICEBERG, PROFILE_SPECIFIC_TESTS})
    public void testGrantRevoke()
    {
        String tableName = createOnTrinoIceberg("test_grant_revoke");
        assertThatThrownBy(() -> onTrinoHive("GRANT SELECT on %s TO alice", tableName))
                .hasMessageMatching(notSupportedError("GRANT", tableName));
        assertThatThrownBy(() -> onTrinoHive("REVOKE ALL PRIVILEGES ON %s FROM bob", tableName))
                .hasMessageMatching(notSupportedError("REVOKE", tableName));
        dropWithTrinoIceberg(tableName);
    }

    @Test(groups = {HIVE_REDIRECTION_TO_ICEBERG, PROFILE_SPECIFIC_TESTS})
    public void testComments()
    {
        String tableName = createOnTrinoIceberg("test_comment_table");

        String icebergComment = "this is a comment from iceberg catalog";
        onTrinoIceberg("COMMENT ON TABLE %s IS '" + icebergComment + "'", tableName);

        assertThat(onTrino().executeQuery(format("SELECT comment FROM system.metadata.table_comments WHERE catalog_name='%s'", HIVE_CATALOG)))
                .contains(row(icebergComment));

        assertThat(onTrino().executeQuery(format("SELECT comment FROM system.metadata.table_comments WHERE catalog_name='%s' and schema_name='%s'", HIVE_CATALOG, DEFAULT_SCHEMA)))
                .contains(row(icebergComment));

        // Assert the case for filters pointing to a specific table
        assertThat(onTrino().executeQuery(format("SELECT comment FROM system.metadata.table_comments WHERE catalog_name='%s' and schema_name='%s' and table_name='%s'", HIVE_CATALOG, DEFAULT_SCHEMA, tableName)))
                .containsOnly(row(icebergComment));

        assertThatThrownBy(() -> onTrinoHive("COMMENT ON TABLE %s IS 'ignore'", tableName))
                .hasMessageMatching(notSupportedError("COMMENT", tableName));

        assertThatThrownBy(() -> onTrinoHive("COMMENT ON COLUMN %s._bigint IS 'ignore'", tableName))
                .hasMessageMatching(notSupportedError("COMMENT", tableName));

        dropWithTrinoIceberg(tableName);
    }

    @Test(groups = {HIVE_REDIRECTION_TO_ICEBERG, PROFILE_SPECIFIC_TESTS})
    public void testHiveBehavior()
    {
        // Just a sanity check to verify hive catalog behavior is unaltered, perform some operations through hive catalog.
        String hiveTable = createOnTrinoHive("test_hive_behavior");
        insertWithTrinoHive(hiveTable);
        String icebergTable = createOnTrinoIceberg("test_hive_behavior_iceberg");
        insertWithTrinoIceberg(icebergTable);

        assertThat(onTrinoHive("SELECT * FROM %s", hiveTable)).containsOnly(EXPECTED_ROWS);
        onTrinoHive("ANALYZE %s", hiveTable);
        onTrinoHive("DELETE FROM %s WHERE (_bigint is NULL OR _bigint = 2)", hiveTable);
        assertThat(onTrinoHive("SELECT * FROM %s", hiveTable)).containsOnly(EXPECTED_ROWS.get(1));

        // Verify view behavior
        String hiveView = "test_hive_view" + randomTableSuffix();
        onTrino().executeQuery(format(
                "CREATE VIEW %s AS SELECT * FROM (SELECT * FROM %s union all SELECT * FROM %s)",
                fullName(HIVE_CATALOG, DEFAULT_SCHEMA, hiveView),
                fullName(HIVE_CATALOG, DEFAULT_SCHEMA, hiveTable),
                fullName(HIVE_CATALOG, DEFAULT_SCHEMA, icebergTable)));
        assertThat(onTrinoHive("SELECT * FROM %s", hiveView))
                .containsOnly(ImmutableList.<Row>builder()
                        // From redirected icebergTable
                        .addAll(EXPECTED_ROWS)
                        // From hiveTable
                        .add(EXPECTED_ROWS.get(1))
                        .build());

        String icebergMaterializedView = "test_materialized_view" + randomTableSuffix();
        onTrinoIceberg("CREATE MATERIALIZED VIEW %s AS SELECT 1 value", icebergMaterializedView);
        // SELECT on materialized view is not redirected to iceberg
        assertThatThrownBy(() -> onTrinoHive("SELECT * FROM %s", icebergMaterializedView))
                .hasMessageContaining("View data missing prefix: ");

        assertThatThrownBy(() -> onTrinoIceberg("SELECT * FROM %s", hiveTable))
                .hasMessageContaining(format("Not an Iceberg table: %s.%s", DEFAULT_SCHEMA, hiveTable));

        onTrinoHive("DROP VIEW %s", hiveView);
        onTrinoIceberg("DROP MATERIALIZED VIEW %s", icebergMaterializedView);
        dropWithTrinoIceberg(icebergTable);
        dropWithTrinoHive(hiveTable);
    }

    @Test(groups = {HIVE_REDIRECTION_TO_ICEBERG, PROFILE_SPECIFIC_TESTS})
    public void testColumnListing()
    {
        String testSchema = "redirection_schema_" + randomTableSuffix();
        onTrino().executeQuery(format("CREATE SCHEMA %s.%s", ICEBERG_CATALOG, testSchema));

        String testTable = createOnTrinoIceberg(testSchema, "test_info_schema_columns");

        List<Row> expectedColumns = ImmutableList.<Row>builder()
                .add(row(HIVE_CATALOG, testSchema, testTable, "_string"))
                .add(row(HIVE_CATALOG, testSchema, testTable, "_bigint"))
                .add(row(HIVE_CATALOG, testSchema, testTable, "_date"))
                .build();

        assertThat(onTrino().executeQuery(format("SELECT table_catalog, table_schema, table_name, column_name FROM %s.information_schema.columns", HIVE_CATALOG)))
                .contains(expectedColumns);
        assertThat(onTrino().executeQuery(format("SELECT table_cat, table_schem, table_name, column_name FROM system.jdbc.columns WHERE table_cat='%s'", HIVE_CATALOG)))
                .contains(expectedColumns);

        assertThat(onTrino().executeQuery(format("SELECT table_catalog, table_schema, table_name, column_name FROM %s.information_schema.columns WHERE table_schema = '%s'", HIVE_CATALOG, testSchema)))
                .containsOnly(expectedColumns);
        assertThat(onTrino().executeQuery(format("SELECT table_cat, table_schem, table_name, column_name FROM system.jdbc.columns WHERE table_cat='%s' AND table_schem='%s'", HIVE_CATALOG, testSchema)))
                .containsOnly(expectedColumns);

        assertThat(onTrino().executeQuery(format(
                "SELECT table_catalog, table_schema, table_name, column_name"
                        + " FROM %s.information_schema.columns"
                        + " WHERE table_schema = '%s' AND table_name = '%s'",
                HIVE_CATALOG,
                testSchema,
                testTable)))
                .containsOnly(expectedColumns);
        assertThat(onTrino().executeQuery(format(
                "SELECT table_cat, table_schem, table_name, column_name"
                        + " FROM system.jdbc.columns"
                        + " WHERE table_cat='%s' AND table_schem='%s' AND table_name = '%s'",
                HIVE_CATALOG,
                testSchema,
                testTable)))
                .containsOnly(expectedColumns);
        assertThat(onTrino().executeQuery(format("SHOW COLUMNS FROM %s", fullName(HIVE_CATALOG, testSchema, testTable))).project(1))
                .containsOnly(row("_string"), row("_bigint"), row("_date"));

        dropWithTrinoIceberg(testSchema, testTable);
        onTrino().executeQuery(format("DROP SCHEMA %s.%s", ICEBERG_CATALOG, testSchema));
    }

    @Test(groups = {HIVE_REDIRECTION_TO_ICEBERG, PROFILE_SPECIFIC_TESTS})
    public void testTableListing()
    {
        String testSchema = "redirection_schema_" + randomTableSuffix();
        onTrino().executeQuery(format("CREATE SCHEMA %s.%s", ICEBERG_CATALOG, testSchema));
        String testTable = createOnTrinoIceberg(testSchema, "test_info_schema_tables");

        List<Row> expectedTableEntry = ImmutableList.of(row(HIVE_CATALOG, testSchema, testTable));

        assertThat(onTrino().executeQuery(format("SELECT table_catalog, table_schema, table_name FROM %s.information_schema.tables", HIVE_CATALOG)))
                .contains(expectedTableEntry);
        assertThat(onTrino().executeQuery(format("SELECT table_cat, table_schem, table_name FROM system.jdbc.tables WHERE table_cat='%s'", HIVE_CATALOG)))
                .contains(expectedTableEntry);

        assertThat(onTrino().executeQuery(format("SELECT table_catalog, table_schema, table_name FROM %s.information_schema.tables WHERE table_schema = '%s'", HIVE_CATALOG, testSchema)))
                .containsOnly(expectedTableEntry);
        assertThat(onTrino().executeQuery(format("SELECT table_cat, table_schem, table_name FROM system.jdbc.tables WHERE table_cat='%s' AND table_schem='%s'", HIVE_CATALOG, testSchema)))
                .containsOnly(expectedTableEntry);
        assertThat(onTrino().executeQuery(format("SHOW TABLES FROM %s.%s", HIVE_CATALOG, testSchema)))
                .containsOnly(row(testTable));

        // Assert the case for filters pointing to a specific table
        assertThat(onTrino().executeQuery(format(
                "SELECT table_catalog, table_schema, table_name"
                        + " FROM %s.information_schema.tables"
                        + " WHERE table_schema = '%s' AND table_name = '%s'",
                HIVE_CATALOG,
                testSchema,
                testTable)))
                .containsOnly(expectedTableEntry);
        assertThat(onTrino().executeQuery(format(
                "SELECT table_cat, table_schem, table_name"
                        + " FROM system.jdbc.tables"
                        + " WHERE table_cat='%s' AND table_schem='%s' AND table_name = '%s'",
                HIVE_CATALOG,
                testSchema,
                testTable)))
                .containsOnly(expectedTableEntry);

        dropWithTrinoIceberg(testSchema, testTable);
        onTrino().executeQuery(format("DROP SCHEMA %s.%s", ICEBERG_CATALOG, testSchema));
    }

    private static QueryResult onTrinoIceberg(String queryTemplate, String tableName)
    {
        return onTrinoIceberg(queryTemplate, DEFAULT_SCHEMA, tableName);
    }

    private static QueryResult onTrinoIceberg(String queryTemplate, String schemaName, String tableName)
    {
        return onTrino().executeQuery(format(queryTemplate, fullName(ICEBERG_CATALOG, schemaName, tableName)));
    }

    private static String createOnTrinoIceberg(String tableNamePrefix)
    {
        return createOnTrinoIceberg(DEFAULT_SCHEMA, tableNamePrefix);
    }

    private static String createOnTrinoIceberg(String schemaName, String tableNamePrefix)
    {
        String tableName = tableNamePrefix + randomTableSuffix();
        onTrinoIceberg(CREATE_TABLE_TEMPLATE_ICEBERG, schemaName, tableName);
        return tableName;
    }

    private static void insertWithTrinoIceberg(String tableName)
    {
        onTrinoIceberg(INSERT_TEMPLATE, DEFAULT_SCHEMA, tableName);
    }

    private static void dropWithTrinoIceberg(String schemaName, String tableName)
    {
        onTrinoIceberg("DROP TABLE %s", schemaName, tableName);
    }

    private static void dropWithTrinoIceberg(String tableName)
    {
        dropWithTrinoIceberg(DEFAULT_SCHEMA, tableName);
    }

    private static QueryResult onTrinoHive(String queryTemplate, String tableName)
    {
        return onTrino().executeQuery(format(queryTemplate, fullName(HIVE_CATALOG, DEFAULT_SCHEMA, tableName)));
    }

    private static String createOnTrinoHive(String tableNamePrefix)
    {
        String tableName = tableNamePrefix + randomTableSuffix();
        onTrinoHive(CREATE_TABLE_TEMPLATE_HIVE, tableName);
        return tableName;
    }

    private static void insertWithTrinoHive(String tableName)
    {
        onTrinoHive(INSERT_TEMPLATE, tableName);
    }

    private static void dropWithTrinoHive(String tableName)
    {
        onTrinoHive("DROP TABLE %s", tableName);
    }

    private static String fullName(String catalogName, String schemaName, String tableName)
    {
        return format("%s.%s.%s", catalogName, schemaName, tableName);
    }

    private static final String notSupportedError(String operation, String tableName)
    {
        return format(
                ".*Failed to perform operation '%s'. It is not supported on table '%s' because the table is redirected to '%s'",
                operation,
                fullName(HIVE_CATALOG, DEFAULT_SCHEMA, tableName),
                fullName(ICEBERG_CATALOG, DEFAULT_SCHEMA, tableName));
    }
}
