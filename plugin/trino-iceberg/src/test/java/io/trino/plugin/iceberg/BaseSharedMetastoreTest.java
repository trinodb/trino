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
package io.trino.plugin.iceberg;

import io.trino.testing.AbstractTestQueryFramework;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class BaseSharedMetastoreTest
        extends AbstractTestQueryFramework
{
    /**
     * This schema should contain only nation and region tables. Use {@link #testSchema} when creating new tables.
     */
    protected final String tpchSchema = "test_tpch_shared_schema_" + randomNameSuffix();
    protected final String testSchema = "test_mutable_shared_schema_" + randomNameSuffix();

    protected abstract String getExpectedHiveCreateSchema(String catalogName);

    protected abstract String getExpectedIcebergCreateSchema(String catalogName);

    @Test
    public void testSelect()
    {
        assertQuery("SELECT * FROM iceberg." + tpchSchema + ".nation", "SELECT * FROM nation");
        assertQuery("SELECT * FROM hive." + tpchSchema + ".region", "SELECT * FROM region");
        assertQuery("SELECT * FROM hive_with_redirections." + tpchSchema + ".nation", "SELECT * FROM nation");
        assertQuery("SELECT * FROM hive_with_redirections." + tpchSchema + ".region", "SELECT * FROM region");
        assertQuery("SELECT * FROM iceberg_with_redirections." + tpchSchema + ".nation", "SELECT * FROM nation");
        assertQuery("SELECT * FROM iceberg_with_redirections." + tpchSchema + ".region", "SELECT * FROM region");

        assertThat(query("SELECT * FROM iceberg." + tpchSchema + ".region"))
                .failure().hasMessageContaining("Not an Iceberg table");
        assertThat(query("SELECT * FROM iceberg." + tpchSchema + ".\"region$data\""))
                .failure().hasMessageMatching(".* Table .* does not exist");
        assertThat(query("SELECT * FROM iceberg." + tpchSchema + ".\"region$files\""))
                .failure().hasMessageMatching(".* Table .* does not exist");

        assertThat(query("SELECT * FROM hive." + tpchSchema + ".nation"))
                .failure().hasMessageContaining("Cannot query Iceberg table");
        assertThat(query("SELECT * FROM hive." + tpchSchema + ".\"nation$partitions\""))
                .failure().hasMessageMatching(".* Table .* does not exist");
        assertThat(query("SELECT * FROM hive." + tpchSchema + ".\"nation$properties\""))
                .failure().hasMessageMatching(".* Table .* does not exist");
    }

    @Test
    public void testReadInformationSchema()
    {
        assertThat(query("SELECT table_schema FROM hive.information_schema.tables WHERE table_name = 'region' AND table_schema='" + tpchSchema + "'"))
                .skippingTypesCheck()
                .containsAll("VALUES '" + tpchSchema + "'");
        assertThat(query("SELECT table_schema FROM iceberg.information_schema.tables WHERE table_name = 'nation' AND table_schema='" + tpchSchema + "'"))
                .skippingTypesCheck()
                .containsAll("VALUES '" + tpchSchema + "'");
        assertThat(query("SELECT table_schema FROM hive_with_redirections.information_schema.tables WHERE table_name = 'region' AND table_schema='" + tpchSchema + "'"))
                .skippingTypesCheck()
                .containsAll("VALUES '" + tpchSchema + "'");
        assertThat(query("SELECT table_schema FROM hive_with_redirections.information_schema.tables WHERE table_name = 'nation' AND table_schema='" + tpchSchema + "'"))
                .skippingTypesCheck()
                .containsAll("VALUES '" + tpchSchema + "'");
        assertThat(query("SELECT table_schema FROM iceberg_with_redirections.information_schema.tables WHERE table_name = 'region' AND table_schema='" + tpchSchema + "'"))
                .skippingTypesCheck()
                .containsAll("VALUES '" + tpchSchema + "'");

        assertQuery("SELECT table_name, column_name from hive.information_schema.columns WHERE table_schema = '" + tpchSchema + "'",
                "VALUES ('region', 'regionkey'), ('region', 'name'), ('region', 'comment')");
        assertQuery("SELECT table_name, column_name from iceberg.information_schema.columns WHERE table_schema = '" + tpchSchema + "'",
                "VALUES ('nation', 'nationkey'), ('nation', 'name'), ('nation', 'regionkey'), ('nation', 'comment')");
        assertQuery("SELECT table_name, column_name from hive_with_redirections.information_schema.columns WHERE table_schema = '" + tpchSchema + "'",
                "VALUES" +
                        "('region', 'regionkey'), ('region', 'name'), ('region', 'comment'), " +
                        "('nation', 'nationkey'), ('nation', 'name'), ('nation', 'regionkey'), ('nation', 'comment')");
        assertQuery("SELECT table_name, column_name from iceberg_with_redirections.information_schema.columns WHERE table_schema = '" + tpchSchema + "'",
                "VALUES" +
                        "('region', 'regionkey'), ('region', 'name'), ('region', 'comment'), " +
                        "('nation', 'nationkey'), ('nation', 'name'), ('nation', 'regionkey'), ('nation', 'comment')");
    }

    @Test
    void testHiveSelectTableColumns()
    {
        assertThat(query("SELECT table_cat, table_schem, table_name, column_name FROM system.jdbc.columns WHERE table_cat = 'hive' AND table_schem = '" + tpchSchema + "' AND table_name = 'region'"))
                .skippingTypesCheck()
                .matches("VALUES " +
                        "('hive', '" + tpchSchema + "', 'region', 'regionkey')," +
                        "('hive', '" + tpchSchema + "', 'region', 'name')," +
                        "('hive', '" + tpchSchema + "', 'region', 'comment')");

        // Hive does not show any information about tables with unsupported format
        assertQueryReturnsEmptyResult("SELECT table_cat, table_schem, table_name, column_name FROM system.jdbc.columns WHERE table_cat = 'hive' AND table_schem = '" + tpchSchema + "' AND table_name = 'nation'");
    }

    @Test
    public void testShowTables()
    {
        assertQuery("SHOW TABLES FROM iceberg." + tpchSchema, "VALUES 'region', 'nation'");
        assertQuery("SHOW TABLES FROM hive." + tpchSchema, "VALUES 'region', 'nation'");
        assertQuery("SHOW TABLES FROM hive_with_redirections." + tpchSchema, "VALUES 'region', 'nation'");
        assertQuery("SHOW TABLES FROM iceberg_with_redirections." + tpchSchema, "VALUES 'region', 'nation'");

        assertThat(query("SHOW CREATE TABLE iceberg." + tpchSchema + ".region"))
                .failure().hasMessageContaining("Not an Iceberg table");
        assertThat(query("SHOW CREATE TABLE hive." + tpchSchema + ".nation"))
                .failure().hasMessageContaining("Cannot query Iceberg table");

        assertThat(query("DESCRIBE iceberg." + tpchSchema + ".region"))
                .failure().hasMessageContaining("Not an Iceberg table");
        assertThat(query("DESCRIBE hive." + tpchSchema + ".nation"))
                .failure().hasMessageContaining("Cannot query Iceberg table");
    }

    @Test
    public void testShowSchemas()
    {
        assertThat(query("SHOW SCHEMAS FROM hive"))
                .skippingTypesCheck()
                .containsAll("VALUES '" + tpchSchema + "'");
        assertThat(query("SHOW SCHEMAS FROM iceberg"))
                .skippingTypesCheck()
                .containsAll("VALUES '" + tpchSchema + "'");
        assertThat(query("SHOW SCHEMAS FROM hive_with_redirections"))
                .skippingTypesCheck()
                .containsAll("VALUES '" + tpchSchema + "'");

        String showCreateHiveSchema = (String) computeActual("SHOW CREATE SCHEMA hive." + tpchSchema).getOnlyValue();
        assertThat(showCreateHiveSchema).isEqualTo(getExpectedHiveCreateSchema("hive"));
        String showCreateIcebergSchema = (String) computeActual("SHOW CREATE SCHEMA iceberg." + tpchSchema).getOnlyValue();
        assertThat(showCreateIcebergSchema).isEqualTo(getExpectedIcebergCreateSchema("iceberg"));
        String showCreateHiveWithRedirectionsSchema = (String) computeActual("SHOW CREATE SCHEMA hive_with_redirections." + tpchSchema).getOnlyValue();
        assertThat(showCreateHiveWithRedirectionsSchema).isEqualTo(getExpectedHiveCreateSchema("hive_with_redirections"));
        String showCreateIcebergWithRedirectionsSchema = (String) computeActual("SHOW CREATE SCHEMA iceberg_with_redirections." + tpchSchema).getOnlyValue();
        assertThat(showCreateIcebergWithRedirectionsSchema).isEqualTo(getExpectedIcebergCreateSchema("iceberg_with_redirections"));
    }

    @Test
    public void testIcebergTablesFunction()
    {
        assertQuery("SELECT * FROM TABLE(iceberg.system.iceberg_tables(SCHEMA_NAME => '%s'))".formatted(tpchSchema), "VALUES ('%s', 'nation')".formatted(tpchSchema));
        assertQuery("SELECT * FROM TABLE(iceberg_with_redirections.system.iceberg_tables(SCHEMA_NAME => '%s'))".formatted(tpchSchema), "VALUES ('%s', 'nation')".formatted(tpchSchema));
    }

    @Test
    public void testTimeTravelWithRedirection()
            throws InterruptedException
    {
        try {
            assertUpdate(format("CREATE TABLE iceberg.%s.nation_test AS SELECT * FROM nation", testSchema), 25);
            assertQuery("SELECT * FROM hive_with_redirections." + testSchema + ".nation_test", "SELECT * FROM nation");
            long snapshot1 = getLatestSnapshotId(testSchema);
            long v1EpochMillis = getCommittedAtInEpochMilliSeconds(snapshot1, testSchema);
            Thread.sleep(1);
            assertUpdate(format("INSERT INTO hive_with_redirections.%s.nation_test VALUES(25, 'POLAND', 3, 'test 1')", testSchema), 1);
            long snapshot2 = getLatestSnapshotId(testSchema);
            long v2EpochMillis = getCommittedAtInEpochMilliSeconds(snapshot2, testSchema);
            Thread.sleep(1);
            assertUpdate(format("INSERT INTO hive_with_redirections.%s.nation_test VALUES(26, 'CHILE', 1, 'test 2')", testSchema), 1);
            long snapshot3 = getLatestSnapshotId(testSchema);
            long v3EpochMillis = getCommittedAtInEpochMilliSeconds(snapshot3, testSchema);
            long incorrectSnapshot = 2324324333L;
            Thread.sleep(1);
            assertQuery(format("SELECT * FROM hive_with_redirections.%s.nation_test FOR VERSION AS OF %d", testSchema, snapshot1), "SELECT * FROM nation");
            assertQuery(format("SELECT * FROM hive_with_redirections.%s.nation_test FOR TIMESTAMP AS OF %s", testSchema, timestampLiteral(v1EpochMillis)), "SELECT * FROM nation");
            assertQuery(format("SELECT count(*) FROM hive_with_redirections.%s.nation_test FOR VERSION AS OF %d", testSchema, snapshot2), "VALUES(26)");
            assertQuery(format(
                    "SELECT count(*) FROM iceberg_with_redirections.%s.nation_test FOR TIMESTAMP AS OF %s", testSchema, timestampLiteral(v2EpochMillis)), "VALUES(26)");
            assertQuery(format("SELECT count(*) FROM hive_with_redirections.%s.nation_test FOR VERSION AS OF %d", testSchema, snapshot3), "VALUES(27)");
            assertQuery(format(
                    "SELECT count(*) FROM hive_with_redirections.%s.nation_test FOR TIMESTAMP AS OF %s", testSchema, timestampLiteral(v3EpochMillis)), "VALUES(27)");
            assertQueryFails(format("SELECT * FROM hive_with_redirections.%s.nation_test FOR VERSION AS OF %d", testSchema, incorrectSnapshot), "Iceberg snapshot ID does not exists: " + incorrectSnapshot);
            assertQueryFails(
                    format("SELECT * FROM hive_with_redirections.%s.nation_test FOR TIMESTAMP AS OF TIMESTAMP '1970-01-01 00:00:00.001000000 Z'", testSchema),
                    format("\\QNo version history table \"%s\".\"nation_test\" at or before 1970-01-01T00:00:00.001Z", testSchema));
            assertQueryFails(
                    format("SELECT * FROM iceberg_with_redirections.%s.region FOR TIMESTAMP AS OF TIMESTAMP '1970-01-01 00:00:00.001000000 Z'", tpchSchema),
                    "\\QThis connector does not support versioned tables");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS iceberg." + testSchema + ".nation_test");
        }
    }

    @Test
    void testIcebergCannotCreateTableNamesakeToHiveTable()
    {
        String tableName = "test_iceberg_create_namesake_hive_table_" + randomNameSuffix();
        String hiveTableName = "hive.%s.%s".formatted(testSchema, tableName);
        String icebergTableName = "iceberg.%s.%s".formatted(testSchema, tableName);

        assertUpdate("CREATE TABLE " + hiveTableName + "(a bigint)");
        assertThat(query("CREATE TABLE " + icebergTableName + "(a bigint)"))
                .failure().hasMessageMatching(".* Table .* of unsupported type already exists");

        assertUpdate("DROP TABLE " + hiveTableName);
    }

    @Test
    void testHiveCannotCreateTableNamesakeToIcebergTable()
    {
        String tableName = "test_iceberg_create_namesake_hive_table_" + randomNameSuffix();
        String hiveTableName = "hive.%s.%s".formatted(testSchema, tableName);
        String icebergTableName = "iceberg.%s.%s".formatted(testSchema, tableName);

        assertUpdate("CREATE TABLE " + icebergTableName + "(a bigint)");
        assertThat(query("CREATE TABLE " + hiveTableName + "(a bigint)"))
                .failure().hasMessageMatching(".* Table .* of unsupported type already exists");

        assertUpdate("DROP TABLE " + icebergTableName);
    }

    @Test
    public void testMigrateTable()
    {
        String tableName = "test_migrate_" + randomNameSuffix();
        String hiveTableName = "hive.%s.%s".formatted(testSchema, tableName);
        String icebergTableName = "iceberg.%s.%s".formatted(testSchema, tableName);

        assertUpdate("CREATE TABLE " + hiveTableName + " AS SELECT 1 id", 1);
        assertQueryFails("SELECT * FROM " + icebergTableName, "Not an Iceberg table: .*");

        assertUpdate("CALL iceberg.system.migrate('" + testSchema + "', '" + tableName + "')");
        assertQuery("SELECT * FROM " + icebergTableName, "VALUES 1");

        assertUpdate("DROP TABLE " + icebergTableName);
    }

    @Test
    public void testMigratePartitionedTable()
    {
        String tableName = "test_migrate_" + randomNameSuffix();
        String hiveTableName = "hive.%s.%s".formatted(testSchema, tableName);
        String icebergTableName = "iceberg.%s.%s".formatted(testSchema, tableName);

        assertUpdate("CREATE TABLE " + hiveTableName + " WITH (partitioned_by = ARRAY['part']) AS SELECT 1 id, 'test' part", 1);
        assertQueryFails("SELECT * FROM " + icebergTableName, "Not an Iceberg table: .*");

        assertUpdate("CALL iceberg.system.migrate('" + testSchema + "', '" + tableName + "')");
        assertQuery("SELECT * FROM " + icebergTableName, "VALUES (1, 'test')");

        assertUpdate("DROP TABLE " + icebergTableName);
    }

    private long getLatestSnapshotId(String schema)
    {
        return (long) computeScalar(format("SELECT snapshot_id FROM iceberg.%s.\"nation_test$snapshots\" ORDER BY committed_at DESC FETCH FIRST 1 ROW WITH TIES", schema));
    }

    private long getCommittedAtInEpochMilliSeconds(long snapshotId, String schema)
    {
        return ((ZonedDateTime) computeScalar(format("SELECT committed_at FROM iceberg.%s.\"nation_test$snapshots\" WHERE snapshot_id=%s", schema, snapshotId)))
                .toInstant().toEpochMilli();
    }

    private static String timestampLiteral(long epochMilliSeconds)
    {
        return DateTimeFormatter.ofPattern("'TIMESTAMP '''uuuu-MM-dd HH:mm:ss." + "S".repeat(9) + " VV''")
                .format(Instant.ofEpochMilli(epochMilliSeconds).atZone(UTC));
    }
}
