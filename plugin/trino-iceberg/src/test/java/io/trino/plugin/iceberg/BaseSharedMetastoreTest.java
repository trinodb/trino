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
import org.testng.annotations.Test;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

public abstract class BaseSharedMetastoreTest
        extends AbstractTestQueryFramework
{
    protected final String schema = "test_shared_schema_" + randomNameSuffix();

    protected abstract String getExpectedHiveCreateSchema(String catalogName);

    protected abstract String getExpectedIcebergCreateSchema(String catalogName);

    @Test
    public void testSelect()
    {
        assertQuery("SELECT * FROM iceberg." + schema + ".nation", "SELECT * FROM nation");
        assertQuery("SELECT * FROM hive." + schema + ".region", "SELECT * FROM region");
        assertQuery("SELECT * FROM hive_with_redirections." + schema + ".nation", "SELECT * FROM nation");
        assertQuery("SELECT * FROM hive_with_redirections." + schema + ".region", "SELECT * FROM region");
        assertQuery("SELECT * FROM iceberg_with_redirections." + schema + ".nation", "SELECT * FROM nation");
        assertQuery("SELECT * FROM iceberg_with_redirections." + schema + ".region", "SELECT * FROM region");

        assertThatThrownBy(() -> query("SELECT * FROM iceberg." + schema + ".region"))
                .hasMessageContaining("Not an Iceberg table");
        assertThatThrownBy(() -> query("SELECT * FROM hive." + schema + ".nation"))
                .hasMessageContaining("Cannot query Iceberg table");
    }

    @Test
    public void testReadInformationSchema()
    {
        assertThat(query("SELECT table_schema FROM hive.information_schema.tables WHERE table_name = 'region' AND table_schema='" + schema + "'"))
                .skippingTypesCheck()
                .containsAll("VALUES '" + schema + "'");
        assertThat(query("SELECT table_schema FROM iceberg.information_schema.tables WHERE table_name = 'nation' AND table_schema='" + schema + "'"))
                .skippingTypesCheck()
                .containsAll("VALUES '" + schema + "'");
        assertThat(query("SELECT table_schema FROM hive_with_redirections.information_schema.tables WHERE table_name = 'region' AND table_schema='" + schema + "'"))
                .skippingTypesCheck()
                .containsAll("VALUES '" + schema + "'");
        assertThat(query("SELECT table_schema FROM hive_with_redirections.information_schema.tables WHERE table_name = 'nation' AND table_schema='" + schema + "'"))
                .skippingTypesCheck()
                .containsAll("VALUES '" + schema + "'");
        assertThat(query("SELECT table_schema FROM iceberg_with_redirections.information_schema.tables WHERE table_name = 'region' AND table_schema='" + schema + "'"))
                .skippingTypesCheck()
                .containsAll("VALUES '" + schema + "'");

        assertQuery("SELECT table_name, column_name from hive.information_schema.columns WHERE table_schema = '" + schema + "'",
                "VALUES ('region', 'regionkey'), ('region', 'name'), ('region', 'comment')");
        assertQuery("SELECT table_name, column_name from iceberg.information_schema.columns WHERE table_schema = '" + schema + "'",
                "VALUES ('nation', 'nationkey'), ('nation', 'name'), ('nation', 'regionkey'), ('nation', 'comment')");
        assertQuery("SELECT table_name, column_name from hive_with_redirections.information_schema.columns WHERE table_schema = '" + schema + "'",
                "VALUES" +
                        "('region', 'regionkey'), ('region', 'name'), ('region', 'comment'), " +
                        "('nation', 'nationkey'), ('nation', 'name'), ('nation', 'regionkey'), ('nation', 'comment')");
        assertQuery("SELECT table_name, column_name from iceberg_with_redirections.information_schema.columns WHERE table_schema = '" + schema + "'",
                "VALUES" +
                        "('region', 'regionkey'), ('region', 'name'), ('region', 'comment'), " +
                        "('nation', 'nationkey'), ('nation', 'name'), ('nation', 'regionkey'), ('nation', 'comment')");
    }

    @Test
    public void testShowTables()
    {
        assertQuery("SHOW TABLES FROM iceberg." + schema, "VALUES 'region', 'nation'");
        assertQuery("SHOW TABLES FROM hive." + schema, "VALUES 'region', 'nation'");
        assertQuery("SHOW TABLES FROM hive_with_redirections." + schema, "VALUES 'region', 'nation'");
        assertQuery("SHOW TABLES FROM iceberg_with_redirections." + schema, "VALUES 'region', 'nation'");

        assertThatThrownBy(() -> query("SHOW CREATE TABLE iceberg." + schema + ".region"))
                .hasMessageContaining("Not an Iceberg table");
        assertThatThrownBy(() -> query("SHOW CREATE TABLE hive." + schema + ".nation"))
                .hasMessageContaining("Cannot query Iceberg table");

        assertThatThrownBy(() -> query("DESCRIBE iceberg." + schema + ".region"))
                .hasMessageContaining("Not an Iceberg table");
        assertThatThrownBy(() -> query("DESCRIBE hive." + schema + ".nation"))
                .hasMessageContaining("Cannot query Iceberg table");
    }

    @Test
    public void testShowSchemas()
    {
        assertThat(query("SHOW SCHEMAS FROM hive"))
                .skippingTypesCheck()
                .containsAll("VALUES '" + schema + "'");
        assertThat(query("SHOW SCHEMAS FROM iceberg"))
                .skippingTypesCheck()
                .containsAll("VALUES '" + schema + "'");
        assertThat(query("SHOW SCHEMAS FROM hive_with_redirections"))
                .skippingTypesCheck()
                .containsAll("VALUES '" + schema + "'");

        String showCreateHiveSchema = (String) computeActual("SHOW CREATE SCHEMA hive." + schema).getOnlyValue();
        assertEquals(
                showCreateHiveSchema,
                getExpectedHiveCreateSchema("hive"));
        String showCreateIcebergSchema = (String) computeActual("SHOW CREATE SCHEMA iceberg." + schema).getOnlyValue();
        assertEquals(
                showCreateIcebergSchema,
                getExpectedIcebergCreateSchema("iceberg"));
        String showCreateHiveWithRedirectionsSchema = (String) computeActual("SHOW CREATE SCHEMA hive_with_redirections." + schema).getOnlyValue();
        assertEquals(
                showCreateHiveWithRedirectionsSchema,
                getExpectedHiveCreateSchema("hive_with_redirections"));
        String showCreateIcebergWithRedirectionsSchema = (String) computeActual("SHOW CREATE SCHEMA iceberg_with_redirections." + schema).getOnlyValue();
        assertEquals(
                showCreateIcebergWithRedirectionsSchema,
                getExpectedIcebergCreateSchema("iceberg_with_redirections"));
    }

    @Test
    public void testTimeTravelWithRedirection()
            throws InterruptedException
    {
        String testLocalSchema = "test_schema_" + randomNameSuffix();
        try {
            assertUpdate("CREATE SCHEMA iceberg. " + testLocalSchema);
            assertUpdate(format("CREATE TABLE iceberg.%s.nation_test AS SELECT * FROM nation", testLocalSchema), 25);
            assertQuery("SELECT * FROM hive_with_redirections." + testLocalSchema + ".nation_test", "SELECT * FROM nation");
            long snapshot1 = getLatestSnapshotId(testLocalSchema);
            long v1EpochMillis = getCommittedAtInEpochMilliSeconds(snapshot1, testLocalSchema);
            Thread.sleep(1);
            assertUpdate(format("INSERT INTO hive_with_redirections.%s.nation_test VALUES(25, 'POLAND', 3, 'test 1')", testLocalSchema), 1);
            long snapshot2 = getLatestSnapshotId(testLocalSchema);
            long v2EpochMillis = getCommittedAtInEpochMilliSeconds(snapshot2, testLocalSchema);
            Thread.sleep(1);
            assertUpdate(format("INSERT INTO hive_with_redirections.%s.nation_test VALUES(26, 'CHILE', 1, 'test 2')", testLocalSchema), 1);
            long snapshot3 = getLatestSnapshotId(testLocalSchema);
            long v3EpochMillis = getCommittedAtInEpochMilliSeconds(snapshot3, testLocalSchema);
            long incorrectSnapshot = 2324324333L;
            Thread.sleep(1);
            assertQuery(format("SELECT * FROM hive_with_redirections.%s.nation_test FOR VERSION AS OF %d", testLocalSchema, snapshot1), "SELECT * FROM nation");
            assertQuery(format("SELECT * FROM hive_with_redirections.%s.nation_test FOR TIMESTAMP AS OF %s", testLocalSchema, timestampLiteral(v1EpochMillis)), "SELECT * FROM nation");
            assertQuery(format("SELECT count(*) FROM hive_with_redirections.%s.nation_test FOR VERSION AS OF %d", testLocalSchema, snapshot2), "VALUES(26)");
            assertQuery(format(
                    "SELECT count(*) FROM iceberg_with_redirections.%s.nation_test FOR TIMESTAMP AS OF %s", testLocalSchema, timestampLiteral(v2EpochMillis)), "VALUES(26)");
            assertQuery(format("SELECT count(*) FROM hive_with_redirections.%s.nation_test FOR VERSION AS OF %d", testLocalSchema, snapshot3), "VALUES(27)");
            assertQuery(format(
                    "SELECT count(*) FROM hive_with_redirections.%s.nation_test FOR TIMESTAMP AS OF %s", testLocalSchema, timestampLiteral(v3EpochMillis)), "VALUES(27)");
            assertQueryFails(format("SELECT * FROM hive_with_redirections.%s.nation_test FOR VERSION AS OF %d", testLocalSchema, incorrectSnapshot), "Iceberg snapshot ID does not exists: " + incorrectSnapshot);
            assertQueryFails(
                    format("SELECT * FROM hive_with_redirections.%s.nation_test FOR TIMESTAMP AS OF TIMESTAMP '1970-01-01 00:00:00.001000000 Z'", testLocalSchema),
                    format("\\QNo version history table \"%s\".\"nation_test\" at or before 1970-01-01T00:00:00.001Z", testLocalSchema));
            assertQueryFails(
                    format("SELECT * FROM iceberg_with_redirections.%s.region FOR TIMESTAMP AS OF TIMESTAMP '1970-01-01 00:00:00.001000000 Z'", schema),
                    "\\QThis connector does not support versioned tables");
        }
        finally {
            query("DROP TABLE IF EXISTS iceberg." + testLocalSchema + ".nation_test");
            query("DROP SCHEMA IF EXISTS iceberg." + testLocalSchema);
        }
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
