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

import io.trino.testing.containers.HdfsClient;
import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static io.trino.plugin.hive.HiveMetadata.MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE;
import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

/**
 * JUnit 5 port of TestExternalHiveTable.
 * <p>
 * Tests external Hive table functionality in the Hive connector.
 */
@ProductTest
@RequiresEnvironment(HiveBasicEnvironment.class)
@TestGroup.HdfsNoImpersonation
@TestInstance(PER_CLASS)
class TestExternalHiveTable
{
    private static final String EXTERNAL_TABLE_NAME = "target_table";
    private static final int NATION_PARTITIONED_BY_REGIONKEY_NUMBER_OF_LINES_PER_SPLIT = 5;

    // Table names for nation and partitioned nation data
    private static final String NATION_TABLE = "nation_for_external_tests";
    private static final String NATION_PARTITIONED_TABLE = "nation_partitioned_for_external_tests";

    @BeforeEach
    void setUp(HiveBasicEnvironment env)
    {
        // Create nation table from TPCH data (similar to mutable NATION table)
        env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + NATION_TABLE);
        env.executeTrinoUpdate("CREATE TABLE hive.default." + NATION_TABLE + " AS SELECT * FROM tpch.tiny.nation");

        // Create partitioned nation table (similar to NATION_PARTITIONED_BY_BIGINT_REGIONKEY)
        env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + NATION_PARTITIONED_TABLE);
        env.executeTrinoUpdate(
                "CREATE TABLE hive.default." + NATION_PARTITIONED_TABLE + " (" +
                        "  p_nationkey BIGINT," +
                        "  p_name VARCHAR(25)," +
                        "  p_comment VARCHAR(152)," +
                        "  p_regionkey BIGINT" +
                        ") WITH (partitioned_by = ARRAY['p_regionkey'])");

        // Populate with nation data for regions 1, 2, 3 (5 rows each)
        env.executeTrinoUpdate(
                "INSERT INTO hive.default." + NATION_PARTITIONED_TABLE +
                        " SELECT nationkey, name, comment, regionkey FROM tpch.tiny.nation WHERE regionkey IN (1, 2, 3)");

        // Clean up external table from any previous run
        env.executeHiveUpdate("DROP TABLE IF EXISTS " + EXTERNAL_TABLE_NAME);
    }

    @Test
    void testShowStatisticsForExternalTable(HiveBasicEnvironment env)
    {
        String location = "/tmp/" + EXTERNAL_TABLE_NAME + "_" + NATION_PARTITIONED_TABLE;

        env.executeHiveUpdate("DROP TABLE IF EXISTS " + EXTERNAL_TABLE_NAME);
        env.executeHiveUpdate("CREATE EXTERNAL TABLE " + EXTERNAL_TABLE_NAME + " LIKE " + NATION_PARTITIONED_TABLE + " LOCATION '" + location + "'");
        insertNationPartition(env, NATION_PARTITIONED_TABLE, 1);

        env.executeHiveUpdate("ANALYZE TABLE " + EXTERNAL_TABLE_NAME + " PARTITION (p_regionkey) COMPUTE STATISTICS");
        assertThat(env.executeTrino("SHOW STATS FOR " + EXTERNAL_TABLE_NAME)).containsOnly(
                row("p_nationkey", null, null, null, null, null, null),
                row("p_name", null, null, null, null, null, null),
                row("p_comment", null, null, null, null, null, null),
                row("p_regionkey", null, 1.0, 0.0, null, "1", "1"),
                row(null, null, null, null, 5.0, null, null));

        env.executeHiveUpdate("ANALYZE TABLE " + EXTERNAL_TABLE_NAME + " PARTITION (p_regionkey) COMPUTE STATISTICS FOR COLUMNS");
        env.executeTrinoUpdate("CALL system.flush_metadata_cache()");
        assertThat(env.executeTrino("SHOW STATS FOR " + EXTERNAL_TABLE_NAME)).containsOnly(
                row("p_nationkey", null, 5.0, 0.0, null, "1", "24"),
                row("p_name", 38.0, 5.0, 0.0, null, null, null),
                row("p_comment", 500.0, 5.0, 0.0, null, null, null),
                row("p_regionkey", null, 1.0, 0.0, null, "1", "1"),
                row(null, null, null, null, 5.0, null, null));
    }

    @Test
    void testAnalyzeExternalTable(HiveBasicEnvironment env)
    {
        String location = "/tmp/" + EXTERNAL_TABLE_NAME + "_" + NATION_PARTITIONED_TABLE;

        env.executeHiveUpdate("DROP TABLE IF EXISTS " + EXTERNAL_TABLE_NAME);
        env.executeHiveUpdate("CREATE EXTERNAL TABLE " + EXTERNAL_TABLE_NAME + " LIKE " + NATION_PARTITIONED_TABLE + " LOCATION '" + location + "'");
        insertNationPartition(env, NATION_PARTITIONED_TABLE, 1);

        // Running ANALYZE on an external table is allowed as long as the user has the privileges.
        assertThat(env.executeTrino("ANALYZE hive.default." + EXTERNAL_TABLE_NAME)).containsExactlyInOrder(row(5L));
    }

    @Test
    void testInsertIntoExternalTable(HiveBasicEnvironment env)
    {
        env.executeHiveUpdate("DROP TABLE IF EXISTS " + EXTERNAL_TABLE_NAME);
        env.executeHiveUpdate("CREATE EXTERNAL TABLE " + EXTERNAL_TABLE_NAME + " LIKE " + NATION_TABLE);

        assertThatThrownBy(() -> env.executeTrinoUpdate(
                "INSERT INTO hive.default." + EXTERNAL_TABLE_NAME + " SELECT * FROM hive.default." + NATION_TABLE))
                .hasMessageContaining("Cannot write to non-managed Hive table");
    }

    @Test
    void testDeleteFromExternalTable(HiveBasicEnvironment env)
    {
        env.executeHiveUpdate("DROP TABLE IF EXISTS " + EXTERNAL_TABLE_NAME);
        env.executeHiveUpdate("CREATE EXTERNAL TABLE " + EXTERNAL_TABLE_NAME + " LIKE " + NATION_TABLE);

        assertThatThrownBy(() -> env.executeTrinoUpdate("DELETE FROM hive.default." + EXTERNAL_TABLE_NAME))
                .hasMessageContaining("Cannot delete from non-managed Hive table");
    }

    @Test
    void testDeleteFromExternalPartitionedTableTable(HiveBasicEnvironment env)
    {
        String location = "/tmp/" + EXTERNAL_TABLE_NAME + "_" + NATION_PARTITIONED_TABLE;

        env.executeHiveUpdate("DROP TABLE IF EXISTS " + EXTERNAL_TABLE_NAME);
        env.executeHiveUpdate("CREATE EXTERNAL TABLE " + EXTERNAL_TABLE_NAME + " LIKE " + NATION_PARTITIONED_TABLE + " LOCATION '" + location + "'");
        insertNationPartition(env, NATION_PARTITIONED_TABLE, 1);
        insertNationPartition(env, NATION_PARTITIONED_TABLE, 2);
        insertNationPartition(env, NATION_PARTITIONED_TABLE, 3);
        assertThat(env.executeTrino("SELECT * FROM " + EXTERNAL_TABLE_NAME))
                .hasRowsCount(3 * NATION_PARTITIONED_BY_REGIONKEY_NUMBER_OF_LINES_PER_SPLIT);

        assertThatThrownBy(() -> env.executeTrinoUpdate("DELETE FROM hive.default." + EXTERNAL_TABLE_NAME + " WHERE p_name IS NOT NULL"))
                .hasMessageContaining(MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);

        env.executeTrinoUpdate("DELETE FROM hive.default." + EXTERNAL_TABLE_NAME + " WHERE p_regionkey = 1");
        assertThat(env.executeTrino("SELECT * FROM " + EXTERNAL_TABLE_NAME))
                .hasRowsCount(2 * NATION_PARTITIONED_BY_REGIONKEY_NUMBER_OF_LINES_PER_SPLIT);

        env.executeTrinoUpdate("DELETE FROM hive.default." + EXTERNAL_TABLE_NAME);
        assertThat(env.executeTrino("SELECT * FROM " + EXTERNAL_TABLE_NAME)).hasRowsCount(0);
    }

    @Test
    void testCreateExternalTableWithInaccessibleSchemaLocation(HiveBasicEnvironment env)
    {
        String schema = "schema_without_location_" + System.currentTimeMillis();
        String schemaLocation = "/tmp/" + schema;
        HdfsClient hdfsClient = env.createHdfsClient();
        String table = "test_create_external";
        String tableLocation = "/tmp/" + table + "_" + System.currentTimeMillis();

        try {
            hdfsClient.createDirectory(schemaLocation);
            env.executeTrinoUpdate(format("CREATE SCHEMA hive_with_external_writes.%s WITH (location='%s')", schema, schemaLocation));

            hdfsClient.delete(schemaLocation);

            env.executeTrinoUpdate(format("CREATE TABLE hive_with_external_writes.%s.%s WITH (external_location = '%s') AS SELECT * FROM tpch.tiny.nation", schema, table, tableLocation));

            // Verify the table was created and has data
            assertThat(env.executeTrino(format("SELECT count(*) FROM hive_with_external_writes.%s.%s", schema, table)))
                    .containsOnly(row(25L));
        }
        finally {
            env.executeTrinoUpdate(format("DROP TABLE IF EXISTS hive_with_external_writes.%s.%s", schema, table));
            env.executeTrinoUpdate(format("DROP SCHEMA IF EXISTS hive_with_external_writes.%s", schema));
            hdfsClient.delete(tableLocation);
        }
    }

    private void insertNationPartition(HiveBasicEnvironment env, String sourceTable, int partition)
    {
        env.executeHiveUpdate(
                "INSERT INTO TABLE " + EXTERNAL_TABLE_NAME + " PARTITION (p_regionkey=" + partition + ")"
                        + " SELECT p_nationkey, p_name, p_comment FROM " + sourceTable
                        + " WHERE p_regionkey=" + partition);
    }
}
