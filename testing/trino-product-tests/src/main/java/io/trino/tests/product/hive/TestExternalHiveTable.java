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

import com.google.inject.Inject;
import io.trino.tempto.ProductTest;
import io.trino.tempto.Requirement;
import io.trino.tempto.RequirementsProvider;
import io.trino.tempto.configuration.Configuration;
import io.trino.tempto.fulfillment.table.TableInstance;
import io.trino.tempto.hadoop.hdfs.HdfsClient;
import org.testng.annotations.Test;

import static io.trino.plugin.hive.HiveMetadata.MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE;
import static io.trino.tempto.Requirements.compose;
import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertQueryFailure;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.tempto.fulfillment.table.MutableTablesState.mutableTablesState;
import static io.trino.tempto.fulfillment.table.TableRequirements.mutableTable;
import static io.trino.tempto.fulfillment.table.hive.tpch.TpchTableDefinitions.NATION;
import static io.trino.tests.product.hive.HiveTableDefinitions.NATION_PARTITIONED_BY_BIGINT_REGIONKEY;
import static io.trino.tests.product.hive.HiveTableDefinitions.NATION_PARTITIONED_BY_REGIONKEY_NUMBER_OF_LINES_PER_SPLIT;
import static io.trino.tests.product.utils.QueryExecutors.onHive;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;

public class TestExternalHiveTable
        extends ProductTest
        implements RequirementsProvider
{
    private static final String HIVE_CATALOG_WITH_EXTERNAL_WRITES = "hive_with_external_writes";
    private static final String EXTERNAL_TABLE_NAME = "target_table";

    @Inject
    private HdfsClient hdfsClient;

    @Override
    public Requirement getRequirements(Configuration configuration)
    {
        return compose(
                mutableTable(NATION),
                mutableTable(NATION_PARTITIONED_BY_BIGINT_REGIONKEY));
    }

    @Test
    public void testShowStatisticsForExternalTable()
    {
        TableInstance<?> nation = mutableTablesState().get(NATION_PARTITIONED_BY_BIGINT_REGIONKEY.getName());
        onHive().executeQuery("DROP TABLE IF EXISTS " + EXTERNAL_TABLE_NAME);
        onHive().executeQuery("CREATE EXTERNAL TABLE " + EXTERNAL_TABLE_NAME + " LIKE " + nation.getNameInDatabase() + " LOCATION '/tmp/" + EXTERNAL_TABLE_NAME + "_" + nation.getNameInDatabase() + "'");
        insertNationPartition(nation, 1);

        onHive().executeQuery("ANALYZE TABLE " + EXTERNAL_TABLE_NAME + " PARTITION (p_regionkey) COMPUTE STATISTICS");
        assertThat(onTrino().executeQuery("SHOW STATS FOR " + EXTERNAL_TABLE_NAME)).containsOnly(
                row("p_nationkey", null, null, null, null, null, null),
                row("p_name", null, null, null, null, null, null),
                row("p_comment", null, null, null, null, null, null),
                row("p_regionkey", null, 1.0, 0.0, null, "1", "1"),
                row(null, null, null, null, 5.0, null, null));

        onHive().executeQuery("ANALYZE TABLE " + EXTERNAL_TABLE_NAME + " PARTITION (p_regionkey) COMPUTE STATISTICS FOR COLUMNS");
        onTrino().executeQuery("CALL system.flush_metadata_cache()");
        assertThat(onTrino().executeQuery("SHOW STATS FOR " + EXTERNAL_TABLE_NAME)).containsOnly(
                row("p_nationkey", null, 5.0, 0.0, null, "1", "24"),
                row("p_name", 38.0, 5.0, 0.0, null, null, null),
                row("p_comment", 499.0, 5.0, 0.0, null, null, null),
                row("p_regionkey", null, 1.0, 0.0, null, "1", "1"),
                row(null, null, null, null, 5.0, null, null));
    }

    @Test
    public void testAnalyzeExternalTable()
    {
        TableInstance<?> nation = mutableTablesState().get(NATION_PARTITIONED_BY_BIGINT_REGIONKEY.getName());
        onHive().executeQuery("DROP TABLE IF EXISTS " + EXTERNAL_TABLE_NAME);
        onHive().executeQuery("CREATE EXTERNAL TABLE " + EXTERNAL_TABLE_NAME + " LIKE " + nation.getNameInDatabase() + " LOCATION '/tmp/" + EXTERNAL_TABLE_NAME + "_" + nation.getNameInDatabase() + "'");
        insertNationPartition(nation, 1);

        // Running ANALYZE on an external table is allowed as long as the user has the privileges.
        assertThat(onTrino().executeQuery("ANALYZE hive.default." + EXTERNAL_TABLE_NAME)).containsExactlyInOrder(row(5));
    }

    @Test
    public void testInsertIntoExternalTable()
    {
        TableInstance<?> nation = mutableTablesState().get(NATION.getName());
        onHive().executeQuery("DROP TABLE IF EXISTS " + EXTERNAL_TABLE_NAME);
        onHive().executeQuery("CREATE EXTERNAL TABLE " + EXTERNAL_TABLE_NAME + " LIKE " + nation.getNameInDatabase());
        assertQueryFailure(() -> onTrino().executeQuery(
                "INSERT INTO hive.default." + EXTERNAL_TABLE_NAME + " SELECT * FROM hive.default." + nation.getNameInDatabase()))
                .hasMessageContaining("Cannot write to non-managed Hive table");
    }

    @Test
    public void testDeleteFromExternalTable()
    {
        TableInstance<?> nation = mutableTablesState().get(NATION.getName());
        onHive().executeQuery("DROP TABLE IF EXISTS " + EXTERNAL_TABLE_NAME);
        onHive().executeQuery("CREATE EXTERNAL TABLE " + EXTERNAL_TABLE_NAME + " LIKE " + nation.getNameInDatabase());
        assertQueryFailure(() -> onTrino().executeQuery("DELETE FROM hive.default." + EXTERNAL_TABLE_NAME))
                .hasMessageContaining("Cannot delete from non-managed Hive table");
    }

    @Test
    public void testDeleteFromExternalPartitionedTableTable()
    {
        TableInstance<?> nation = mutableTablesState().get(NATION_PARTITIONED_BY_BIGINT_REGIONKEY.getName());
        onHive().executeQuery("DROP TABLE IF EXISTS " + EXTERNAL_TABLE_NAME);
        onHive().executeQuery("CREATE EXTERNAL TABLE " + EXTERNAL_TABLE_NAME + " LIKE " + nation.getNameInDatabase() + " LOCATION '/tmp/" + EXTERNAL_TABLE_NAME + "_" + nation.getNameInDatabase() + "'");
        insertNationPartition(nation, 1);
        insertNationPartition(nation, 2);
        insertNationPartition(nation, 3);
        assertThat(onTrino().executeQuery("SELECT * FROM " + EXTERNAL_TABLE_NAME))
                .hasRowsCount(3 * NATION_PARTITIONED_BY_REGIONKEY_NUMBER_OF_LINES_PER_SPLIT);

        assertQueryFailure(() -> onTrino().executeQuery("DELETE FROM hive.default." + EXTERNAL_TABLE_NAME + " WHERE p_name IS NOT NULL"))
                .hasMessageContaining(MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);

        onTrino().executeQuery("DELETE FROM hive.default." + EXTERNAL_TABLE_NAME + " WHERE p_regionkey = 1");
        assertThat(onTrino().executeQuery("SELECT * FROM " + EXTERNAL_TABLE_NAME))
                .hasRowsCount(2 * NATION_PARTITIONED_BY_REGIONKEY_NUMBER_OF_LINES_PER_SPLIT);

        onTrino().executeQuery("DELETE FROM hive.default." + EXTERNAL_TABLE_NAME);
        assertThat(onTrino().executeQuery("SELECT * FROM " + EXTERNAL_TABLE_NAME)).hasRowsCount(0);
    }

    @Test
    public void testCreateExternalTableWithInaccessibleSchemaLocation()
    {
        String schema = "schema_without_location";
        String schemaLocation = "/tmp/" + schema;
        hdfsClient.createDirectory(schemaLocation);
        onTrino().executeQuery(format("CREATE SCHEMA %s.%s WITH (location='%s')", HIVE_CATALOG_WITH_EXTERNAL_WRITES, schema, schemaLocation));

        hdfsClient.delete(schemaLocation);

        String table = "test_create_external";
        String tableLocation = "/tmp/" + table;
        onTrino().executeQuery(format("CREATE TABLE %s.%s.%s WITH (external_location = '%s') AS SELECT * FROM tpch.tiny.nation", HIVE_CATALOG_WITH_EXTERNAL_WRITES, schema, table, tableLocation));
    }

    private void insertNationPartition(TableInstance<?> nation, int partition)
    {
        onHive().executeQuery(
                "INSERT INTO TABLE " + EXTERNAL_TABLE_NAME + " PARTITION (p_regionkey=" + partition + ")"
                        + " SELECT p_nationkey, p_name, p_comment FROM " + nation.getNameInDatabase()
                        + " WHERE p_regionkey=" + partition);
    }
}
