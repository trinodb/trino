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
import io.trino.tempto.fulfillment.table.MutableTablesState;
import io.trino.tempto.hadoop.hdfs.HdfsClient;
import org.testng.annotations.Test;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.tempto.fulfillment.table.MutableTableRequirement.State.LOADED;
import static io.trino.tempto.fulfillment.table.TableRequirements.mutableTable;
import static io.trino.tempto.query.QueryExecutor.query;
import static io.trino.tests.product.hive.HiveTableDefinitions.NATION_PARTITIONED_BY_BIGINT_REGIONKEY;
import static io.trino.tests.product.hive.util.TableLocationUtils.getTablePath;
import static java.lang.String.format;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestHiveIgnoreAbsentPartitions
        extends ProductTest
        implements RequirementsProvider
{
    @Inject
    private MutableTablesState tablesState;

    @Inject
    private HdfsClient hdfsClient;

    @Override
    public Requirement getRequirements(Configuration configuration)
    {
        return mutableTable(NATION_PARTITIONED_BY_BIGINT_REGIONKEY, "test_table", LOADED);
    }

    @Test
    public void testIgnoreAbsentPartitions()
            throws Exception
    {
        String tableNameInDatabase = tablesState.get("test_table").getNameInDatabase();
        String tablePath = getTablePath(tableNameInDatabase, 1);
        String partitionPath = format("%s/p_regionkey=9999", tablePath);

        assertThat(query("SELECT count(*) FROM " + tableNameInDatabase)).containsOnly(row(15));

        assertFalse(hdfsClient.exist(partitionPath), format("Expected partition %s to not exist", tableNameInDatabase));
        query(format("CALL hive.system.create_empty_partition('default', '%s', array['p_regionkey'], array['9999'])", tableNameInDatabase));

        query("SET SESSION hive.ignore_absent_partitions = false");
        hdfsClient.delete(partitionPath);
        assertFalse(hdfsClient.exist(partitionPath), format("Expected partition %s to not exist", partitionPath));
        assertThat(() -> query("SELECT count(*) FROM " + tableNameInDatabase)).failsWithMessage("Partition location does not exist");

        query("SET SESSION hive.ignore_absent_partitions = true");
        assertThat(query("SELECT count(*) FROM " + tableNameInDatabase)).containsOnly(row(15));
    }

    @Test
    public void testShouldThrowErrorOnUnpartitionedTableMissingData()
            throws Exception
    {
        String tableName = "unpartitioned_absent_table_data";

        assertThat(query("DROP TABLE IF EXISTS " + tableName));

        assertThat(query(format("CREATE TABLE %s AS SELECT * FROM (VALUES 1,2,3) t(dummy_col)", tableName))).containsOnly(row(3));
        assertThat(query("SELECT count(*) FROM " + tableName)).containsOnly(row(3));

        String tablePath = getTablePath(tableName, 0);
        assertTrue(hdfsClient.exist(tablePath));
        hdfsClient.delete(tablePath);

        query("SET SESSION hive.ignore_absent_partitions = false");
        assertThat(() -> query("SELECT count(*) FROM " + tableName)).failsWithMessage("Partition location does not exist");

        query("SET SESSION hive.ignore_absent_partitions = true");
        assertThat(() -> query("SELECT count(*) FROM " + tableName)).failsWithMessage("Partition location does not exist");

        assertThat(query("DROP TABLE " + tableName));
    }
}
