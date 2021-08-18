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
import com.google.inject.name.Named;
import io.trino.tempto.ProductTest;
import io.trino.tempto.hadoop.hdfs.HdfsClient;
import org.testng.annotations.Test;

import static io.trino.tempto.assertions.QueryAssert.assertQueryFailure;
import static io.trino.tempto.query.QueryExecutor.query;
import static io.trino.tests.product.TestGroups.ALTER_TABLE;
import static io.trino.tests.product.TestGroups.SMOKE;
import static io.trino.tests.product.TestGroups.TRINO_JDBC;
import static io.trino.tests.product.hive.util.TableLocationUtils.getExternalTableLocation;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

public class TestAlterTableLocation
        extends ProductTest
{
    @Inject
    @Named("databases.hive.warehouse_directory_path")
    private String warehouseDirectory;

    @Inject
    private HdfsClient hdfsClient;

    @Test(groups = {ALTER_TABLE, SMOKE, TRINO_JDBC})
    public void testAlterTable()
    {
        String tableName = "test_alter_table_location";
        String newLocation = tableLocation("test_alter_table_location_new");

        hdfsClient.createDirectory(newLocation);
        query("DROP TABLE IF EXISTS " + tableName);
        query(format("CREATE TABLE default.%s (col int) WITH (external_location = '/tmp')", tableName));

        assertEquals(getExternalTableLocation(tableName), "hdfs://hadoop-master:9000/tmp");
        query(format("CALL system.alter_table_location('default', '%s', '%s')", tableName, newLocation));
        assertEquals(getExternalTableLocation(tableName), "hdfs://hadoop-master:9000/user/hive/warehouse/test_alter_table_location_new");

        query("DROP TABLE " + tableName);
        hdfsClient.delete(newLocation);
    }

    @Test(groups = {ALTER_TABLE, SMOKE, TRINO_JDBC})
    public void testInvalidLocation()
    {
        String tableName = "test_alter_invalid_table_location";

        query("DROP TABLE IF EXISTS " + tableName);
        query(format("CREATE TABLE default.%s (col int) WITH (external_location = '/tmp')", tableName));

        assertQueryFailure(() -> query(format("CALL system.alter_table_location('default', '%s', '/tmp/invalid')", tableName)))
                .hasMessageMatching("Query failed (.*): External location is not a valid file system URI: /tmp/invalid");

        query("DROP TABLE " + tableName);
    }

    @Test(groups = {ALTER_TABLE, SMOKE, TRINO_JDBC})
    public void testInvalidTableType()
    {
        String tableName = "test_alter_invalid_table_type";
        String location = tableLocation(tableName);

        query("DROP TABLE IF EXISTS " + tableName);
        query(format("CREATE TABLE default.%s (col int)", tableName));

        assertQueryFailure(() -> query(format("CALL system.alter_table_location('default', '%s', '%s')", tableName, location)))
                .hasMessageMatching("Query failed (.*): Invalid table type: MANAGED_TABLE");

        query("DROP TABLE " + tableName);
    }

    private String tableLocation(String tableName)
    {
        return warehouseDirectory + '/' + tableName;
    }
}
