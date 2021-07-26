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

import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.tempto.query.QueryExecutor.query;
import static io.trino.tests.product.utils.QueryExecutors.onHive;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestCreateDropSchema
        extends ProductTest
{
    @Inject
    private HdfsClient hdfsClient;

    @javax.inject.Inject
    @Named("databases.hive.warehouse_directory_path")
    private String warehouseDirectory;

    @Test
    public void testCreateDropSchema()
    {
        onHive().executeQuery("DROP DATABASE IF EXISTS test_drop_schema CASCADE");

        onTrino().executeQuery("CREATE SCHEMA test_drop_schema");
        assertTrue(hdfsClient.exist(warehouseDirectory + "/test_drop_schema.db"));

        onTrino().executeQuery("CREATE TABLE test_drop_schema.test_drop (col1 int)");
        assertThat(() -> query("DROP SCHEMA test_drop_schema"))
                .failsWithMessage("line 1:1: Cannot drop non-empty schema 'test_drop_schema'");

        onTrino().executeQuery("DROP TABLE test_drop_schema.test_drop");

        onTrino().executeQuery("DROP SCHEMA test_drop_schema");
        assertFalse(hdfsClient.exist(warehouseDirectory + "/test_drop_schema.db"));
    }
}
