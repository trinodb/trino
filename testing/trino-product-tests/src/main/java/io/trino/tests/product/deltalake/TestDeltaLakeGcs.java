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
package io.trino.tests.product.deltalake;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.trino.tempto.ProductTest;
import org.testng.annotations.Test;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_GCS;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDeltaLakeGcs
        extends ProductTest
{
    @Inject
    @Named("databases.hive.warehouse_directory_path")
    private String warehouseDirectory;

    @Test(groups = {DELTA_LAKE_GCS, PROFILE_SPECIFIC_TESTS})
    public void testCreateAndSelectNationTable()
    {
        String tableName = "nation_" + randomNameSuffix();
        onTrino().executeQuery(format(
                "CREATE TABLE delta.default.%1$s WITH (location = '%2$s/%1$s') AS SELECT * FROM tpch.tiny.nation",
                tableName,
                warehouseDirectory));

        assertThat(onTrino().executeQuery("SELECT count(*) FROM delta.default." + tableName)).containsOnly(row(25));
        onTrino().executeQuery("DROP TABLE delta.default." + tableName);
    }

    @Test(groups = {DELTA_LAKE_GCS, PROFILE_SPECIFIC_TESTS})
    public void testBasicWriteOperations()
    {
        String tableName = "table_write_operations_" + randomNameSuffix();
        onTrino().executeQuery(format(
                "CREATE TABLE delta.default.%1$s (a_bigint bigint, a_varchar varchar) WITH (location = '%2$s/%1$s')",
                tableName,
                warehouseDirectory));

        onTrino().executeQuery(format("INSERT INTO delta.default.%s VALUES (1, 'hello world')".formatted(tableName)));
        assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName)).containsOnly(row(1L, "hello world"));

        onTrino().executeQuery(format("UPDATE delta.default.%s SET a_varchar = 'hallo Welt' WHERE a_bigint = 1".formatted(tableName)));
        assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName)).containsOnly(row(1L, "hallo Welt"));

        onTrino().executeQuery(format("DELETE FROM delta.default.%s WHERE a_bigint = 1".formatted(tableName)));
        assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName)).hasNoRows();
        onTrino().executeQuery("DROP TABLE delta.default." + tableName);
    }
}
