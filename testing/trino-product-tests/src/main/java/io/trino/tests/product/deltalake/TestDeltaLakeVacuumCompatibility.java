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

import org.testng.annotations.Test;

import static io.trino.tests.product.TestGroups.DELTA_LAKE_DATABRICKS;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_OSS;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.hive.util.TemporaryHiveTable.randomTableSuffix;
import static io.trino.tests.product.utils.QueryExecutors.onDelta;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestDeltaLakeVacuumCompatibility
        extends BaseTestDeltaLakeS3Storage
{
    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testVacuumOnUnsupportedTableVersion()
    {
        String tableName = "test_dl_create_table_compat_" + randomTableSuffix();
        String tableDirectory = "databricks-compatibility-test-" + tableName;

        onDelta().executeQuery(format("" +
                        "CREATE TABLE default.%s (col int) " +
                        "USING DELTA LOCATION 's3://%s/%s'" +
                        "TBLPROPERTIES ('delta.minWriterVersion'='4')",
                tableName,
                bucketName,
                tableDirectory));
        onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES 1, 2, 3, 4");

        assertThatThrownBy(() -> onTrino().executeQuery("CALL delta.system.vacuum('default', '" + tableName + "', '7d')"))
                .hasMessageContaining("Table default." + tableName + " requires Delta Lake writer version 4 which is not supported");
    }
}
