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
package io.trino.tests.product.hudi;

import com.google.common.collect.ImmutableList;
import io.trino.tempto.BeforeTestWithContext;
import io.trino.tempto.ProductTest;
import io.trino.tempto.assertions.QueryAssert;
import org.testng.annotations.Test;

import java.util.List;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.tests.product.TestGroups.HUDI;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.hive.util.TemporaryHiveTable.randomTableSuffix;
import static io.trino.tests.product.utils.QueryExecutors.onHudi;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class TestHudiCompatibility
        extends ProductTest
{
    protected String bucketName;

    @BeforeTestWithContext
    public void setUp()
    {
        bucketName = requireNonNull(System.getenv("S3_BUCKET"), "Environment variable not set: S3_BUCKET");
    }

    @Test(groups = {HUDI, PROFILE_SPECIFIC_TESTS})
    public void testDemo()
    {
        String tableName = "test_hudi_demo_" + randomTableSuffix();
        String tableDirectory = "hudi-compatibility-test-" + tableName;

        onHudi().executeQuery(format("CREATE TABLE default.%s (uuid int, col string) USING hudi LOCATION 's3://%s/%s'",
                tableName,
                bucketName,
                tableDirectory));

        onHudi().executeQuery("insert into default." + tableName + " select 1, 'Trino'");
        onHudi().executeQuery("insert into default." + tableName + " select 2, 'rocks'");

        List<QueryAssert.Row> expectedRows = ImmutableList.of(
                row(1, "Trino"),
                row(2, "rocks"));

        try {
            assertThat(onHudi().executeQuery("SELECT uuid, col FROM default." + tableName))
                    .containsOnly(expectedRows);
            assertThat(onTrino().executeQuery("SELECT uuid, col FROM hudi.default." + tableName))
                    .containsOnly(expectedRows);
        }
        finally {
            onHudi().executeQuery("DROP TABLE default." + tableName);
        }
    }
}
