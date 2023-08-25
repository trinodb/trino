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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectListing;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.trino.tempto.BeforeMethodWithContext;
import io.trino.testng.services.Flaky;
import io.trino.tests.product.hive.Engine;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_DATABRICKS;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_OSS;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.DATABRICKS_COMMUNICATION_FAILURE_ISSUE;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.DATABRICKS_COMMUNICATION_FAILURE_MATCH;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.dropDeltaTableWithRetry;
import static io.trino.tests.product.hive.Engine.DELTA;
import static io.trino.tests.product.hive.Engine.TRINO;
import static io.trino.tests.product.utils.QueryExecutors.onDelta;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDeltaLakeDropTableCompatibility
        extends BaseTestDeltaLakeS3Storage
{
    @Inject
    @Named("s3.server_type")
    private String s3ServerType;

    private AmazonS3 s3;

    @BeforeMethodWithContext
    public void setup()
    {
        super.setUp();
        s3 = new S3ClientFactory().createS3Client(s3ServerType);
    }

    @DataProvider
    public static Object[][] engineConfigurations()
    {
        return new Object[][] {
                {TRINO, TRINO, true},
                {TRINO, TRINO, false},
                {TRINO, DELTA, true},
                {TRINO, DELTA, false},
                {DELTA, TRINO, true},
                {DELTA, TRINO, false},
                {DELTA, DELTA, true},
                {DELTA, DELTA, false},
        };
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS}, dataProvider = "engineConfigurations")
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testDropTable(Engine creator, Engine dropper, boolean explicitLocation)
    {
        testDropTableAccuracy(creator, dropper, explicitLocation);
    }

    private void testDropTableAccuracy(Engine creator, Engine dropper, boolean explicitLocation)
    {
        String schemaName = "test_schema_with_location_" + randomNameSuffix();
        String schemaLocation = format("s3://%s/databricks-compatibility-test-%s", bucketName, schemaName);
        String tableName = explicitLocation ? "test_external_table" : "test_managed_table";
        Optional<String> tableLocation = explicitLocation
                ? Optional.of(format("s3://" + bucketName + "/databricks-compatibility-test-%s/%s", schemaName, tableName))
                : Optional.empty();

        switch (creator) {
            case TRINO -> onTrino().executeQuery(format("CREATE SCHEMA delta.%s WITH (location = '%s')", schemaName, schemaLocation));
            case DELTA -> onDelta().executeQuery(format("CREATE SCHEMA %s LOCATION \"%s\"", schemaName, schemaLocation));
            default -> throw new UnsupportedOperationException("Unsupported engine: " + creator);
        }
        try {
            onTrino().executeQuery("USE delta." + schemaName);
            switch (creator) {
                case TRINO -> onTrino().executeQuery(format(
                        "CREATE TABLE %s.%s (a, b) %s AS VALUES (1, 2), (2, 3), (3, 4)",
                        schemaName,
                        tableName,
                        tableLocation.map(location -> "WITH (location = '" + location + "')").orElse("")));
                case DELTA -> onDelta().executeQuery(format(
                        "CREATE TABLE %s.%s USING DELTA %s AS VALUES (1, 2), (2, 3), (3, 4)",
                        schemaName,
                        tableName,
                        tableLocation.map(location -> "LOCATION \"" + location + "\"").orElse("")));
                default -> throw new UnsupportedOperationException("Unsupported engine: " + creator);
            }

            ObjectListing tableFiles = s3.listObjects(bucketName, "databricks-compatibility-test-" + schemaName + "/" + tableName);
            assertThat(tableFiles.getObjectSummaries()).isNotEmpty();

            switch (dropper) {
                case DELTA -> dropDeltaTableWithRetry(schemaName + "." + tableName);
                case TRINO -> onTrino().executeQuery("DROP TABLE " + schemaName + "." + tableName);
                default -> throw new UnsupportedOperationException("Unsupported engine: " + dropper);
            }
            tableFiles = s3.listObjects(bucketName, "databricks-compatibility-test-" + schemaName + "/" + tableName);
            if (explicitLocation) {
                assertThat(tableFiles.getObjectSummaries()).isNotEmpty();
            }
            else {
                assertThat(tableFiles.getObjectSummaries()).isEmpty();
            }
        }
        finally {
            dropDeltaTableWithRetry(schemaName + "." + tableName);
            onDelta().executeQuery("DROP SCHEMA " + schemaName);
        }
    }
}
