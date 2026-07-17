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

import io.trino.tempto.AfterMethodWithContext;
import io.trino.tempto.BeforeMethodWithContext;
import io.trino.tempto.ProductTest;
import io.trino.testing.minio.MinioClient;
import org.testng.annotations.Test;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_MINIO;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class BaseTestDeltaLakeMinioReads
        extends ProductTest
{
    protected static final String BUCKET_NAME = "delta-test-basic-reads";

    protected MinioClient client;

    protected String tableName;
    protected String regionResourcePath;

    public BaseTestDeltaLakeMinioReads(String regionTableName, String regionResourcePath)
    {
        this.tableName = requireNonNull(regionTableName, "regionTableName is null");
        this.regionResourcePath = requireNonNull(regionResourcePath, "regionResourcePath is null");
    }

    @BeforeMethodWithContext
    public void setUp()
    {
        client = new MinioClient();

        client.copyResourcePath(BUCKET_NAME, regionResourcePath, tableName);
    }

    @AfterMethodWithContext
    public void tearDown()
    {
        client.close();
        client = null;
    }

    @Test(groups = {DELTA_LAKE_MINIO, PROFILE_SPECIFIC_TESTS})
    public void testReadRegionTable()
    {
        onTrino().executeQuery(format("CALL delta.system.register_table('default', '%1$s', 's3://%2$s/%1$s')", tableName, BUCKET_NAME));

        assertThat(onTrino().executeQuery(
                format("SELECT count(name) FROM delta.default.\"%s\"", tableName)))
                .containsOnly(row(5L));

        onTrino().executeQuery(format("DROP TABLE delta.default.\"%s\"", tableName));
    }
}
