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

package io.trino.tests.product.warp;

import com.google.inject.Inject;
import io.trino.tempto.AfterMethodWithContext;
import io.trino.tempto.BeforeMethodWithContext;
import io.trino.testing.minio.MinioClient;
import org.testng.ITestContext;
import org.testng.annotations.Test;

import java.util.Arrays;

import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.TestGroups.WARP_SPEED_MINIO;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static io.trino.tests.product.warp.utils.TestUtils.countMethodsWithAnnotation;
import static java.lang.String.format;

public class TestWarpSpeedMinio
{
    private static final String BUCKET_NAME = "product-tests-warp-speed";
    private static final String CATALOG_NAME = "warp";
    private static final String SCHEMA_NAME = "product_tests";

    private MinioClient client;
    private boolean initialized;
    private int finished;

    @Inject
    WarpSpeedTests warpSpeedTests;

    @BeforeMethodWithContext
    public void setUp()
    {
        synchronized (this) {
            if (!initialized) {
                initialize();
                initialized = true;
            }
        }
    }

    @AfterMethodWithContext
    public void tearDown()
    {
        synchronized (this) {
            finished++;
            if (finished == countMethodsWithAnnotation(TestWarpSpeedMinio.class, Test.class)) {
                teardown();
            }
        }
    }

    private void initialize()
    {
        client = new MinioClient();
        client.ensureBucketExists(BUCKET_NAME);

        onTrino().executeQuery(format("CREATE SCHEMA IF NOT EXISTS %s.%s WITH (location = 's3://%s/')", CATALOG_NAME, SCHEMA_NAME, BUCKET_NAME));
    }

    private void teardown()
    {
        if (client != null) {
            onTrino().executeQuery(format("DROP SCHEMA IF EXISTS %s.%s", CATALOG_NAME, SCHEMA_NAME));

            client.close();
            client = null;
        }
    }

    @Test(groups = {WARP_SPEED_MINIO, PROFILE_SPECIFIC_TESTS})
    public void testWarpSimpleQuery(ITestContext iTestContext)
    {
        String testName = Arrays.stream(iTestContext.getAllTestMethods()).findFirst().orElseThrow().getMethodName();
        warpSpeedTests.testWarpSimpleQuery(CATALOG_NAME, SCHEMA_NAME, testName);
    }

    @Test(groups = {WARP_SPEED_MINIO, PROFILE_SPECIFIC_TESTS})
    public void testWarpBucketedBy(ITestContext iTestContext)
    {
        String testName = Arrays.stream(iTestContext.getAllTestMethods()).findFirst().orElseThrow().getMethodName();
        warpSpeedTests.testWarpBucketedBy(CATALOG_NAME, SCHEMA_NAME, testName);
    }
}
