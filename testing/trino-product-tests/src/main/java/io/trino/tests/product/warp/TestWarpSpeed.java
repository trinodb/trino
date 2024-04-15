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
import io.trino.tempto.BeforeMethodWithContext;
import org.testng.ITestContext;
import org.testng.annotations.Test;

import java.util.Arrays;

import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.TestGroups.WARP_SPEED_DELTA_LAKE;
import static io.trino.tests.product.TestGroups.WARP_SPEED_ICEBERG;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;

public class TestWarpSpeed
{
    private static final String CATALOG_NAME = "warp";
    private static final String SCHEMA_NAME = "warp_product_tests"; // AWS Glue Database name, must have a Location URI

    @Inject
    WarpSpeedTests warpSpeedTests;

    private boolean initialized;

    public TestWarpSpeed() {}

    @BeforeMethodWithContext
    public void before()
            throws Exception
    {
        synchronized (this) {
            if (!initialized) {
                onTrino().executeQuery(format("USE %s.%s", CATALOG_NAME, SCHEMA_NAME));
                initialized = true;
            }
        }
    }

    @Test(groups = {/*WARP_SPEED_HIVE_2,*/ WARP_SPEED_DELTA_LAKE, WARP_SPEED_ICEBERG, PROFILE_SPECIFIC_TESTS})
    public void testWarpSimpleQuery(ITestContext iTestContext)
    {
        String testName = Arrays.stream(iTestContext.getAllTestMethods()).findFirst().orElseThrow().getMethodName();
        warpSpeedTests.testWarpSimpleQuery(CATALOG_NAME, SCHEMA_NAME, testName);
    }

    /* ToDo: temporarily commented out until fixing
    @Test(groups = {WARP_SPEED_HIVE_2, PROFILE_SPECIFIC_TESTS})
    public void testWarpBucketedBy(ITestContext iTestContext)
    {
        String testName = Arrays.stream(iTestContext.getAllTestMethods()).findFirst().orElseThrow().getMethodName();
        warpSpeedTests.testWarpBucketedBy(CATALOG_NAME, SCHEMA_NAME, testName);
    }
    */
}
