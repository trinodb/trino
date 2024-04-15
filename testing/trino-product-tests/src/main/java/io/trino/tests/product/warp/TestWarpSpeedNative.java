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
import io.airlift.log.Logger;
import io.trino.plugin.varada.util.FailureGeneratorInvocationHandler;
import io.trino.plugin.warp.extension.execution.debugtools.FailureGeneratorResource;
import io.trino.plugin.warp.extension.execution.debugtools.NativeStorageStateResource;
import io.trino.plugin.warp.gen.constants.FailureRepetitionMode;
import io.trino.tempto.AfterMethodWithContext;
import io.trino.tempto.BeforeMethodWithContext;
import io.trino.tempto.query.QueryExecutor;
import io.trino.tests.product.warp.utils.RestUtils;
import jakarta.ws.rs.HttpMethod;
import org.testng.ITestContext;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.List;

import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.TestGroups.WARP_SPEED_HIVE_2;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;

public class TestWarpSpeedNative
{
    private static final Logger logger = Logger.get(TestWarpSpeedNative.class);

    private static final String CATALOG_NAME = "warp";
    private static final String SCHEMA_NAME = "warp_product_tests";

    @Inject
    WarpSpeedTests warpSpeedTests;

    @Inject
    RestUtils restUtils;

    private final String tableName = "nation_native";

    @BeforeMethodWithContext
    public void beforeMethod()
    {
        try {
            restUtils.executeWorkerRestCommand(
                    NativeStorageStateResource.PATH,
                    "",
                    new NativeStorageStateResource.NativeStorageState(false, false),
                    HttpMethod.POST,
                    HttpURLConnection.HTTP_NO_CONTENT);

            QueryExecutor queryExecutor = onTrino();
            queryExecutor.executeQuery(format("USE %s.%s", CATALOG_NAME, SCHEMA_NAME));
            queryExecutor.executeQuery("set session warp.enable_import_export = false");
            queryExecutor.executeQuery(format("CREATE TABLE IF NOT EXISTS %s.%s.%s AS SELECT * FROM tpch.tiny.nation", CATALOG_NAME, SCHEMA_NAME, tableName));
        }
        catch (Throwable e) {
            logger.error(e, "beforeMethod failed");
        }
    }

    @AfterMethodWithContext
    public void afterMethod()
    {
        try {
            restUtils.executeWorkerRestCommand(
                    NativeStorageStateResource.PATH,
                    "",
                    new NativeStorageStateResource.NativeStorageState(false, false),
                    HttpMethod.POST,
                    HttpURLConnection.HTTP_NO_CONTENT);

            String[] columnNames = new String[] {"name", "nationkey", "regionkey", "comment"};
            warpSpeedTests.testCleanup(SCHEMA_NAME, tableName, columnNames);
        }
        catch (Throwable e) {
            logger.error(e, "afterMethod failed");
        }
    }

    @Test(groups = {WARP_SPEED_HIVE_2, PROFILE_SPECIFIC_TESTS}, priority = 10)
    public void testWarpGenerateNativePanicStorageWrite(ITestContext iTestContext)
            throws IOException
    {
        restUtils.executeWorkerRestCommand(
                FailureGeneratorResource.TASK_NAME,
                "",
                List.of(new FailureGeneratorResource.FailureGeneratorData(
                        null,
                        "2388",
                        FailureRepetitionMode.REP_MODE_ONCE,
                        FailureGeneratorInvocationHandler.FailureType.NATIVE_PANIC,
                        0)),
                HttpMethod.POST,
                HttpURLConnection.HTTP_NO_CONTENT);
    }

    /* ToDo: temporarily commented out until fixing
    @Test(groups = {WARP_SPEED_HIVE_2, PROFILE_SPECIFIC_TESTS}, priority = 10)
    public void testWarpGenerateNativePanicStorageRead(ITestContext iTestContext)
            throws IOException
    {
        test(List.of(new FailureGeneratorResource.FailureGeneratorData(
                        null,
                        "2389",
                        FailureRepetitionMode.REP_MODE_ONCE,
                        FailureGeneratorInvocationHandler.FailureType.NATIVE_PANIC,
                        0)),
                iTestContext.getName());
    }

    @Test(groups = {WARP_SPEED_HIVE_2, PROFILE_SPECIFIC_TESTS}, priority = 10)
    public void testWarpGenerateNativePanicStorageWait(ITestContext iTestContext)
            throws IOException
    {
        test(List.of(new FailureGeneratorResource.FailureGeneratorData(
                        null,
                        "2390",
                        FailureRepetitionMode.REP_MODE_ONCE,
                        FailureGeneratorInvocationHandler.FailureType.NATIVE_PANIC,
                        0)),
                iTestContext.getName());
    }

    private void test(
            List<FailureGeneratorResource.FailureGeneratorData> failureGeneratorDataList,
            String testName)
            throws IOException
    {
        logger.info("test::before warmAndValidate");
        //check that it works before setting failures
        warmUtils.warmAndValidate(
                QUERY.formatted("name", tableName),
                Map.of(
                        WARM_ACCOMPLISHED, 1L,
                        ROW_GROUP_COUNT, 1L),
                Duration.valueOf("30s"));

        logger.info("test::before queryAndValidate");
        queryUtils.queryAndValidate(
                QUERY.formatted("name", tableName),
                Map.of(VARADA_COLLECT, 2L,
                        VARADA_MATCH, 1L,
                        EXTERNAL_COLLECT, 0L,
                        EXTERNAL_MATCH, 0L),
                testName);

        validateNativeState(false, false);

        //now test failure
        restUtils.executeWorkerRestCommand(
                FailureGeneratorResource.TASK_NAME,
                "",
                failureGeneratorDataList,
                HttpMethod.POST,
                HttpURLConnection.HTTP_NO_CONTENT);

        logger.info("test::before assertQueryFailure");
        //first query fails due to storage exception
        assertQueryFailure(() -> onTrino().executeQuery(QUERY.formatted("name", tableName)))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("native storage engine");

        logger.info("test::before proxy queryAndValidate");
        //this one is served from proxy
        queryUtils.queryAndValidate(
                QUERY.formatted("name", tableName),
                Map.of(VARADA_COLLECT, 0L,
                        VARADA_MATCH, 0L,
                        EXTERNAL_COLLECT, 1L,
                        EXTERNAL_MATCH, 1L),
                testName);

        validateNativeState(false, true);
    }

    private void validateNativeState(boolean storagePermanentException, boolean storageTemporaryException)
            throws IOException
    {
        logger.info("test::before validateNativeState storagePermanentException=%b, storageTemporaryException=%b",
                storagePermanentException, storageTemporaryException);
        String result = restUtils.executeWorkerRestCommand(
                NativeStorageStateResource.PATH,
                "",
                null,
                HttpMethod.GET,
                HttpURLConnection.HTTP_OK);
        NativeStorageStateResource.NativeStorageState state =
                objectMapper.readValue(
                        result,
                        NativeStorageStateResource.NativeStorageState.class);

        assertThat(state.storagePermanentException()).isEqualTo(storagePermanentException);
        assertThat(state.storageTemporaryException()).isEqualTo(storageTemporaryException);
    }
    */
}
