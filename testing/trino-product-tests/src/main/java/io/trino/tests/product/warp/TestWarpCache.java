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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.tempto.AfterMethodWithContext;
import io.trino.tempto.BeforeMethodWithContext;
import io.trino.tests.product.warp.utils.DemoterUtils;
import io.trino.tests.product.warp.utils.QueryUtils;
import io.trino.tests.product.warp.utils.RuleUtils;
import io.trino.tests.product.warp.utils.TableFormat;
import io.trino.tests.product.warp.utils.TestFormat;
import io.trino.tests.product.warp.utils.syntheticconfig.TestConfiguration;
import org.testng.ITestContext;
import org.testng.SkipException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URI;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.TestGroups.WARP_SPEED_CACHE;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static io.trino.tests.product.warp.utils.DemoterUtils.objectMapper;
import static java.lang.String.format;

public class TestWarpCache
{
    private static final Logger logger = Logger.get(TestSynthetic.class);
    private final String formattedDateTime;
    private boolean initialized;

    @Inject
    QueryUtils queryUtils;
    @Inject
    RuleUtils ruleUtils;
    @Inject
    DemoterUtils demoterUtils;

    public TestWarpCache()
    {
        LocalDateTime now = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyMMdd_HHmmss");
        formattedDateTime = now.format(formatter);
    }

    @BeforeMethodWithContext
    public void before()
            throws Exception
    {
        synchronized (this) {
            if (!initialized) {
                onTrino().executeQuery("CREATE SCHEMA IF NOT EXISTS warp.synthetic");
                onTrino().executeQuery("USE warp.synthetic");
                initialized = true;
            }
        }
        ruleUtils.resetAllRules();
    }

    @AfterMethodWithContext
    public void after()
    {
    }

    public static Iterator<Object[]> executeDataProvider(String filePath)
            throws Exception
    {
        logger.info("running %s", filePath);
        JsonNode jsonNodeTests = objectMapper.readTree(new URI(filePath).toURL());
        List<TestFormat> tests = objectMapper.readerFor(new TypeReference<List<TestFormat>>() {})
                .readValue(jsonNodeTests);
        List<TestFormat> testsToExecute = new ArrayList<>(tests);
        if (TestConfiguration.hasRunConfiguration()) {
            if (TestConfiguration.hasTestFilter()) {
                testsToExecute = TestConfiguration.filterTests(tests);
            }
            testsToExecute = TestConfiguration.updateTableType(testsToExecute);
            logger.info("Suite run on table type %s", TestConfiguration.getTableType().name());
        }
        return testsToExecute
                .stream()
                .filter(TestFormat::pt_enable)
                .map(x -> new Object[] {x})
                .iterator();
    }

    @DataProvider
    public Iterator<Object[]> cache(ITestContext context)
            throws Exception
    {
        return executeDataProvider("file:///docker/presto-product-tests/warp/cache.json");
    }

    @Test(groups = {WARP_SPEED_CACHE, PROFILE_SPECIFIC_TESTS}, dataProvider = "cache")
    public void cache(TestFormat testFormat)
            throws IOException
    {
        execute(testFormat, "synthetic", List.of(new TableFormat(testFormat.table_name(), testFormat.structure())), true);
    }

    @DataProvider
    public Iterator<Object[]> synthetic(ITestContext context)
            throws Exception
    {
        return executeDataProvider("file:///docker/presto-product-tests/warp/synthetic.json");
    }

    @Test(groups = {WARP_SPEED_CACHE, PROFILE_SPECIFIC_TESTS}, dataProvider = "synthetic")
    public void synthetic(TestFormat testFormat)
            throws IOException
    {
        execute(testFormat, "synthetic", List.of(new TableFormat(testFormat.name(), testFormat.structure())), false);
    }

    private void execute(TestFormat testFormat, String schemaName, List<TableFormat> usedTables, boolean assertOnCounters)
            throws IOException
    {
        if (testFormat.skip() || testFormat.skip_caching()) {
            logger.info("test %s is skipped. description=%s", testFormat.name(), testFormat.description());
            throw new SkipException("Skipping this test");
        }
        try {
            logger.info("starting run test %s", testFormat.name());
            onTrino().executeQuery("set session warp.enable_default_warming = false");
            onTrino().executeQuery(format("set session warp.import_export_s3_path = 's3://systemtest-export-import/test_export_import/pt/%s'", formattedDateTime));
            onTrino().executeQuery("set session warp.enable_import_export = true");
            onTrino().executeQuery(format("USE warp.%s", schemaName));
            queryUtils.runCacheQueries(testFormat, assertOnCounters);
            logger.info("successfully finish run test %s", testFormat.name());
        }
        catch (Exception e) {
            logger.error(e, "failed on test=%s", testFormat.name());
            throw e;
        }
        finally {
            for (TableFormat tableFormat : usedTables) {
                demoterUtils.demote(schemaName, tableFormat.tableName(), tableFormat);
            }
            demoterUtils.resetToDefaultDemoterConfiguration();
        }
    }
}
