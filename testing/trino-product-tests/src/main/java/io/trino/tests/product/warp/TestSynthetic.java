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
import io.trino.tests.product.warp.utils.FastWarming;
import io.trino.tests.product.warp.utils.QueryUtils;
import io.trino.tests.product.warp.utils.RuleUtils;
import io.trino.tests.product.warp.utils.TestFormat;
import io.trino.tests.product.warp.utils.WarmUtils;
import io.trino.tests.product.warp.utils.syntheticconfig.TestConfiguration;
import org.testng.ITestContext;
import org.testng.SkipException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URI;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.TestGroups.WARP_SPEED;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static io.trino.tests.product.warp.utils.DemoterUtils.objectMapper;
import static java.lang.String.format;

public class TestSynthetic
{
    private static final Logger logger = Logger.get(TestSynthetic.class);
    private final String formattedDateTime;
    private boolean initialized;

    @Inject
    WarmUtils warmUtils;
    @Inject
    QueryUtils queryUtils;
    @Inject
    RuleUtils ruleUtils;
    @Inject
    DemoterUtils demoterUtils;

    public TestSynthetic()
    {
        LocalDateTime now = LocalDateTime.now(ZoneId.systemDefault());
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyMMdd_HHmmss");
        formattedDateTime = now.format(formatter);
    }

    @BeforeMethodWithContext
    public void before()
            throws Exception
    {
        synchronized (this) {
            if (!initialized) {
//                onTrino().executeQuery("USE warp.default");
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

    @DataProvider
    public Iterator<TestFormat> synthetic(ITestContext context)
            throws Exception
    {
        return executeDataProvider("file:///docker/presto-product-tests/warp/synthetic.json");
    }

    @Test(groups = {WARP_SPEED, PROFILE_SPECIFIC_TESTS}, dataProvider = "synthetic")
    public void synthetic(TestFormat testFormat)
            throws IOException
    {
        execute(testFormat);
    }

    public static Iterator<TestFormat> executeDataProvider(String filePath)
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
                .iterator();
    }

    private void execute(TestFormat testFormat)
            throws IOException
    {
        if (testFormat.skip()) {
            logger.info("test %s is skipped. description=%s", testFormat.name(), testFormat.description());
            throw new SkipException("Skipping this test");
        }
        String schemaName = "synthetic";
        String tableName = testFormat.getTableName();
        try {
            logger.info("starting run test %s", testFormat.name());
            onTrino().executeQuery(format("set session warp.import_export_s3_path = 's3://systemtest-export-import/test_export_import/pt/%s'", formattedDateTime));
            onTrino().executeQuery("USE warp.synthetic");
            Map<String, Object> sessionPropertiesWithCatalog = testFormat.session_properties() != null ?
                    testFormat.session_properties().entrySet().stream().collect(Collectors.toMap(e -> "warp." + e.getKey(), Map.Entry::getValue)) :
                    new HashMap<>();
            boolean defaultWarming = Boolean.parseBoolean(
                    sessionPropertiesWithCatalog.getOrDefault("warp.enable_default_warming", "false").toString());
            warmUtils.createTable(testFormat);
            if (!defaultWarming) {
                ruleUtils.createWarmupRules(schemaName, testFormat);
            }
            warmUtils.setSessions(sessionPropertiesWithCatalog);

            boolean fastWarming = Boolean.parseBoolean(
                    sessionPropertiesWithCatalog.getOrDefault("warp.enable_import_export", "false").toString());
            if (fastWarming) {
                warmUtils.warmAndValidate(testFormat, FastWarming.EXPORT);
                demoterUtils.demote(schemaName, tableName, testFormat);
                demoterUtils.resetToDefaultDemoterConfiguration();
                warmUtils.warmAndValidate(testFormat, FastWarming.IMPORT);
            }
            else {
                warmUtils.warmAndValidate(testFormat, FastWarming.NONE);
            }
            warmUtils.resetSessions(sessionPropertiesWithCatalog);

            queryUtils.runQueries(testFormat);
            logger.info("successfully finish run test %s", testFormat.name());
        }
        catch (Exception e) {
            logger.error(e, "failed on test=%s", testFormat.name());
            throw new RuntimeException(e);
        }
        finally {
            ruleUtils.resetTableRules(schemaName, testFormat);
            demoterUtils.demote(schemaName, tableName, testFormat);
            demoterUtils.resetToDefaultDemoterConfiguration();
        }
    }
}
