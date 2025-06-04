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
package io.trino.verifier;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.Duration;
import org.joda.time.DateTime;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.trino.verifier.QueryType.CREATE;
import static io.trino.verifier.QueryType.MODIFY;
import static io.trino.verifier.QueryType.READ;

public class TestVerifierConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(VerifierConfig.class)
                .setTestUsernameOverride(null)
                .setControlUsernameOverride(null)
                .setTestPasswordOverride(null)
                .setControlPasswordOverride(null)
                .setSuite(null)
                .setSuites(ImmutableList.of())
                .setControlQueryTypes(ImmutableSet.of(READ, CREATE, MODIFY))
                .setTestQueryTypes(ImmutableSet.of(READ, CREATE, MODIFY))
                .setSource(null)
                .setRunId(new DateTime().toString("yyyy-MM-dd"))
                .setEventClients(ImmutableSet.of("human-readable"))
                .setThreadCount(10)
                .setQueryDatabase(null)
                .setControlGateway(null)
                .setTestGateway(null)
                .setControlTimeout(new Duration(10, TimeUnit.MINUTES))
                .setTestTimeout(new Duration(1, TimeUnit.HOURS))
                .setBannedQueries(ImmutableSet.of())
                .setAllowedQueries(ImmutableSet.of())
                .setMaxRowCount(10_000)
                .setMaxQueries(1_000_000)
                .setAlwaysReport(false)
                .setSuiteRepetitions(1)
                .setCheckCorrectnessEnabled(true)
                .setCheckDeterminismEnabled(true)
                .setCheckCpuEnabled(true)
                .setExplainOnly(false)
                .setSkipCorrectnessRegex("^$")
                .setSkipCpuCheckRegex("(?i)(?s).*LIMIT.*")
                .setQueryRepetitions(1)
                .setTestCatalogOverride(null)
                .setTestSchemaOverride(null)
                .setControlCatalogOverride(null)
                .setControlSchemaOverride(null)
                .setQuiet(false)
                .setVerboseResultsComparison(false)
                .setEventLogFile(null)
                .setAdditionalJdbcDriverPath(null)
                .setTestJdbcDriverName(null)
                .setControlJdbcDriverName(null)
                .setDoublePrecision(3)
                .setRegressionMinCpuTime(new Duration(5, TimeUnit.MINUTES))
                .setShadowWrites(true)
                .setShadowTestTablePrefix("tmp_verifier_")
                .setShadowControlTablePrefix("tmp_verifier_")
                .setControlTeardownRetries(1)
                .setTestTeardownRetries(1)
                .setRunTearDownOnResultMismatch(false)
                .setSkipControl(false)
                .setSimplifiedControlQueriesGenerationEnabled(false)
                .setSimplifiedControlQueriesOutputDirectory("/tmp/verifier/generated-control-queries"));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("suites", "my_suite")
                .put("suite", "my_suite")
                .put("control.query-types", "create,modify")
                .put("test.query-types", "modify")
                .put("source", "my_source")
                .put("run-id", "my_run_id")
                .put("event-client", "file,human-readable")
                .put("thread-count", "1")
                .put("banned-queries", "1,2")
                .put("allowed-queries", "3,4")
                .put("verbose-results-comparison", "true")
                .put("max-row-count", "1")
                .put("max-queries", "1")
                .put("always-report", "true")
                .put("suite-repetitions", "2")
                .put("query-repetitions", "2")
                .put("check-correctness", "false")
                .put("check-determinism", "false")
                .put("check-cpu", "false")
                .put("explain-only", "true")
                .put("skip-correctness-regex", "limit")
                .put("skip-cpu-check-regex", "LIMIT")
                .put("quiet", "true")
                .put("event-log-file", "./test")
                .put("query-database", "jdbc:mysql://localhost:3306/my_database?user=my_username&password=my_password")
                .put("test.username-override", "test_user")
                .put("test.password-override", "test_password")
                .put("test.gateway", "jdbc:trino://localhost:8080")
                .put("test.timeout", "1s")
                .put("test.catalog-override", "my_catalog")
                .put("test.schema-override", "my_schema")
                .put("control.username-override", "control_user")
                .put("control.password-override", "control_password")
                .put("control.gateway", "jdbc:trino://localhost:8081")
                .put("control.timeout", "1s")
                .put("control.catalog-override", "my_catalog")
                .put("control.schema-override", "my_schema")
                .put("additional-jdbc-driver-path", "/test/path")
                .put("test.jdbc-driver-class", "com.example.Driver")
                .put("control.jdbc-driver-class", "com.example.Driver")
                .put("expected-double-precision", "5")
                .put("regression.min-cpu-time", "30s")
                .put("shadow-writes.enabled", "false")
                .put("shadow-writes.test-table-prefix", "tmp_")
                .put("shadow-writes.control-table-prefix", "schema.tmp_")
                .put("control.teardown-retries", "5")
                .put("test.teardown-retries", "7")
                .put("run-teardown-on-result-mismatch", "true")
                .put("skip-control", "true")
                .put("simplified-control-queries-generation-enabled", "true")
                .put("simplified-control-queries-output-directory", "/tmp/some/directory")
                .buildOrThrow();

        VerifierConfig expected = new VerifierConfig().setTestUsernameOverride("verifier-test")
                .setSuites(ImmutableList.of("my_suite"))
                .setSuite("my_suite")
                .setControlQueryTypes(ImmutableSet.of(CREATE, MODIFY))
                .setTestQueryTypes(ImmutableSet.of(MODIFY))
                .setSource("my_source")
                .setRunId("my_run_id")
                .setEventClients(ImmutableSet.of("file", "human-readable"))
                .setThreadCount(1)
                .setBannedQueries(ImmutableSet.of("1", "2"))
                .setAllowedQueries(ImmutableSet.of("3", "4"))
                .setMaxRowCount(1)
                .setMaxQueries(1)
                .setAlwaysReport(true)
                .setVerboseResultsComparison(true)
                .setSuiteRepetitions(2)
                .setQueryRepetitions(2)
                .setCheckCorrectnessEnabled(false)
                .setCheckDeterminismEnabled(false)
                .setCheckCpuEnabled(false)
                .setExplainOnly(true)
                .setSkipCorrectnessRegex("limit")
                .setSkipCpuCheckRegex("LIMIT")
                .setQuiet(true)
                .setEventLogFile("./test")
                .setQueryDatabase("jdbc:mysql://localhost:3306/my_database?user=my_username&password=my_password")
                .setTestUsernameOverride("test_user")
                .setTestPasswordOverride("test_password")
                .setTestGateway("jdbc:trino://localhost:8080")
                .setTestTimeout(new Duration(1, TimeUnit.SECONDS))
                .setTestCatalogOverride("my_catalog")
                .setTestSchemaOverride("my_schema")
                .setControlUsernameOverride("control_user")
                .setControlPasswordOverride("control_password")
                .setControlGateway("jdbc:trino://localhost:8081")
                .setControlTimeout(new Duration(1, TimeUnit.SECONDS))
                .setControlCatalogOverride("my_catalog")
                .setControlSchemaOverride("my_schema")
                .setAdditionalJdbcDriverPath("/test/path")
                .setTestJdbcDriverName("com.example.Driver")
                .setControlJdbcDriverName("com.example.Driver")
                .setDoublePrecision(5)
                .setRegressionMinCpuTime(new Duration(30, TimeUnit.SECONDS))
                .setShadowWrites(false)
                .setShadowTestTablePrefix("tmp_")
                .setShadowControlTablePrefix("schema.tmp_")
                .setControlTeardownRetries(5)
                .setTestTeardownRetries(7)
                .setRunTearDownOnResultMismatch(true)
                .setSkipControl(true)
                .setSimplifiedControlQueriesGenerationEnabled(true)
                .setSimplifiedControlQueriesOutputDirectory("/tmp/some/directory");

        assertFullMapping(properties, expected);
    }
}
