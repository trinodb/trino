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
import com.google.common.collect.ImmutableSet;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.LegacyConfig;
import io.airlift.units.Duration;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.QualifiedName;
import jakarta.annotation.Nullable;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import org.joda.time.DateTime;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static io.trino.verifier.QueryType.CREATE;
import static io.trino.verifier.QueryType.MODIFY;
import static io.trino.verifier.QueryType.READ;

public class VerifierConfig
{
    private String testUsernameOverride;
    private String controlUsernameOverride;
    private String testPasswordOverride;
    private String controlPasswordOverride;
    private List<String> suites = ImmutableList.of();
    private Set<QueryType> controlQueryTypes = ImmutableSet.of(READ, CREATE, MODIFY);
    private Set<QueryType> testQueryTypes = ImmutableSet.of(READ, CREATE, MODIFY);
    private String source;
    private String runId = new DateTime().toString("yyyy-MM-dd");
    private Set<String> eventClients = ImmutableSet.of("human-readable");
    private int threadCount = 10;
    private String queryDatabase;
    private String controlGateway;
    private String testGateway;
    private Duration controlTimeout = new Duration(10, TimeUnit.MINUTES);
    private Duration testTimeout = new Duration(1, TimeUnit.HOURS);
    private Set<String> bannedQueries = ImmutableSet.of();
    private Set<String> allowedQueries = ImmutableSet.of();
    private int maxRowCount = 10_000;
    private int maxQueries = 1_000_000;
    private boolean alwaysReport;
    private String eventLogFile;
    private int suiteRepetitions = 1;
    private int queryRepetitions = 1;
    private String skipCorrectnessRegex = "^$";
    private boolean checkCorrectness = true;
    private boolean checkDeterminism = true;
    private String skipCpuCheckRegex = "(?i)(?s).*LIMIT.*";
    private boolean checkCpu = true;
    private boolean explainOnly;
    private boolean verboseResultsComparison;
    private String testCatalogOverride;
    private String testSchemaOverride;
    private String controlCatalogOverride;
    private String controlSchemaOverride;
    private boolean quiet;
    private String additionalJdbcDriverPath;
    private String testJdbcDriverName;
    private String controlJdbcDriverName;
    private int doublePrecision = 3;
    private int controlTeardownRetries = 1;
    private int testTeardownRetries = 1;
    private boolean shadowWrites = true;
    private String shadowTestTablePrefix = "tmp_verifier_";
    private String shadowControlTablePrefix = "tmp_verifier_";
    private boolean runTearDownOnResultMismatch;
    private boolean skipControl;
    private boolean simplifiedControlQueriesGenerationEnabled;
    private String simplifiedControlQueriesOutputDirectory = "/tmp/verifier/generated-control-queries";

    private Duration regressionMinCpuTime = new Duration(5, TimeUnit.MINUTES);

    @NotNull
    public String getSkipCorrectnessRegex()
    {
        return skipCorrectnessRegex;
    }

    @ConfigDescription("Correctness check will be skipped if this regex matches query")
    @Config("skip-correctness-regex")
    public VerifierConfig setSkipCorrectnessRegex(String skipCorrectnessRegex)
    {
        this.skipCorrectnessRegex = skipCorrectnessRegex;
        return this;
    }

    @NotNull
    public String getSkipCpuCheckRegex()
    {
        return skipCpuCheckRegex;
    }

    @ConfigDescription("CPU check will be skipped if this regex matches query")
    @Config("skip-cpu-check-regex")
    public VerifierConfig setSkipCpuCheckRegex(String skipCpuCheckRegex)
    {
        this.skipCpuCheckRegex = skipCpuCheckRegex;
        return this;
    }

    public boolean isVerboseResultsComparison()
    {
        return verboseResultsComparison;
    }

    @ConfigDescription("Display a diff of results that don't match")
    @Config("verbose-results-comparison")
    public VerifierConfig setVerboseResultsComparison(boolean verboseResultsComparison)
    {
        this.verboseResultsComparison = verboseResultsComparison;
        return this;
    }

    public boolean isQuiet()
    {
        return quiet;
    }

    @ConfigDescription("Reduces the number of informational messages printed")
    @Config("quiet")
    public VerifierConfig setQuiet(boolean quiet)
    {
        this.quiet = quiet;
        return this;
    }

    public int getQueryRepetitions()
    {
        return queryRepetitions;
    }

    @ConfigDescription("The number of times to repeat each query")
    @Config("query-repetitions")
    public VerifierConfig setQueryRepetitions(int queryRepetitions)
    {
        this.queryRepetitions = queryRepetitions;
        return this;
    }

    public String getSuite()
    {
        return suites.isEmpty() ? null : suites.get(0);
    }

    @ConfigDescription("The suites of queries in the query database to run")
    @Config("suite")
    public VerifierConfig setSuite(String suite)
    {
        if (suite == null) {
            return this;
        }
        suites = ImmutableList.of(suite);
        return this;
    }

    public Set<QueryType> getControlQueryTypes()
    {
        return controlQueryTypes;
    }

    @ConfigDescription("The types of control queries allowed to run [CREATE, READ, MODIFY]")
    @Config("control.query-types")
    public VerifierConfig setControlQueryTypes(Set<QueryType> controlQueryTypes)
    {
        this.controlQueryTypes = ImmutableSet.copyOf(controlQueryTypes);
        return this;
    }

    public Set<QueryType> getTestQueryTypes()
    {
        return testQueryTypes;
    }

    @ConfigDescription("The types of control queries allowed to run [CREATE, READ, MODIFY]")
    @Config("test.query-types")
    public VerifierConfig setTestQueryTypes(Set<QueryType> testQueryTypes)
    {
        this.testQueryTypes = ImmutableSet.copyOf(testQueryTypes);
        return this;
    }

    @NotNull
    public List<String> getSuites()
    {
        return suites;
    }

    @ConfigDescription("The suites of queries in the query database to run")
    @Config("suites")
    public VerifierConfig setSuites(List<String> suites)
    {
        this.suites = ImmutableList.copyOf(suites);
        return this;
    }

    @Min(1)
    public int getThreadCount()
    {
        return threadCount;
    }

    @ConfigDescription("The concurrency level")
    @Config("thread-count")
    public VerifierConfig setThreadCount(int threadCount)
    {
        this.threadCount = threadCount;
        return this;
    }

    @NotNull
    public String getQueryDatabase()
    {
        return queryDatabase;
    }

    @ConfigDescription("Database to fetch query suites from")
    @Config("query-database")
    public VerifierConfig setQueryDatabase(String queryDatabase)
    {
        this.queryDatabase = queryDatabase;
        return this;
    }

    @NotNull
    public Set<String> getBannedQueries()
    {
        return bannedQueries;
    }

    @ConfigDescription("Names of queries which are banned")
    @Config("banned-queries")
    @LegacyConfig("blacklist")
    public VerifierConfig setBannedQueries(Set<String> bannedQueries)
    {
        this.bannedQueries = ImmutableSet.copyOf(bannedQueries);
        return this;
    }

    @NotNull
    public Set<String> getAllowedQueries()
    {
        return allowedQueries;
    }

    @ConfigDescription("Names of queries which are allowed. If non-empty, only allowed queries are used.")
    @Config("allowed-queries")
    @LegacyConfig("whitelist")
    public VerifierConfig setAllowedQueries(Set<String> allowedQueries)
    {
        this.allowedQueries = ImmutableSet.copyOf(allowedQueries);
        return this;
    }

    @Min(1)
    public int getMaxRowCount()
    {
        return maxRowCount;
    }

    @ConfigDescription("The maximum number of rows a query may return. If it exceeds this limit it's marked as failed")
    @Config("max-row-count")
    public VerifierConfig setMaxRowCount(int maxRowCount)
    {
        this.maxRowCount = maxRowCount;
        return this;
    }

    public boolean isAlwaysReport()
    {
        return alwaysReport;
    }

    @ConfigDescription("Print more informational messages")
    @Config("always-report")
    public VerifierConfig setAlwaysReport(boolean alwaysReport)
    {
        this.alwaysReport = alwaysReport;
        return this;
    }

    public int getMaxQueries()
    {
        return maxQueries;
    }

    @ConfigDescription("The maximum number of queries to run for each suite")
    @Config("max-queries")
    public VerifierConfig setMaxQueries(int maxQueries)
    {
        this.maxQueries = maxQueries;
        return this;
    }

    public boolean isCheckCorrectnessEnabled()
    {
        return checkCorrectness;
    }

    @ConfigDescription("Whether to check that the rows from control and test match")
    @Config("check-correctness")
    public VerifierConfig setCheckCorrectnessEnabled(boolean checkCorrectness)
    {
        this.checkCorrectness = checkCorrectness;
        return this;
    }

    public boolean isCheckDeterminismEnabled()
    {
        return checkDeterminism;
    }

    @Config("check-determinism")
    public VerifierConfig setCheckDeterminismEnabled(boolean checkDeterminism)
    {
        this.checkDeterminism = checkDeterminism;
        return this;
    }

    public boolean isCheckCpuEnabled()
    {
        return checkCpu;
    }

    @ConfigDescription("Whether to check that CPU from control and test match")
    @Config("check-cpu")
    public VerifierConfig setCheckCpuEnabled(boolean checkCpu)
    {
        this.checkCpu = checkCpu;
        return this;
    }

    public boolean isExplainOnly()
    {
        return explainOnly;
    }

    @ConfigDescription("Only attempt to explain queries but do not execute them")
    @Config("explain-only")
    public VerifierConfig setExplainOnly(boolean explainOnly)
    {
        this.explainOnly = explainOnly;
        return this;
    }

    public int getSuiteRepetitions()
    {
        return suiteRepetitions;
    }

    @ConfigDescription("Number of times to run each suite")
    @Config("suite-repetitions")
    public VerifierConfig setSuiteRepetitions(int suiteRepetitions)
    {
        this.suiteRepetitions = suiteRepetitions;
        return this;
    }

    @NotNull
    public Set<String> getEventClients()
    {
        return eventClients;
    }

    @ConfigDescription("The event client(s) to log the results to")
    @Config("event-client")
    public VerifierConfig setEventClients(Set<String> eventClients)
    {
        this.eventClients = ImmutableSet.copyOf(eventClients);
        return this;
    }

    public String getSource()
    {
        return source;
    }

    @ConfigDescription("The source to pass to Trino")
    @Config("source")
    public VerifierConfig setSource(String source)
    {
        this.source = source;
        return this;
    }

    public String getRunId()
    {
        return runId;
    }

    @ConfigDescription("A customizable string that will be logged with the results")
    @Config("run-id")
    public VerifierConfig setRunId(String runId)
    {
        this.runId = runId;
        return this;
    }

    public String getEventLogFile()
    {
        return eventLogFile;
    }

    @ConfigDescription("The file to log events to. Used with event-client=file")
    @Config("event-log-file")
    public VerifierConfig setEventLogFile(String eventLogFile)
    {
        this.eventLogFile = eventLogFile;
        return this;
    }

    public String getTestCatalogOverride()
    {
        return testCatalogOverride;
    }

    @ConfigDescription("Overrides the test_catalog field in all queries in the suites")
    @Config("test.catalog-override")
    public VerifierConfig setTestCatalogOverride(String testCatalogOverride)
    {
        this.testCatalogOverride = testCatalogOverride;
        return this;
    }

    public String getTestSchemaOverride()
    {
        return testSchemaOverride;
    }

    @ConfigDescription("Overrides the test_schema field in all queries in the suites")
    @Config("test.schema-override")
    public VerifierConfig setTestSchemaOverride(String testSchemaOverride)
    {
        this.testSchemaOverride = testSchemaOverride;
        return this;
    }

    public Duration getTestTimeout()
    {
        return testTimeout;
    }

    @ConfigDescription("Timeout for queries to the test cluster")
    @Config("test.timeout")
    public VerifierConfig setTestTimeout(Duration testTimeout)
    {
        this.testTimeout = testTimeout;
        return this;
    }

    @Nullable
    public String getTestUsernameOverride()
    {
        return testUsernameOverride;
    }

    @ConfigDescription("Username for test cluster")
    @Config("test.username-override")
    @LegacyConfig("test.username")
    public VerifierConfig setTestUsernameOverride(String testUsernameOverride)
    {
        this.testUsernameOverride = testUsernameOverride;
        return this;
    }

    @Nullable
    public String getTestPasswordOverride()
    {
        return testPasswordOverride;
    }

    @ConfigDescription("Password for test cluster")
    @Config("test.password-override")
    @LegacyConfig("test.password")
    public VerifierConfig setTestPasswordOverride(String testPasswordOverride)
    {
        this.testPasswordOverride = testPasswordOverride;
        return this;
    }

    @NotNull
    public String getTestGateway()
    {
        return testGateway;
    }

    @ConfigDescription("URL for test cluster")
    @Config("test.gateway")
    public VerifierConfig setTestGateway(String testGateway)
    {
        this.testGateway = testGateway;
        return this;
    }

    public String getControlCatalogOverride()
    {
        return controlCatalogOverride;
    }

    @ConfigDescription("Overrides the control_catalog field in all queries in the suites")
    @Config("control.catalog-override")
    public VerifierConfig setControlCatalogOverride(String controlCatalogOverride)
    {
        this.controlCatalogOverride = controlCatalogOverride;
        return this;
    }

    public String getControlSchemaOverride()
    {
        return controlSchemaOverride;
    }

    @ConfigDescription("Overrides the control_schema field in all queries in the suites")
    @Config("control.schema-override")
    public VerifierConfig setControlSchemaOverride(String controlSchemaOverride)
    {
        this.controlSchemaOverride = controlSchemaOverride;
        return this;
    }

    public Duration getControlTimeout()
    {
        return controlTimeout;
    }

    @ConfigDescription("Timeout for queries to the control cluster")
    @Config("control.timeout")
    public VerifierConfig setControlTimeout(Duration controlTimeout)
    {
        this.controlTimeout = controlTimeout;
        return this;
    }

    @Nullable
    public String getControlUsernameOverride()
    {
        return controlUsernameOverride;
    }

    @ConfigDescription("Username for control cluster")
    @Config("control.username-override")
    @LegacyConfig("control.username")
    public VerifierConfig setControlUsernameOverride(String controlUsernameOverride)
    {
        this.controlUsernameOverride = controlUsernameOverride;
        return this;
    }

    @Nullable
    public String getControlPasswordOverride()
    {
        return controlPasswordOverride;
    }

    @ConfigDescription("Password for control cluster")
    @Config("control.password-override")
    @LegacyConfig("control.password")
    public VerifierConfig setControlPasswordOverride(String controlPasswordOverride)
    {
        this.controlPasswordOverride = controlPasswordOverride;
        return this;
    }

    @NotNull
    public String getControlGateway()
    {
        return controlGateway;
    }

    @ConfigDescription("URL for control cluster")
    @Config("control.gateway")
    public VerifierConfig setControlGateway(String controlGateway)
    {
        this.controlGateway = controlGateway;
        return this;
    }

    @Nullable
    public String getAdditionalJdbcDriverPath()
    {
        return additionalJdbcDriverPath;
    }

    @ConfigDescription("Path for test jdbc driver")
    @Config("additional-jdbc-driver-path")
    public VerifierConfig setAdditionalJdbcDriverPath(String path)
    {
        this.additionalJdbcDriverPath = path;
        return this;
    }

    @Nullable
    public String getTestJdbcDriverName()
    {
        return testJdbcDriverName;
    }

    @ConfigDescription("Fully qualified test JDBC driver name")
    @Config("test.jdbc-driver-class")
    public VerifierConfig setTestJdbcDriverName(String testJdbcDriverName)
    {
        this.testJdbcDriverName = testJdbcDriverName;
        return this;
    }

    @Nullable
    public String getControlJdbcDriverName()
    {
        return controlJdbcDriverName;
    }

    @ConfigDescription("Fully qualified control JDBC driver name")
    @Config("control.jdbc-driver-class")
    public VerifierConfig setControlJdbcDriverName(String controlJdbcDriverName)
    {
        this.controlJdbcDriverName = controlJdbcDriverName;
        return this;
    }

    public int getDoublePrecision()
    {
        return doublePrecision;
    }

    @ConfigDescription("The expected precision when comparing test and control results")
    @Config("expected-double-precision")
    public VerifierConfig setDoublePrecision(int doublePrecision)
    {
        this.doublePrecision = doublePrecision;
        return this;
    }

    @NotNull
    public Duration getRegressionMinCpuTime()
    {
        return regressionMinCpuTime;
    }

    @ConfigDescription("Minimum cpu time a query must use in the control to be considered for regression")
    @Config("regression.min-cpu-time")
    public VerifierConfig setRegressionMinCpuTime(Duration regressionMinCpuTime)
    {
        this.regressionMinCpuTime = regressionMinCpuTime;
        return this;
    }

    @Min(0)
    public int getControlTeardownRetries()
    {
        return controlTeardownRetries;
    }

    @ConfigDescription("Number of retries for control teardown queries")
    @Config("control.teardown-retries")
    public VerifierConfig setControlTeardownRetries(int controlTeardownRetries)
    {
        this.controlTeardownRetries = controlTeardownRetries;
        return this;
    }

    @Min(0)
    public int getTestTeardownRetries()
    {
        return testTeardownRetries;
    }

    @ConfigDescription("Number of retries for test teardown queries")
    @Config("test.teardown-retries")
    public VerifierConfig setTestTeardownRetries(int testTeardownRetries)
    {
        this.testTeardownRetries = testTeardownRetries;
        return this;
    }

    public boolean getShadowWrites()
    {
        return shadowWrites;
    }

    @ConfigDescription("Modify write queries to write to a temporary table instead")
    @Config("shadow-writes.enabled")
    public VerifierConfig setShadowWrites(boolean shadowWrites)
    {
        this.shadowWrites = shadowWrites;
        return this;
    }

    public QualifiedName getShadowTestTablePrefix()
    {
        return QualifiedName.of(Arrays.stream(shadowTestTablePrefix.split("\\."))
                .map(Identifier::new)
                .collect(Collectors.toList()));
    }

    @ConfigDescription("The prefix to use for temporary test shadow tables. May be fully qualified like 'tmp_catalog.tmp_schema.tmp_'")
    @Config("shadow-writes.test-table-prefix")
    public VerifierConfig setShadowTestTablePrefix(String prefix)
    {
        this.shadowTestTablePrefix = prefix;
        return this;
    }

    public QualifiedName getShadowControlTablePrefix()
    {
        return QualifiedName.of(Arrays.stream(shadowControlTablePrefix.split("\\."))
                .map(Identifier::new)
                .collect(Collectors.toList()));
    }

    @ConfigDescription("The prefix to use for temporary control shadow tables. May be fully qualified like 'tmp_catalog.tmp_schema.tmp_'")
    @Config("shadow-writes.control-table-prefix")
    public VerifierConfig setShadowControlTablePrefix(String prefix)
    {
        this.shadowControlTablePrefix = prefix;
        return this;
    }

    public boolean getRunTearDownOnResultMismatch()
    {
        return runTearDownOnResultMismatch;
    }

    @ConfigDescription("If set to false, temporary tables will not be dropped after checksum failure")
    @Config("run-teardown-on-result-mismatch")
    public VerifierConfig setRunTearDownOnResultMismatch(boolean runTearDownOnResultMismatch)
    {
        this.runTearDownOnResultMismatch = runTearDownOnResultMismatch;
        return this;
    }

    public boolean isSkipControl()
    {
        return skipControl;
    }

    @Config("skip-control")
    public VerifierConfig setSkipControl(boolean skipControl)
    {
        this.skipControl = skipControl;
        return this;
    }

    public boolean isSimplifiedControlQueriesGenerationEnabled()
    {
        return simplifiedControlQueriesGenerationEnabled;
    }

    @Config("simplified-control-queries-generation-enabled")
    public VerifierConfig setSimplifiedControlQueriesGenerationEnabled(boolean simplifiedControlQueriesGenerationEnabled)
    {
        this.simplifiedControlQueriesGenerationEnabled = simplifiedControlQueriesGenerationEnabled;
        return this;
    }

    @NotNull
    public String getSimplifiedControlQueriesOutputDirectory()
    {
        return simplifiedControlQueriesOutputDirectory;
    }

    @Config("simplified-control-queries-output-directory")
    public VerifierConfig setSimplifiedControlQueriesOutputDirectory(String simplifiedControlQueriesOutputDirectory)
    {
        this.simplifiedControlQueriesOutputDirectory = simplifiedControlQueriesOutputDirectory;
        return this;
    }
}
