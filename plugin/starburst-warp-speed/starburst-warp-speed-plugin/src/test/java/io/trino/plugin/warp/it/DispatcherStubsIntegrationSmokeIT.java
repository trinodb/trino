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
package io.trino.plugin.warp.it;

import com.fasterxml.jackson.core.type.TypeReference;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.execution.QueryInfo;
import io.trino.operator.OperatorStats;
import io.trino.plugin.base.metrics.LongCount;
import io.trino.plugin.varada.VaradaSessionProperties;
import io.trino.plugin.varada.api.warmup.WarmUpType;
import io.trino.plugin.varada.api.warmup.WarmupPropertiesData;
import io.trino.plugin.varada.dispatcher.DispatcherPageSourceFactory;
import io.trino.plugin.varada.dispatcher.warmup.WorkerWarmingService;
import io.trino.plugin.varada.storage.engine.StubsStorageEngine;
import io.trino.plugin.varada.warmup.WarmupRuleService;
import io.trino.plugin.warp.di.VaradaStubsStorageEngineModule;
import io.trino.plugin.warp.extension.execution.debugtools.RowGroupTask;
import io.trino.plugin.warp.extension.execution.debugtools.WarmupDemoterData;
import io.trino.plugin.warp.extension.execution.debugtools.WarmupDemoterTask;
import io.trino.plugin.warp.extension.execution.debugtools.WorkerWarmupDemoterTask;
import io.trino.plugin.warp.extension.execution.debugtools.dictionary.DictionaryTask;
import io.trino.plugin.warp.extension.execution.warmup.WarmupTask;
import io.trino.plugin.warp.gen.stats.VaradaStatsWarmingService;
import io.trino.spi.QueryId;
import io.trino.spi.metrics.Count;
import io.trino.spi.metrics.Metrics;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import jakarta.ws.rs.HttpMethod;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static java.util.Map.entry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

// TODO - there seems to be a problem with running warmup with multiple workers on IT (we get an NPE)
public abstract class DispatcherStubsIntegrationSmokeIT
        extends DispatcherAbstractTestQueryFramework
{
    protected static final String VARADA_MATCH_COLUMNS_STAT = "varada_match_columns";
    protected static final String VARADA_COLLECT_COLUMNS_STAT = "varada_collect_columns";
    protected static final String PREFILLED_COLUMNS_STAT = "prefilled_collect_columns";
    protected static final String EXTERNAL_MATCH_STAT = "external_match_columns";
    protected static final String EXTERNAL_COLLECT_STAT = "external_collect_columns";
    protected static final String CACHED_TOTAL_ROWS = "cached_total_rows";
    protected static final String C1 = "int1";
    protected static final String C2 = "v1";
    protected static final double DEMOTE_CLEAN_UP_USAGE = 0;
    protected static final double DEMOTE_MAX_USAGE_THRESHOLD_DEFAULT = 90;
    protected static final double DEMOTE_CLEAN_USAGE_THRESHOLD_DEFAULT = 80;
    protected static final int DEMOTE_DEFAULT_BATCH_SIZE = 1;
    protected static final int DEMOTE_DEFAULT_EPSILON = 1;
    protected static final int DEMOTE_MAX_ELEMENTS_TO_DEMOTE = 1;
    private static final Logger logger = Logger.get(DispatcherStubsIntegrationSmokeIT.class);
    public static final List<String> DEMOTE_JMX_NAMES = List.of("number_of_runs", "number_of_runs_fail", "not_executed_due_threshold", "not_executed_due_is_already_executing");
    protected final int numNodes;
    protected final StubsStorageEngine stubsStorageEngine;
    protected final VaradaStubsStorageEngineModule storageEngineModule;
    protected final String catalog;
    protected final boolean isWarpExtensionModule;

    public DispatcherStubsIntegrationSmokeIT(int numNodes, String catalog)
    {
        this(numNodes, catalog, true);
    }

    public DispatcherStubsIntegrationSmokeIT(int numNodes, String catalog, boolean isWarpExtensionModule)
    {
        this.numNodes = numNodes;
        this.catalog = catalog;
        this.isWarpExtensionModule = isWarpExtensionModule;
        storageEngineModule = new VaradaStubsStorageEngineModule();
        stubsStorageEngine = (StubsStorageEngine) storageEngineModule.getStorageEngine();
    }

    @BeforeEach
    @Override
    public void beforeMethod(TestInfo testInfo)
    {
        super.beforeMethod(testInfo);
        createSchemaAndTable(DEFAULT_SCHEMA, "t", format("(%s integer, %s varchar(20))", C1, C2));
    }

    @AfterEach
    @Override
    public void afterMethod(TestInfo testInfo)
    {
        runWithRetries(() -> {
            try {
                if (isWarpExtensionModule) {
                    demoteAll();
                    executeRestCommand(RowGroupTask.ROW_GROUP_PATH, RowGroupTask.ROW_GROUP_RESET_TASK_NAME, null, HttpMethod.POST, HttpURLConnection.HTTP_NO_CONTENT);
                    executeRestCommand(WarmupRuleService.WARMUP_PATH, WarmupTask.TASK_NAME_REPLACE, List.of(), HttpMethod.POST, HttpURLConnection.HTTP_OK);
                    cleanWarmupRules();
                }
                MaterializedResult materializedRows = computeActual(format("show tables from %s", DEFAULT_SCHEMA));
                for (MaterializedRow materializedRow : materializedRows.getMaterializedRows()) {
                    logger.info("deleting table %s", materializedRow.getField(0));
                    computeActual("DROP TABLE IF EXISTS " + materializedRow.getField(0));
                }
                computeActual(format("DROP SCHEMA IF EXISTS %s", DEFAULT_SCHEMA));

                stubsStorageEngine.clear();
            }
            catch (Throwable e) {
                fail(e.getMessage());
            }
        });
        super.afterMethod(testInfo);
    }

    protected void demoteAll()
    {
        try {
            logger.info("demote all start");
            WarmupDemoterData warmupDemoterData = WarmupDemoterData.builder().maxUsageThresholdInPercentage(DEMOTE_CLEAN_UP_USAGE)
                    .cleanupUsageThresholdInPercentage(DEMOTE_CLEAN_UP_USAGE)
                    .executeDemoter(true)
                    .modifyConfiguration(true)
                    .resetHighestPriority(true)
                    .forceDeleteFailedObjects(true)
                    .build();
            String result = executeRestCommand(WarmupDemoterTask.WARMUP_DEMOTER_PATH,
                    WarmupDemoterTask.WARMUP_DEMOTER_START_TASK_NAME,
                    warmupDemoterData,
                    HttpMethod.POST,
                    HttpURLConnection.HTTP_OK);
            Map<String, Object> res = objectMapper.readerFor(new TypeReference<Map<String, Object>>() {}).readValue(result);
            Double highestPriority = (Double) res.entrySet()
                    .stream()
                    .filter((entry) -> entry.getKey().endsWith(WorkerWarmupDemoterTask.HIGHEST_PRIORITY_KEY))
                    .findAny()
                    .orElseThrow()
                    .getValue();
            logger.info("demote task finish =" + res);
            assertThat(highestPriority).isEqualTo(0);
            restDemoteConfigurationToDefaults();
//            validateEmptyUsage();
            String dictionariesReset = executeRestCommand(DictionaryTask.DICTIONARY_PATH,
                    DictionaryTask.DICTIONARY_RESET_MEMORY_TASK_NAME,
                    null,
                    HttpMethod.POST,
                    HttpURLConnection.HTTP_OK);
            logger.info("reset %s existing dictionaries after demoting all", dictionariesReset);
            logger.info("demote all finish");
        }
        catch (IOException e) {
            fail("demote failed", e);
        }
    }

    protected void restDemoteConfigurationToDefaults()
    {
        try {
            WarmupDemoterData warmupDemoterData = WarmupDemoterData.builder()
                    .maxUsageThresholdInPercentage(DEMOTE_MAX_USAGE_THRESHOLD_DEFAULT)
                    .cleanupUsageThresholdInPercentage(DEMOTE_CLEAN_USAGE_THRESHOLD_DEFAULT)
                    .batchSize(DEMOTE_DEFAULT_BATCH_SIZE)
                    .epsilon(DEMOTE_DEFAULT_EPSILON)
                    .maxElementsToDemoteInIteration(DEMOTE_MAX_ELEMENTS_TO_DEMOTE)
                    .forceExecuteDeadObjects(false)
                    .modifyConfiguration(true)
                    .resetHighestPriority(true)
                    .executeDemoter(false)
                    .build();
            Map<String, Object> res = objectMapper.readerFor(new TypeReference<Map<String, Object>>() {})
                    .readValue(executeRestCommand(WarmupDemoterTask.WARMUP_DEMOTER_PATH,
                            WarmupDemoterTask.WARMUP_DEMOTER_START_TASK_NAME,
                            warmupDemoterData,
                            HttpMethod.POST,
                            HttpURLConnection.HTTP_OK));
            Double maxUsage = (Double) res.entrySet()
                    .stream()
                    .filter((entry) -> entry.getKey().endsWith(WorkerWarmupDemoterTask.MAX_USAGE_THRESHOLD_KEY))
                    .findAny()
                    .orElseThrow()
                    .getValue();
            Double cleanUsage = (Double) res.entrySet()
                    .stream()
                    .filter((entry) -> entry.getKey().endsWith(WorkerWarmupDemoterTask.CLEANUP_USAGE_THRESHOLD_KEY))
                    .findAny()
                    .orElseThrow()
                    .getValue();
            Integer batchSize = (Integer) res.entrySet()
                    .stream()
                    .filter((entry) -> entry.getKey().endsWith(WorkerWarmupDemoterTask.BATCH_SIZE_KEY))
                    .findAny()
                    .orElseThrow()
                    .getValue();
            Double epsilon = (Double) res.entrySet()
                    .stream()
                    .filter((entry) -> entry.getKey().endsWith(WorkerWarmupDemoterTask.EPSILON_KEY))
                    .findAny()
                    .orElseThrow()
                    .getValue();
            Integer maxElementsToDemote = (Integer) res.entrySet()
                    .stream()
                    .filter((entry) -> entry.getKey().endsWith(WorkerWarmupDemoterTask.MAX_ELEMENTS_TO_DEMOTE_ITERATION_KEY))
                    .findAny()
                    .orElseThrow()
                    .getValue();

            assertThat(maxUsage).isEqualTo(DEMOTE_MAX_USAGE_THRESHOLD_DEFAULT);
            assertThat(cleanUsage).isEqualTo(DEMOTE_CLEAN_USAGE_THRESHOLD_DEFAULT);
            assertThat(batchSize).isEqualTo(DEMOTE_DEFAULT_BATCH_SIZE);
            assertThat(epsilon).isEqualTo(DEMOTE_DEFAULT_EPSILON);
            assertThat(maxElementsToDemote).isEqualTo(DEMOTE_MAX_ELEMENTS_TO_DEMOTE);
        }
        catch (IOException e) {
            logger.error(e);
        }
    }

    protected Session createJmxSession()
    {
        return Session.builder(getSession())
                .setCatalog("jmx")
                .setSchema("current")
                .build();
    }

    protected void validateQueryStats(@Language("SQL") String query, Session session, Map<String, Long> expectedQueryStats, int expectedLuceneReadColumns)
    {
        int beforeLuceneReadColumns = ((StubsStorageEngine) storageEngineModule.getStorageEngine()).getLuceneReadColumns();
        validateQueryStats(query, session, expectedQueryStats, Collections.emptyList(), OptionalInt.empty(), OptionalInt.empty(), false);
        int actualLuceneReadColumns = ((StubsStorageEngine) storageEngineModule.getStorageEngine()).getLuceneReadColumns() - beforeLuceneReadColumns;
        assertThat(actualLuceneReadColumns).as("different result for actualLuceneReadColumns").isEqualTo(expectedLuceneReadColumns);
    }

    protected void validateQueryStats(@Language("SQL") String query, Session session, Map<String, Long> expectedQueryStats, boolean filterRange)
    {
        validateQueryStats(query, session, expectedQueryStats, Collections.emptyList(), OptionalInt.empty(), OptionalInt.empty(), filterRange);
    }

    protected void validateQueryStats(@Language("SQL") String query, Session session, Map<String, Long> expectedQueryStats)
    {
        validateQueryStats(query, session, expectedQueryStats, Collections.emptyList(), OptionalInt.empty(), OptionalInt.empty(), false);
    }

    protected void validateQueryStats(@Language("SQL") String query, Session session, Map<String, Long> expectedQueryStats, OptionalInt expectedSplits)
    {
        validateQueryStats(query, session, expectedQueryStats, Collections.emptyList(), expectedSplits, OptionalInt.empty(), false);
    }

    protected void validateQueryStats(@Language("SQL") String query, Session session, Map<String, Long> expectedQueryStats, OptionalInt expectedSplits, OptionalInt expectedRows)
    {
        validateQueryStats(query, session, expectedQueryStats, Collections.emptyList(), expectedSplits, expectedRows, false);
    }

    protected void validateQueryStats(@Language("SQL") String query,
            Session session,
            Map<String, Long> expectedJmxCounters,
            Collection<String> expectedPositiveQueryStats)
    {
        validateQueryStats(query, session, expectedJmxCounters, expectedPositiveQueryStats, OptionalInt.empty(), OptionalInt.empty(), false);
    }

    protected void validateQueryStats(@Language("SQL") String query,
            Session session,
            Map<String, Long> expectedJmxCounters,
            Collection<String> expectedPositiveQueryStats,
            OptionalInt expectedSplits,
            OptionalInt expectedRows,
            boolean filerRange)
    {
        Session jmxSession = createJmxSession();
        List<String> jmxCounters = expectedJmxCounters.keySet().stream().toList();
        StringJoiner columnJoiner = new StringJoiner(", ");
        for (String jmxCounter : jmxCounters) {
            columnJoiner.add(String.format("sum(%s) as %s", jmxCounter, jmxCounter));
        }
        @Language("SQL") String jmxQuery = format("select %s from \"*DispatcherPageSource*\"", columnJoiner);
        MaterializedResult jmxBefore = null;
        if (jmxCounters.size() > 0) {
            jmxBefore = computeActual(jmxSession, jmxQuery);
        }

        if (!filerRange) {
            session = Session.builder(session)
                    .setSystemProperty(catalog + "." + VaradaSessionProperties.MIN_MAX_FILTER, "false")
                    .build();
        }
        QueryRunner.MaterializedResultWithPlan resultWithQueryId = getQueryRunner().executeWithPlan(
                session,
                query);
        if (expectedSplits.isPresent()) {
            assertThat(resultWithQueryId.result()
                    .getStatementStats()
                    .orElseThrow()
                    .getTotalSplits())
                    .as("different result for total splits ")
                    .isEqualTo(expectedSplits.getAsInt());
        }
        if (expectedRows.isPresent()) {
            assertThat(resultWithQueryId.result().getRowCount())
                    .as("different result for row count ")
                    .isEqualTo(expectedRows.getAsInt());
        }
        Map<String, Long> customMetrics = getCustomMetrics(resultWithQueryId.queryId(), (DistributedQueryRunner) getQueryRunner());
        for (Map.Entry<String, Long> expectedStat : expectedJmxCounters.entrySet()) {
            String key = DispatcherPageSourceFactory.createFixedStatKey(DispatcherPageSourceFactory.STATS_DISPATCHER_KEY, expectedStat.getKey());
            Long actualResult = customMetrics.get(key);
            Long expectedResult = expectedStat.getValue();
            assertThat(actualResult)
                    .as("stat: %s, actualResult: %d, expectedResult: %d. query: %s", key, actualResult, expectedResult, query)
                    .isEqualTo(expectedResult);
        }
        for (String stat : expectedPositiveQueryStats) {
            // STATS_DISPATCHER_KEY is default
            String key = stat.contains(":") ? stat : DispatcherPageSourceFactory.createFixedStatKey(DispatcherPageSourceFactory.STATS_DISPATCHER_KEY, stat);
            Long actualResult = customMetrics.get(key);
            assertThat(actualResult)
                    .as("stat: %s, actualResult: %d, expectedResult: >0. query: %s", key, actualResult, query)
                    .isGreaterThan(0);
        }
        if (!jmxCounters.isEmpty()) {
            MaterializedResult jmxAfter = computeActual(jmxSession, jmxQuery);
            List<MaterializedRow> materializedRows = jmxBefore.getMaterializedRows();
            assertThat(materializedRows.size()).isOne();
            MaterializedRow statBefore = materializedRows.getFirst();
            MaterializedRow statAfter = jmxAfter.getMaterializedRows().getFirst();
            List<Object> fields = statBefore.getFields();
            for (int j = 0; j < jmxCounters.size(); j++) {
                String counterName = jmxCounters.get(j);
                Object valueBefore = fields.get(j);
                Object valueAfter = statAfter.getField(j);
                assertThat((Long) valueAfter - (Long) valueBefore)
                        .as("different result for stat=%s. query=%s", counterName, query)
                        .isEqualTo(expectedJmxCounters.get(counterName));
            }
        }
    }

    public static Map<String, Long> getCustomMetrics(QueryId queryId, DistributedQueryRunner queryRunner)
    {
        QueryInfo info = queryRunner.getCoordinator().getQueryManager().getFullQueryInfo(queryId);
        Metrics.Accumulator metrics = Metrics.accumulator();
        info.getQueryStats().getOperatorSummaries().stream().map(OperatorStats::getConnectorMetrics).forEach(metrics::add);
        return metrics.get()
                .getMetrics()
                .entrySet()
                .stream()
                .filter(metricEntry -> metricEntry.getValue() instanceof LongCount)
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> ((Count<?>) entry.getValue()).getTotal()));
    }

    @SuppressWarnings("LanguageMismatch")
    protected void warmAndValidate(String query, Session session, String warmValidationStat)
    {
        Session jmxSession = createJmxSession();
        int beforeStats = getWarmingServiceStats(jmxSession, warmValidationStat);
        MaterializedResult materializedRows = computeActual(session, query);
        validateLazyWarming(beforeStats, warmValidationStat);
        assertThat(materializedRows.getRowCount()).isEqualTo(1);
    }

    protected void warmAndValidate(String query, boolean defaultWarmup, String warmValidationStat)
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(catalog + "." + VaradaSessionProperties.ENABLE_DEFAULT_WARMING, Boolean.toString(defaultWarmup))
                .build();
        warmAndValidate(query, session, warmValidationStat);
    }

    @SuppressWarnings("LanguageMismatch")
    protected void warmAndValidate(String query,
            Session session,
            int expectedFinishedWarmupElements,
            int expectedWarmAccomplished,
            Integer expectedWarmedFailed)
    {
        warmAndValidate(
                query,
                session,
                expectedFinishedWarmupElements,
                expectedWarmAccomplished,
                Optional.ofNullable(expectedWarmedFailed));
    }

    protected void warmAndValidate(@Language("SQL") String query,
            Session session,
            int expectedFinishedWarmupElements,
            int expectedWarmAccomplished,
            Optional<Integer> expectedWarmedFailed)
    {
        List<String> statsColNames = List.of("warm_accomplished",
                "warmup_elements_count",
                "warm_failed",
                "warm_started");
        String warmStatsTableName = "%s:catalog=%s,name=warming-service.%s,type=%s".formatted(
                VaradaStatsWarmingService.class.getPackageName(),
                catalog,
                catalog,
                VaradaStatsWarmingService.class.getSimpleName().toLowerCase(Locale.ROOT));
        Session jmxSession = createJmxSession();
        MaterializedRow materializedRow = null;
        try {
            materializedRow = getServiceStats(jmxSession, warmStatsTableName, statsColNames);
        }
        catch (Throwable e) {
            MaterializedResult rows = computeActual(createJmxSession(), "show tables");
            logger.error(e,
                    "jmx[%d] tables: %s",
                    rows.getMaterializedRows().size(),
                    rows.getMaterializedRows()
                            .stream()
                            .filter(materializedRowTmp -> ((String) materializedRowTmp.getField(0)).contains(WorkerWarmingService.WARMING_SERVICE_STAT_GROUP))
                            .collect(Collectors.toList()));
            fail("materializedRow is null", e);
        }
        long beforeWarmAccomplishedStats = (Long) materializedRow.getField(0);
        long beforeWarmupElementsCount = (Long) materializedRow.getField(1);
        long beforeWarmupFailedCount = (Long) materializedRow.getField(2);
        computeActual(session, query);

        runWithRetries(() -> {
            MaterializedRow materializedRowAfter = getServiceStats(jmxSession, warmStatsTableName, statsColNames);
            long actualWarmAccomplished = (Long) materializedRowAfter.getField(0) - beforeWarmAccomplishedStats;
            long actualElementsFinishedCount = (Long) materializedRowAfter.getField(1) - beforeWarmupElementsCount;
            long actualWarmFailed = (Long) materializedRowAfter.getField(2) - beforeWarmupFailedCount;
            logger.info("actualWarmAccomplished=%d, expectedWarmAccomplished=%d, actualElementsFinishedCount=%d, expectedFinishedWarmupElements=%d, actualWarmFailed=%d, expectedWarmedFailed=%s",
                    actualWarmAccomplished, expectedWarmAccomplished, actualElementsFinishedCount, expectedFinishedWarmupElements, actualWarmFailed, expectedWarmedFailed.toString());
            assertThat(actualWarmAccomplished)
                    .describedAs("actualWarmAccomplished is not as expected. %s", query)
                    .isEqualTo(expectedWarmAccomplished);
            assertThat(actualElementsFinishedCount)
                    .describedAs("expectedFinishedWarmupElements is not as expected. %s", query)
                    .isEqualTo(expectedFinishedWarmupElements);

            expectedWarmedFailed.ifPresent(integer -> assertThat(actualWarmFailed)
                    .describedAs("expectedWarmedFailed is not as expected. %s", query)
                    .isEqualTo(integer.longValue()));
        });
        logger.info("finished warming");
    }

    protected void validateDictionaryStats(Session jmxSession, long expectedMaxException, long expectedDictionaryWriteCount, long expectedReadDictionaryCount)
    {
        runWithRetries(() -> {
            MaterializedResult dictionaryStats = computeActual(jmxSession, "select sum(dictionary_max_exception_count), sum(write_dictionaries_count), sum(dictionary_read_elements_count), sum(dictionary_active_size) from \"*dictionary*\"");
            long actualDictionaryMaxExceptionCount = (long) (Long) dictionaryStats.getMaterializedRows().getFirst().getField(0);
            long actualDictionaryWriteCount = (long) (Long) dictionaryStats.getMaterializedRows().getFirst().getField(1);
            long actualReadDictionaryCount = (long) dictionaryStats.getMaterializedRows().getFirst().getField(2);
            logger.info("actualDictionaryMaxExceptionCount=%d, expectedMaxException=%d, actualDictionaryWriteCount=%d, ,expectedWarmupElements=%d, actualReadDictionaryCount=%s, expectedReadDictionaryCount=%s",
                    actualDictionaryMaxExceptionCount, expectedMaxException, actualDictionaryWriteCount, expectedDictionaryWriteCount, actualReadDictionaryCount, expectedReadDictionaryCount);
            assertThat(actualDictionaryMaxExceptionCount).isEqualTo(expectedMaxException);
            assertThat(actualDictionaryWriteCount).isEqualTo(expectedDictionaryWriteCount);
            assertThat(actualReadDictionaryCount).isEqualTo(expectedReadDictionaryCount);
            assertThat((long) dictionaryStats.getMaterializedRows().getFirst().getField(3)).isEqualTo(0);
        });
    }

    protected void warmAndValidate(String query,
            boolean defaultWarmup,
            int expectedWarmupElements,
            int expectedWarmFinished)
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(catalog + "." + VaradaSessionProperties.ENABLE_DEFAULT_WARMING_INDEX, "false")
                .setSystemProperty(catalog + "." + VaradaSessionProperties.ENABLE_DEFAULT_WARMING, Boolean.toString(defaultWarmup))
                .setSystemProperty(catalog + "." + VaradaSessionProperties.EMPTY_QUERY, "true")
                .build();
        warmAndValidate(query,
                session,
                expectedWarmupElements,
                expectedWarmFinished,
                0);
    }

    protected void warmAndValidateWithExport(String query,
            Session session,
            int expectedWarmupElements,
            int expectedWarmFinished,
            int expectedExportRowGroupsAccomplished)
    {
        Session jmxSession = createJmxSession();
        MaterializedResult jmxBefore = computeActual(jmxSession, "select sum(export_row_group_accomplished) from \"*warmupExportService*\"");
        long beforeExportRowGroupCount = (long) (Long) jmxBefore.getMaterializedRows().getFirst().getField(0);

        warmAndValidate(query, session, expectedWarmupElements, expectedWarmFinished, 0);

        runWithRetries(() -> {
            MaterializedResult jmxAfter = computeActual(jmxSession, "select sum(export_row_group_accomplished) from \"*warmupExportService*\"");
            MaterializedRow materializedRowAfter = jmxAfter.getMaterializedRows().getFirst();
            long actualNewExportRowGroupAccomplishedAfter = (Long) materializedRowAfter.getField(0) - beforeExportRowGroupCount;
            logger.info("actualNewExportRowGroupAccomplished=%d, expectedExportRowGroupsAccomplished=%d",
                    actualNewExportRowGroupAccomplishedAfter,
                    expectedExportRowGroupsAccomplished);
            assertThat(actualNewExportRowGroupAccomplishedAfter).isEqualTo(expectedExportRowGroupsAccomplished);
        });
    }

    protected void validateLazyWarming(int beforeWarmStats, String warmValidationStat)
    {
        Session jmxSession = createJmxSession();

        runWithRetries(() -> {
            long numberOfWarmProcessedFiles = getWarmingServiceStats(jmxSession, warmValidationStat);
            assertThat(numberOfWarmProcessedFiles).isGreaterThan(beforeWarmStats);
        });
    }

    @SuppressWarnings("SameParameterValue")
    protected void warmAndValidateLazyDemote(String query, boolean lazyWarmup)
    {
        Session jmxSession = createJmxSession();
        String jmxTable = "warmupDemoter";
        MaterializedRow before = getServiceStats(jmxSession, jmxTable, DEMOTE_JMX_NAMES);
        warmAndValidate(query, lazyWarmup, "warm_finished");

        runWithRetries(() -> assertThat(validateStat(before, jmxTable, DEMOTE_JMX_NAMES)).isTrue());
    }

    protected void validateDemoter(int expectedDeadObjects)
            throws IOException
    {
        logger.info("DEMOTER ######");
        WarmupDemoterData warmupDemoterData = WarmupDemoterData.builder().maxUsageThresholdInPercentage(DEMOTE_CLEAN_UP_USAGE)
                .cleanupUsageThresholdInPercentage(DEMOTE_CLEAN_UP_USAGE)
                .executeDemoter(true)
                .modifyConfiguration(true)
                .resetHighestPriority(true)
                .forceDeleteFailedObjects(true)
                .build();
        restDemoteConfigurationToDefaults();
        Session jmxSession = createJmxSession();
        String jmxTable = "warmupDemoter";
        List<String> deadObjectsDeleted = List.of("dead_objects_deleted");
        MaterializedRow before = getServiceStats(jmxSession, jmxTable, deadObjectsDeleted);
        executeRestCommand(WarmupDemoterTask.WARMUP_DEMOTER_PATH, WarmupDemoterTask.WARMUP_DEMOTER_START_TASK_NAME, warmupDemoterData, HttpMethod.POST, HttpURLConnection.HTTP_OK);
        //String actualKey = result.substring(2, result.indexOf(":")) + ":" + jmxTable + ":" + deadObjectsDeleted;
        runWithRetries(() -> {
            MaterializedRow after = getServiceStats(jmxSession, jmxTable, deadObjectsDeleted);
            long actualDeadObjectCount = (Long) after.getField(0) - (Long) before.getField(0);
            assertThat(actualDeadObjectCount).isEqualTo(expectedDeadObjects);
        });
    }

    protected Map<String, Object> demote(WarmupDemoterData warmupDemoterData)
            throws IOException
    {
        restDemoteConfigurationToDefaults();
        Session jmxSession = createJmxSession();
        String jmxTable = "warmupDemoter";
        MaterializedRow before = getServiceStats(jmxSession, jmxTable, DEMOTE_JMX_NAMES);

        String result = executeRestCommand(WarmupDemoterTask.WARMUP_DEMOTER_PATH, WarmupDemoterTask.WARMUP_DEMOTER_START_TASK_NAME, warmupDemoterData, HttpMethod.POST, HttpURLConnection.HTTP_OK);
        Map<String, Object> res = objectMapper.readerFor(new TypeReference<Map<String, Object>>() {}).readValue(result);
        if (warmupDemoterData.isExecuteDemoter()) {
            boolean valid = validateStat(before, jmxTable, DEMOTE_JMX_NAMES);
            assertThat(valid).isTrue();
        }
        return res;
    }

    protected boolean validateStat(MaterializedRow beforeStatsMaterializedRow,
            String jmxTable,
            List<String> statColNames)
    {
        Session jmxSession = createJmxSession();
        MaterializedRow afterStatsMaterializedRow = getServiceStats(jmxSession, jmxTable, statColNames);
        boolean result = true;
        for (int i = 0; i < beforeStatsMaterializedRow.getFieldCount(); i++) {
            result = (long) beforeStatsMaterializedRow.getField(i) <= (long) afterStatsMaterializedRow.getField(i);
            if (!result) {
                logger.warn("validateStat:: before[%s]= %s VS after[%s]=%s",
                        statColNames.get(i), beforeStatsMaterializedRow.getField(i),
                        statColNames.get(i), afterStatsMaterializedRow.getField(i));
                break;
            }
        }
        return result;
    }

    protected int getWarmingServiceStats(Session jmxSession, String statColName)
    {
        long result = (long) getServiceStats(jmxSession,
                "io.trino.plugin.warp.gen.stats:catalog=" + catalog + ",name=warming-service." + catalog + ",type=varadastatswarmingservice",
                List.of(statColName))
                .getField(0);
        return (int) result;
    }

    protected MaterializedRow getServiceStats(Session jmxSession, String jmxTable, List<String> statColNames)
    {
        String statSumColNames = statColNames.stream()
                .map(s -> "sum(" + s + ")")
                .collect(Collectors.joining(","));
        MaterializedResult jmx0 = computeActual(jmxSession, String.format("select %s from \"*%s*\"", statSumColNames, jmxTable));
        logger.info("getServiceStats::jmxTable=%s", jmxTable);
        return jmx0.getMaterializedRows().getFirst();
    }

    protected void runWithRetries(Runnable runnable)
    {
        Failsafe.with(new RetryPolicy<>()
                        .handle(AssertionError.class)
                        .withMaxRetries(10)
                        .withDelay(Duration.ofSeconds(2)))
                .run(runnable::run);
    }

    protected Session buildSession(boolean defaultWarm, boolean enableDefaultWarmIndex)
    {
        Session.SessionBuilder sessionBuilder = Session.builder(getSession());
        if (defaultWarm) {
            sessionBuilder.setSystemProperty(catalog + "." + VaradaSessionProperties.ENABLE_DEFAULT_WARMING, "true");
            if (enableDefaultWarmIndex) {
                sessionBuilder.setSystemProperty(catalog + "." + VaradaSessionProperties.ENABLE_DEFAULT_WARMING_INDEX, "true");
            }
        }
        return sessionBuilder.build();
    }

    @Test
    public void testGoAllProxyOnlyWhenHavePushDowns()
            throws IOException
    {
        try {
            computeActual("CREATE TABLE all_proxy_test (int_1 integer, int_2 integer)");
            computeActual("INSERT INTO all_proxy_test (int_1, int_2) values (1, 10), (2, 20), (3, 30)");
            createWarmupRules(DEFAULT_SCHEMA, "all_proxy_test", Map.ofEntries(entry("int_1", Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, DEFAULT_PRIORITY, DEFAULT_TTL)))));
            Session session = buildSession(false, false);
            warmAndValidate("select * from all_proxy_test", session, 1, 1, 0);

            @Language("SQL") String pushDownQuery = "select * from all_proxy_test where int_1 < 2";
            Map<String, Long> expectedPushDownQueryStats = Map.of(
                    "varada_match_columns", 0L,
                    "varada_collect_columns", 0L,
                    "external_match_columns", 1L,
                    "external_collect_columns", 2L);
            validateQueryStats(pushDownQuery, session, expectedPushDownQueryStats);
            @Language("SQL") String noPushDownQuery = "select * from all_proxy_test where ceiling(int_1) < 2";
            Map<String, Long> expectedNoPushDownQueryStats = Map.of(
                    "varada_match_columns", 0L,
                    "varada_collect_columns", 1L,
                    "external_match_columns", 0L,
                    "external_collect_columns", 1L);
            validateQueryStats(noPushDownQuery, session, expectedNoPushDownQueryStats);
        }
        finally {
            computeActual("DROP TABLE IF EXISTS all_proxy_test");
        }
    }
}
