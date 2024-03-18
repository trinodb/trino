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

package io.trino.tests.product.warp.utils;

import com.google.common.annotations.VisibleForTesting;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.tempto.query.QueryExecutor;
import io.trino.tempto.query.QueryResult;
import org.intellij.lang.annotations.Language;
import org.testng.asserts.SoftAssert;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import static io.trino.tests.product.utils.QueryAssertions.assertEventually;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static io.trino.tests.product.warp.utils.JMXCachingConstants.Dictionary.DICTIONARY_MAX_EXCEPTION_COUNT;
import static io.trino.tests.product.warp.utils.JMXCachingConstants.Dictionary.DICTIONARY_REJECTED_ELEMENTS_COUNT;
import static io.trino.tests.product.warp.utils.JMXCachingConstants.Dictionary.DICTIONARY_SUCCESS_ELEMENTS_COUNT;
import static io.trino.tests.product.warp.utils.JMXCachingConstants.Dictionary.WRITE_DICTIONARIES_COUNT;
import static io.trino.tests.product.warp.utils.JMXCachingConstants.WarmingService.FINISHED;
import static io.trino.tests.product.warp.utils.JMXCachingConstants.WarmingService.SCHEDULED;
import static io.trino.tests.product.warp.utils.JMXCachingConstants.WarmingService.STARTED;
import static io.trino.tests.product.warp.utils.JMXCachingConstants.WarmingService.WARM_ACCOMPLISHED;
import static io.trino.tests.product.warp.utils.JMXCachingConstants.WarmingService.WARM_FAILED;
import static io.trino.tests.product.warp.utils.JMXCachingConstants.WarmupExportService.EXPORT_ROW_GROUP_ACCOMPLISHED;
import static io.trino.tests.product.warp.utils.JMXCachingConstants.WarmupExportService.EXPORT_ROW_GROUP_FINISHED;
import static io.trino.tests.product.warp.utils.JMXCachingConstants.WarmupExportService.EXPORT_ROW_GROUP_SCHEDULED;
import static io.trino.tests.product.warp.utils.JMXCachingConstants.WarmupExportService.EXPORT_ROW_GROUP_STARTED;
import static io.trino.tests.product.warp.utils.JMXCachingConstants.WarmupImportService.IMPORT_ELEMENTS_ACCOMPLISHED;
import static io.trino.tests.product.warp.utils.JMXCachingConstants.WarmupImportService.IMPORT_ELEMENTS_FAILED;
import static io.trino.tests.product.warp.utils.JMXCachingConstants.WarmupImportService.IMPORT_ELEMENTS_STARTED;
import static io.trino.tests.product.warp.utils.JMXCachingConstants.WarmupImportService.IMPORT_ROW_GROUP_COUNT_ACCOMPLISHED;
import static io.trino.tests.product.warp.utils.JMXCachingConstants.WarmupImportService.IMPORT_ROW_GROUP_COUNT_FAILED;
import static io.trino.tests.product.warp.utils.JMXCachingConstants.WarmupImportService.IMPORT_ROW_GROUP_COUNT_STARTED;
import static io.trino.tests.product.warp.utils.JMXCachingManager.getDiffFromInitial;
import static io.trino.tests.product.warp.utils.JMXCachingManager.getValue;
import static io.trino.tests.product.warp.utils.QueryUtils.verifyQueryCountersJson;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class WarmUtils
{
    private static final Logger logger = Logger.get(WarmUtils.class);

    private static final List<String> DICTIONARY_COUNTERS_LIST = List.of(
            WRITE_DICTIONARIES_COUNT,
            DICTIONARY_SUCCESS_ELEMENTS_COUNT,
            DICTIONARY_MAX_EXCEPTION_COUNT,
            DICTIONARY_REJECTED_ELEMENTS_COUNT);

    public WarmUtils()
    {
    }

    public String createWarmupQuery(String tableName, List<TestFormat.Column> tableStructure)
    {
        if (tableStructure == null) {
            throw new IllegalArgumentException("Need to provide either tableStructure or columnsList");
        }
        StringJoiner columns;
        columns = new StringJoiner(",");
        // iterate through the remaining elements in the array
        for (TestFormat.Column value : tableStructure) {
            String column = format(" count(%s)", value.name());
            columns.add(column);
        }
        return "select " + columns + " from " + tableName;
    }

    public void warmAndValidate(TestFormat testFormat, FastWarming fastWarming)
    {
        @Language("SQL")
        String warmQuery = testFormat.warm_query();
        if (warmQuery == null) {
            warmQuery = createWarmupQuery(testFormat.name(), testFormat.structure());
        }
        warmAndValidate(
                testFormat.name(),
                warmQuery,
                testFormat.expected_warm_failures(),
                testFormat.session_properties() != null ? testFormat.session_properties().entrySet().stream().collect(Collectors.toMap(e -> "warp." + e.getKey(), Map.Entry::getValue)) : new HashMap<>(),
                testFormat.expected_dictionary_counters(),
                fastWarming);
    }

    public void warmAndValidate(
            String tableName,
            @Language("SQL") String warmQuery,
            int expectedFailures,
            Map<String, Object> sessionPropertiesWithCatalog,
            Map<String, Long> expectedDictionaryCounters,
            FastWarming fastWarming)
    {
        QueryResult warmingStatsBefore = JMXCachingManager.getWarmingStats();
        QueryResult dictionaryRowBefore = JMXCachingManager.getDictionaryStats();
        QueryResult exportRowBefore = JMXCachingManager.getExportStats();
        QueryResult importRowBefore = JMXCachingManager.getImportStats();
        boolean defaultWarming = sessionPropertiesWithCatalog != null &&
                Boolean.parseBoolean(
                        sessionPropertiesWithCatalog.getOrDefault("warp.enable_default_warming", "false").toString());
        logger.info("STARTING WARMUP=%s, fastWarming=%s, defaultWarming=%b", warmQuery, fastWarming, defaultWarming);
        QueryExecutor queryExecutor = onTrino();
        queryExecutor.executeQuery("SET SESSION warp.empty_query=True");
        queryExecutor.executeQuery(format("set session warp.enable_default_warming = %b", defaultWarming));
        queryExecutor.executeQuery(warmQuery);
        queryExecutor.executeQuery("SET SESSION warp.empty_query=false");

        assertEventually(
                Duration.valueOf("360s"),
                () -> {
                    QueryResult warmingStatsAfter = JMXCachingManager.getWarmingStats();
                    QueryResult dictionaryStatsAfter = JMXCachingManager.getDictionaryStats();
                    QueryResult exportStatsAfter = JMXCachingManager.getExportStats();
                    QueryResult importRowAfter = JMXCachingManager.getImportStats();

                    if (fastWarming != FastWarming.IMPORT) {
                        assertThat(getDiffFromInitial(warmingStatsAfter, warmingStatsBefore, WARM_ACCOMPLISHED))
                                .as("warm_accomplished must be positive during warm but was zero. tableName=%s", tableName)
                                .isPositive();
                    }
                    else {
                        assertThat(getDiffFromInitial(importRowAfter, importRowBefore, IMPORT_ELEMENTS_ACCOMPLISHED))
                                .as("import_elements_accomplished must be positive. tableName=%s", tableName)
                                .isPositive();
                        assertThat(getDiffFromInitial(importRowAfter, importRowBefore, IMPORT_ELEMENTS_STARTED))
                                .as("import_elements_started must be equal to import_elements_accomplished but wasn't. tableName=%s", tableName)
                                .isEqualTo(getDiffFromInitial(importRowAfter, importRowBefore, IMPORT_ELEMENTS_ACCOMPLISHED));
                        assertThat(getDiffFromInitial(importRowAfter, importRowBefore, IMPORT_ELEMENTS_FAILED))
                                .as("import_elements_failed must be zero, but wasn't. tableName=%s", tableName)
                                .isZero();
                    }
                    assertThat(getDiffFromInitial(warmingStatsAfter, warmingStatsBefore, SCHEDULED))
                            .as("warm_scheduled must be equal to warm_finished but wasn't. tableName=%s", tableName)
                            .isEqualTo(getDiffFromInitial(warmingStatsAfter, warmingStatsBefore, FINISHED));
                    assertThat(getDiffFromInitial(warmingStatsAfter, warmingStatsBefore, STARTED))
                            .as("warm_started must be equal to warm_accomplished but wasn't. tableName=%s", tableName)
                            .isEqualTo(getDiffFromInitial(warmingStatsAfter, warmingStatsBefore, WARM_ACCOMPLISHED));
                    if (expectedFailures > 0) {
                        assertThat(getDiffFromInitial(warmingStatsAfter, warmingStatsBefore, WARM_FAILED))
                                .as("warm_failed must be greaterThanOrEqual to expectedFailures=%s. tableName=%s", expectedFailures, tableName)
                                .isGreaterThanOrEqualTo(expectedFailures);
                    }
                    else {
                        assertThat(getDiffFromInitial(warmingStatsAfter, warmingStatsBefore, WARM_FAILED))
                                .as("warm_failed must be equal to dictionary_max_exception_count but wasn't. tableName=%s", tableName)
                                .isEqualTo(getDiffFromInitial(dictionaryStatsAfter, dictionaryRowBefore, DICTIONARY_MAX_EXCEPTION_COUNT));
                    }
                    if (fastWarming == FastWarming.EXPORT) {
                        assertThat(getDiffFromInitial(exportStatsAfter, exportRowBefore, EXPORT_ROW_GROUP_SCHEDULED))
                                .as("export_row_group_scheduled must be positive. tableName=%s", tableName)
                                .isPositive();
                        assertThat(getDiffFromInitial(exportStatsAfter, exportRowBefore, EXPORT_ROW_GROUP_SCHEDULED))
                                .as("export_row_group_scheduled must be equal to export_row_group_finished but wasn't. tableName=%s", tableName)
                                .isEqualTo(getDiffFromInitial(exportStatsAfter, exportRowBefore, EXPORT_ROW_GROUP_FINISHED));
                        assertThat(getDiffFromInitial(exportStatsAfter, exportRowBefore, EXPORT_ROW_GROUP_STARTED))
                                .as("export_row_group_started must be equal to export_row_group_accomplished but wasn't. tableName=%s", tableName)
                                .isEqualTo(getDiffFromInitial(exportStatsAfter, exportRowBefore, EXPORT_ROW_GROUP_ACCOMPLISHED));
                    }
                    else if (fastWarming == FastWarming.IMPORT) {
                        assertThat(getDiffFromInitial(exportStatsAfter, exportRowBefore, EXPORT_ROW_GROUP_SCHEDULED))
                                .as("export_row_group_scheduled must be zero, but wasn't. tableName=%s", tableName)
                                .isZero();
                        assertThat(getDiffFromInitial(importRowAfter, importRowBefore, IMPORT_ROW_GROUP_COUNT_STARTED))
                                .as("import_row_group_count_started must be equal to import_row_group_count_accomplished but wasn't. tableName=%s", tableName)
                                .isEqualTo(getDiffFromInitial(importRowAfter, importRowBefore, IMPORT_ROW_GROUP_COUNT_ACCOMPLISHED));
                        assertThat(getDiffFromInitial(importRowAfter, importRowBefore, IMPORT_ROW_GROUP_COUNT_FAILED))
                                .as("import_row_group_count_failed must be zero, but wasn't. tableName=%s", tableName)
                                .isZero();
                    }
                });
        SoftAssert softAssert = new SoftAssert();
        if (expectedDictionaryCounters != null && !expectedDictionaryCounters.isEmpty()) {
            verifyDictionaryCounters(expectedDictionaryCounters, dictionaryRowBefore, tableName, softAssert);
        }
        softAssert.assertAll();
        logger.info("Warmup process has finished %s %s", tableName, fastWarming);
    }

    public QueryResult warmAndValidate(@Language("SQL") String query, Map<String, Long> expectedResults)
    {
        return warmAndValidate(query, expectedResults, Duration.valueOf("360s"));
    }

    public QueryResult warmAndValidate(
            @Language("SQL") String query,
            Map<String, Long> expectedResults,
            Duration duration)
    {
        QueryResult warmingStatsBefore = JMXCachingManager.getWarmingStats();
        logger.info("STARTING WARMUP=%s", query);
        QueryResult queryResult = onTrino().executeQuery(query);
        assertEventually(
                duration,
                () -> {
                    QueryResult warmingStatsAfter = JMXCachingManager.getWarmingStats();
                    for (Map.Entry<String, Long> entry : expectedResults.entrySet()) {
                        long actualValue = getDiffFromInitial(warmingStatsAfter, warmingStatsBefore, entry.getKey());
                        assertThat(actualValue)
                                .as("warmAndValidate fail - %s expected %d actual %d", entry.getKey(), entry.getValue(), actualValue)
                                .isEqualTo(entry.getValue());
                    }
                });
        return queryResult;
    }

    private void verifyDictionaryCounters(Map<String, Long> expectedDictionaryCounters, QueryResult dictionaryRowBefore, String testName, SoftAssert softAssert)
    {
        if (expectedDictionaryCounters == null) {
            return;
        }

        QueryResult dictionaryStatsAfter = JMXCachingManager.getDictionaryStats();

        Map<String, Long> actualValues = new HashMap<>();
        for (String stat : DICTIONARY_COUNTERS_LIST) {
            long actualValue = getDiffFromInitial(dictionaryStatsAfter, dictionaryRowBefore, stat);
            actualValues.put(stat, actualValue);
        }
        verifyQueryCountersJson(DICTIONARY_COUNTERS_LIST, expectedDictionaryCounters, actualValues, testName, softAssert);
    }

    public void setSessions(Map<String, Object> sessionProperties)
    {
        if (sessionProperties == null) {
            return;
        }
        for (Map.Entry<String, Object> entry : sessionProperties.entrySet()) {
            onTrino().executeQuery(format("set session %s = %s", entry.getKey(), entry.getValue()));
        }
    }

    public void resetSessions(Map<String, Object> sessionProperties)
    {
        if (sessionProperties == null) {
            return;
        }
        for (Map.Entry<String, Object> entry : sessionProperties.entrySet()) {
            onTrino().executeQuery(format("reset session %s", entry.getKey()));
        }
    }

    static void verifyNoWarmups()
    {
        assertEventually(
                Duration.valueOf("20s"),
                () -> {
                    QueryResult warmStats = JMXCachingManager.getWarmingStats();
                    assertThat(getValue(warmStats, STARTED)).isEqualTo(getValue(warmStats, WARM_ACCOMPLISHED));
                });
    }

    @VisibleForTesting
    String createTableQuery(TestFormat testFormat)
    {
        String name = testFormat.name();
        List<TestFormat.Column> structure = testFormat.structure();
        StringBuilder tableDefinition = new StringBuilder();
        for (int i = 0; i < structure.size(); i++) {
            TestFormat.Column column = structure.get(i);
            String fieldName = column.name();
            String fieldType = column.type();
            List<Object> args = column.args();
            String fieldDefinition = fieldName + " " + createFieldDefinition(fieldType, args);
            if (i != 0) {
                tableDefinition.append(", ");
            }
            tableDefinition.append(fieldDefinition);
        }

        List<Object> bucketedBy = testFormat.bucketed_by();
        String bucketedByStr = "ARRAY[]";
        if (bucketedBy != null && !bucketedBy.isEmpty()) {
            StringBuilder bucketedByBuilder = new StringBuilder("ARRAY[");
            for (int i = 0; i < bucketedBy.size(); i++) {
                bucketedByBuilder.append("'" + bucketedBy.get(i) + "'");
                if (i < bucketedBy.size() - 1) {
                    bucketedByBuilder.append(",");
                }
            }
            bucketedByBuilder.append("]");
            bucketedByStr = bucketedByBuilder.toString();
        }

        int bucketCount = testFormat.bucket_count();
        return String.format("CREATE TABLE IF NOT EXISTS warp.synthetic.%s (%s) " +
                        "WITH (external_location='s3://warp-speed-us-east1-systemtests/synthetic/%s'," +
                        "format='PARQUET',partitioned_by=ARRAY[],bucketed_by=%s,bucket_count=%d)",
                name, tableDefinition.toString(), name, bucketedByStr, bucketCount);
    }

    public void createTable(TestFormat testFormat)
    {
        String query = createTableQuery(testFormat);
        onTrino().executeQuery(query);
    }

    private String createFieldDefinition(String fieldType, List<Object> args)
    {
        String fieldDef;
        if (fieldType == null) {
            fieldDef = "integer";
        }
        else if (args == null) {
            fieldDef = fieldType;
        }
        else if ("varchar".equals(fieldType)) {
            fieldDef = "varchar(%s)".formatted(args.getFirst());
        }
        else if ("array".equals(fieldType)) {
            if (args.size() == 1) {
                fieldDef = "array(%s)".formatted(args.getFirst());
            }
            else if (args.size() == 2 && "char".equals(args.get(0))) {
                fieldDef = "array(%s(%s))".formatted(args.get(0), args.get(1));
            }
            else {
                throw new RuntimeException("unknown field type %s, args %s".formatted(fieldType, args));
            }
        }
        else if ("map".equals(fieldType)) {
            fieldDef = "map(%s, %s)".formatted(args.get(0), args.get(1));
        }
        else if ("row".equals(fieldType)) {
            StringJoiner rowColumnDef = new StringJoiner(",", "(", ")");
            args.forEach(arg -> rowColumnDef.add(arg.toString()));
            fieldDef = "ROW" + rowColumnDef;
        }
        else if (fieldType.equals("char") || fieldType.equals("decimal")) {
            if (args.size() == 3) {
                args = args.subList(1, args.size());
            }
            else if (args.size() > 3) {
                throw new RuntimeException();
            }
            StringJoiner rowColumnDef = new StringJoiner(",", "(", ")");
            args.forEach(arg -> rowColumnDef.add(arg.toString()));
            fieldDef = fieldType + rowColumnDef;
        }
        else {
            fieldDef = fieldType;
        }
        return fieldDef;
    }
}
