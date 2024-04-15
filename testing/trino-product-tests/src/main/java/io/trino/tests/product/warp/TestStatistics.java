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
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.plugin.varada.api.warmup.RuleResultDTO;
import io.trino.plugin.varada.api.warmup.WarmupColRuleData;
import io.trino.tempto.BeforeMethodWithContext;
import io.trino.tempto.query.QueryResult;
import io.trino.tests.product.warp.utils.DemoterUtils;
import io.trino.tests.product.warp.utils.FastWarming;
import io.trino.tests.product.warp.utils.QueryUtils;
import io.trino.tests.product.warp.utils.RestUtils;
import io.trino.tests.product.warp.utils.RuleUtils;
import io.trino.tests.product.warp.utils.TestFormat;
import io.trino.tests.product.warp.utils.WarmTypeForStrings;
import io.trino.tests.product.warp.utils.WarmUtils;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.TestGroups.WARP_SPEED_HIVE_2;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static io.trino.tests.product.warp.utils.DemoterUtils.objectMapper;
import static io.trino.tests.product.warp.utils.RuleUtils.TASK_NAME_SET;
import static io.trino.tests.product.warp.utils.RuleUtils.WARMUP_PATH;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestStatistics
{
    private enum ColumnStatisticsNames
    {
        COLUMNNAME,
        DATASIZE,
        DISTINCTVALUESCOUNT,
        NULLSFRACTION,
        ROWCOUNT,
        LOWVALUE,
        HIGHVALUE
    }

    private record ColumnStatistics(
            Optional<String> columnName,
            Optional<Double> dataSize,
            Optional<Double> distinctValuesCount,
            Optional<Double> nullsFraction,
            Optional<Double> rowCount,
            Optional<Double> lowValue,
            Optional<Double> highValue) {}

    private static final Logger logger = Logger.get(TestStatistics.class);

    private static final String BUCKET_NAME = "warp-speed-us-east1-systemtests/tiny_h";
    private static final String CATALOG_NAME = "warp";
    private static final String SCHEMA_NAME = "warp_product_tests";
    private static final String TABLE_NAME = "show_stats_table";
    private static final String TABLE_COLUMN_INT = "int_col";
    private static final String TABLE_COLUMN_BOOL = "boolean_col";
    private static final String TABLE_COLUMN_STRING = "string_col";

    private static final List<TestFormat.Column> COLUMNS = List.of(
            new TestFormat.Column(TABLE_COLUMN_INT, "integer", List.of()),
            new TestFormat.Column(TABLE_COLUMN_BOOL, "boolean", List.of()),
            new TestFormat.Column(TABLE_COLUMN_STRING, "varchar", List.of()));

    private boolean initialized;

    @Inject
    WarmUtils warmUtils;
    @Inject
    QueryUtils queryUtils;
    @Inject
    RuleUtils ruleUtils;
    @Inject
    RestUtils restUtils;
    @Inject
    DemoterUtils demoterUtils;

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

    private void initialize()
    {
        onTrino().executeQuery(format("USE %s.%s", CATALOG_NAME, SCHEMA_NAME));

        onTrino().executeQuery(format("CREATE TABLE IF NOT EXISTS %s (int_col integer, boolean_col boolean, string_col varchar(20)) " +
                "WITH (external_location='s3://%s/',format='JSON',partitioned_by=ARRAY[],bucketed_by=ARRAY[],bucket_count=0)", TABLE_NAME, BUCKET_NAME));

        String warmQuery = format("select * from %s", TABLE_NAME);
        TestFormat testFormat = TestFormat.builder()
                .name(TABLE_NAME)
                .tableName(TABLE_NAME)
                .structure(COLUMNS)
                .warmQuery(warmQuery)
                .build();

        // Cleaning possible previous run results
        try {
            ruleUtils.resetAllRules();
            demoterUtils.demote(SCHEMA_NAME, TABLE_NAME, testFormat);
            demoterUtils.resetToDefaultDemoterConfiguration();
        }
        catch (Exception e) {
            logger.error(e, "Preparing clean failed");
        }

        Set<WarmupColRuleData> rules = ruleUtils.createRulesFromStructure(SCHEMA_NAME, testFormat, WarmTypeForStrings.lucene_data_basic);
        logger.info(format("Prepared Rules=%s", rules));

        try {
            String result = restUtils.executePostCommandWithReturnValue(WARMUP_PATH, TASK_NAME_SET, rules);
            RuleResultDTO res = objectMapper.readerFor(new TypeReference<RuleResultDTO>() {}).readValue(result);
            assertThat(res.rejectedRules().isEmpty() && !res.appliedRules().isEmpty())
                    .as("some rules are rejected. %s", res.rejectedRules())
                    .isTrue();
            logger.debug("created %s rules for schemaTable=%s.%s", res.appliedRules(), SCHEMA_NAME, TABLE_NAME);
            warmUtils.warmAndValidate(testFormat, FastWarming.NONE);
        }
        catch (Exception e) {
            logger.error(e, "failed on ADDING RULES");
        }
    }

    @Test(groups = {WARP_SPEED_HIVE_2, PROFILE_SPECIFIC_TESTS})
    public void testCustomMetrics()
    {
        QueryResult queryResult = onTrino().executeQuery(format("EXPLAIN ANALYZE VERBOSE SELECT * FROM %s.%s.%s", CATALOG_NAME, SCHEMA_NAME, TABLE_NAME));
        logger.debug("queryResult=%s", queryResult.rows());
        assertThat(queryResult.rows().toString().contains("varada-collect:string_col:WARM_UP_TYPE_DATA'")).isTrue();
    }

    @Test(groups = {WARP_SPEED_HIVE_2, PROFILE_SPECIFIC_TESTS})
    public void testShowStats()
    {
        onTrino().executeQuery(format("ANALYZE %s.%s.%s", CATALOG_NAME, SCHEMA_NAME, TABLE_NAME));
        List<ColumnStatistics> allColumnsAfterAnalyze = getStatisitics(); //after running Analyze
        Optional<Double> rowCounts = getRowCountFromStatistics(allColumnsAfterAnalyze);
        Double expectedRowCount = queryUtils.getTableRowCount(CATALOG_NAME, SCHEMA_NAME, TABLE_NAME);
        rowCounts.ifPresent(aDouble -> assertThat(aDouble).as(format("Expected %s rows but received %f",
                expectedRowCount, aDouble)).isEqualTo(expectedRowCount));

        this.validateStatsPerColumn(TABLE_COLUMN_INT, allColumnsAfterAnalyze);
        this.validateStatsPerColumn(TABLE_COLUMN_BOOL, allColumnsAfterAnalyze);
    }

    private List<ColumnStatistics> getStatisitics()
    {
        List<ColumnStatistics> allColumns = new ArrayList<>();
        QueryResult queryResult = onTrino().executeQuery(format("SHOW STATS FOR (SELECT * FROM %s.%s.%s)", CATALOG_NAME, SCHEMA_NAME, TABLE_NAME));
        logger.debug("queryResult=%s", queryResult.rows());
        int rowsCount = queryResult.getRowsCount();
        logger.debug("Row count=%d", rowsCount);
        for (int rowNumber = 0; rowNumber < rowsCount; rowNumber++) {
            ColumnStatistics colStats = new ColumnStatistics(
                    getStringFromObject(queryResult.column(ColumnStatisticsNames.COLUMNNAME.ordinal() + 1).get(rowNumber)),
                    getDoubleFromObject(queryResult.column(ColumnStatisticsNames.DATASIZE.ordinal() + 1).get(rowNumber)),
                    getDoubleFromObject(queryResult.column(ColumnStatisticsNames.DISTINCTVALUESCOUNT.ordinal() + 1).get(rowNumber)),
                    getDoubleFromObject(queryResult.column(ColumnStatisticsNames.NULLSFRACTION.ordinal() + 1).get(rowNumber)),
                    getDoubleFromObject(queryResult.column(ColumnStatisticsNames.ROWCOUNT.ordinal() + 1).get(rowNumber)),
                    getDoubleFromObjectLikeString(queryResult.column(ColumnStatisticsNames.LOWVALUE.ordinal() + 1).get(rowNumber)),
                    getDoubleFromObjectLikeString(queryResult.column(ColumnStatisticsNames.HIGHVALUE.ordinal() + 1).get(rowNumber)));
            allColumns.add(colStats);
        }
        // Debug - printing all list
        for (ColumnStatistics cs : allColumns) {
            logger.debug("cs - %s", cs);
        }

        return allColumns;
    }

    private void validateStatsPerColumn(String columnName, List<ColumnStatistics> allColumns)
    {
        ColumnStatistics csColumn = getStatsByColumnName(columnName, allColumns);
        if (csColumn == null) {
            logger.error("We get null from column %s statistics", columnName);
            return;
        }
        logger.debug("%s column statistics: %s", columnName, csColumn);
        QueryResult queryResult = getColumnData(columnName);
        logger.debug("Column %s Data ==> %s", columnName, queryResult.rows());

        Set<Object> distinctValues = new HashSet<>();
        int rowsCount = queryResult.getRowsCount();
        int nNull = 0;
        for (int rowNumber = 0; rowNumber < rowsCount; rowNumber++) {
            Object dValue = queryResult.column(1).get(rowNumber);
            if (dValue != null) {
                distinctValues.add(dValue);
            }
            else {
                nNull++;
            }
        }
        logger.info("Column %s Distinct values ==> %s, size: %d, nulls: %d", columnName, distinctValues, distinctValues.size(), nNull);
        csColumn.distinctValuesCount.ifPresent(aDouble -> assertThat(Math.abs(aDouble - distinctValues.size()) < 1).isTrue());
        int finalNull = nNull;
        csColumn.nullsFraction.ifPresent(aDouble -> assertThat(Math.round(aDouble * rowsCount)).isEqualTo(finalNull));
    }

    private Optional<Double> getRowCountFromStatistics(List<ColumnStatistics> allColumns)
    {
        Optional<Double> rowCount = Optional.empty();

        for (ColumnStatistics cs : allColumns) {
            if (cs.columnName.isEmpty()) { //Expect only one row where columnName is null
                rowCount = cs.rowCount;
                break;
            }
        }
        return rowCount;
    }

    private ColumnStatistics getStatsByColumnName(String columnName, List<ColumnStatistics> allColumns)
    {
        for (ColumnStatistics cs : allColumns) {
            if (cs.columnName.isPresent() && Objects.equals(cs.columnName.orElseThrow(), columnName)) {
                return cs;
            }
        }
        return null;
    }

    private QueryResult getColumnData(String columnName)
    {
        return onTrino().executeQuery(format("SELECT %s FROM %s.%s.%s", columnName, CATALOG_NAME, SCHEMA_NAME, TABLE_NAME));
    }

    private Optional<String> getStringFromObject(Object convertObj)
    {
        if (convertObj == null) {
            return Optional.empty();
        }
        return Optional.of(convertObj.toString());
    }

    private Optional<Double> getDoubleFromObject(Object convertObj)
    {
        if (convertObj == null) {
            return Optional.empty();
        }
        return Optional.of((Double) convertObj);
    }

    private Optional<Double> getDoubleFromObjectLikeString(Object convertObj)
    {
        if (convertObj == null) {
            return Optional.empty();
        }
        return Optional.of(Double.parseDouble(convertObj.toString()));
    }
}
