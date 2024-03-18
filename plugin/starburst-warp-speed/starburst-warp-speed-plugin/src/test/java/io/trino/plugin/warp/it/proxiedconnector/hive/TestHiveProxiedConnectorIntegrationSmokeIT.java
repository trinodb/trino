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

package io.trino.plugin.warp.it.proxiedconnector.hive;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slices;
import io.trino.Session;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HiveTableHandle;
import io.trino.plugin.varada.VaradaSessionProperties;
import io.trino.plugin.varada.api.health.HealthResult;
import io.trino.plugin.varada.api.warmup.DateSlidingWindowWarmupPredicateRule;
import io.trino.plugin.varada.api.warmup.PartitionValueWarmupPredicateRule;
import io.trino.plugin.varada.api.warmup.WarmUpType;
import io.trino.plugin.varada.api.warmup.WarmupColRuleData;
import io.trino.plugin.varada.api.warmup.WarmupColRuleUsageData;
import io.trino.plugin.varada.api.warmup.WarmupPredicateRule;
import io.trino.plugin.varada.api.warmup.WarmupPropertiesData;
import io.trino.plugin.varada.api.warmup.WarmupRulesUsageData;
import io.trino.plugin.varada.api.warmup.column.RegularColumnData;
import io.trino.plugin.varada.api.warmup.column.WildcardColumnData;
import io.trino.plugin.varada.api.warmup.expression.TransformFunctionData;
import io.trino.plugin.varada.api.warmup.expression.VaradaPrimitiveConstantData;
import io.trino.plugin.varada.dictionary.DebugDictionaryMetadata;
import io.trino.plugin.varada.dispatcher.DispatcherConnectorFactory;
import io.trino.plugin.varada.dispatcher.DispatcherTableHandle;
import io.trino.plugin.varada.dispatcher.model.DictionaryInfo;
import io.trino.plugin.varada.dispatcher.model.DictionaryState;
import io.trino.plugin.varada.dispatcher.model.RegularColumn;
import io.trino.plugin.varada.dispatcher.model.RowGroupData;
import io.trino.plugin.varada.dispatcher.model.RowGroupKey;
import io.trino.plugin.varada.dispatcher.model.VaradaColumn;
import io.trino.plugin.varada.dispatcher.model.WarmUpElement;
import io.trino.plugin.varada.dispatcher.model.WarmUpElementState;
import io.trino.plugin.varada.execution.debugtools.WarmupDemoterWarmupElementData;
import io.trino.plugin.varada.expression.VaradaCall;
import io.trino.plugin.varada.expression.VaradaConstant;
import io.trino.plugin.varada.expression.VaradaExpression;
import io.trino.plugin.varada.expression.VaradaExpressionData;
import io.trino.plugin.varada.expression.VaradaSliceConstant;
import io.trino.plugin.varada.expression.VaradaVariable;
import io.trino.plugin.varada.expression.rewrite.WarpExpression;
import io.trino.plugin.varada.expression.rewrite.coordinator.connectortovarada.SupportedFunctions;
import io.trino.plugin.varada.juffer.PredicateBufferPoolType;
import io.trino.plugin.varada.storage.write.PageSink;
import io.trino.plugin.varada.util.FailureGeneratorInvocationHandler;
import io.trino.plugin.varada.warmup.WarmupRuleService;
import io.trino.plugin.warp.WarpPlugin;
import io.trino.plugin.warp.extension.execution.debugtools.FailureGeneratorResource;
import io.trino.plugin.warp.extension.execution.debugtools.PredicateCacheTask;
import io.trino.plugin.warp.extension.execution.debugtools.RowGroupCountResult;
import io.trino.plugin.warp.extension.execution.debugtools.RowGroupTask;
import io.trino.plugin.warp.extension.execution.debugtools.WarmupDemoterData;
import io.trino.plugin.warp.extension.execution.debugtools.WarmupDemoterThreshold;
import io.trino.plugin.warp.extension.execution.debugtools.WorkerWarmupDemoterTask;
import io.trino.plugin.warp.extension.execution.debugtools.dictionary.DictionaryConfigurationResult;
import io.trino.plugin.warp.extension.execution.debugtools.dictionary.DictionaryCountResult;
import io.trino.plugin.warp.extension.execution.debugtools.dictionary.DictionaryTask;
import io.trino.plugin.warp.extension.execution.health.ClusterHealthTask;
import io.trino.plugin.warp.extension.execution.health.HealthTask;
import io.trino.plugin.warp.extension.execution.warmup.WarmupTask;
import io.trino.plugin.warp.gen.constants.FailureRepetitionMode;
import io.trino.plugin.warp.gen.stats.VaradaStatsWarmupDemoter;
import io.trino.plugin.warp.it.DispatcherQueryRunner;
import io.trino.plugin.warp.it.DispatcherStubsIntegrationSmokeIT;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.SortedRangeSet;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.sql.planner.Plan;
import io.trino.sql.planner.optimizations.PlanNodeSearcher;
import io.trino.sql.planner.plan.ChooseAlternativeNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import io.varada.tools.util.Pair;
import io.varada.tools.util.StringUtils;
import jakarta.ws.rs.HttpMethod;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.StringJoiner;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.trino.plugin.varada.VaradaSessionProperties.ENABLE_DEFAULT_WARMING;
import static io.trino.plugin.varada.VaradaSessionProperties.ENABLE_DEFAULT_WARMING_INDEX;
import static io.trino.plugin.varada.VaradaSessionProperties.ENABLE_MAPPED_MATCH_COLLECT;
import static io.trino.plugin.varada.VaradaSessionProperties.PREDICATE_SIMPLIFY_THRESHOLD;
import static io.trino.plugin.varada.VaradaSessionProperties.UNSUPPORTED_FUNCTIONS;
import static io.trino.plugin.varada.VaradaSessionProperties.UNSUPPORTED_NATIVE_FUNCTIONS;
import static io.trino.plugin.varada.configuration.ProxiedConnectorConfiguration.HIVE_CONNECTOR_NAME;
import static io.trino.plugin.varada.configuration.ProxiedConnectorConfiguration.PROXIED_CONNECTOR;
import static io.trino.plugin.varada.dispatcher.DispatcherPageSourceFactory.PREFILLED;
import static io.trino.plugin.varada.dispatcher.DispatcherPageSourceFactory.createFixedStatKey;
import static io.trino.plugin.varada.dispatcher.model.DictionaryState.DICTIONARY_MAX_EXCEPTION;
import static io.trino.plugin.warp.extension.configuration.WarpExtensionConfiguration.USE_HTTP_SERVER_PORT;
import static io.trino.plugin.warp.extension.execution.dump.RowGroupDataDumpTask.CACHED_ROW_GROUP;
import static io.trino.plugin.warp.extension.execution.dump.RowGroupDataDumpTask.CACHED_SHARED_ROW_GROUP;
import static io.trino.spi.expression.StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.LIKE_FUNCTION_NAME;
import static io.varada.tools.configuration.MultiPrefixConfigurationWrapper.WARP_SPEED_PREFIX;
import static java.lang.String.format;
import static java.util.Map.entry;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHiveProxiedConnectorIntegrationSmokeIT
        extends DispatcherStubsIntegrationSmokeIT
{
    private static final String WIDE_TABLE_NAME = "wide";

    public TestHiveProxiedConnectorIntegrationSmokeIT()
    {
        super(1, "hive");
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return DispatcherQueryRunner.createQueryRunner(storageEngineModule,
                Optional.empty(),
                numNodes,
                Collections.emptyMap(),
                Map.of("http-server.log.enabled", "false",
                        "hive.s3.aws-access-key", "this is a fake key",
                        WARP_SPEED_PREFIX + USE_HTTP_SERVER_PORT, "false",
                        "node.environment", "varada",
                        WARP_SPEED_PREFIX + PROXIED_CONNECTOR, HIVE_CONNECTOR_NAME),
                hiveDir,
                DispatcherConnectorFactory.DISPATCHER_CONNECTOR_NAME,
                catalog,
                new WarpPlugin(),
                Map.of());
    }

    @Test
    public void testBucketedBy()
            throws IOException
    {
        try {
            computeActual(getSession(),
                    "CREATE TABLE bt(int_1 integer, bigint_2 bigint, smallint_3 smallint, tinyint_4 tinyint, char_5 char(9))" +
                            " WITH (format = 'PARQUET', bucketed_by = ARRAY['int_1'], bucket_count = 4)");

            // "warp-speed.config.dictionary.max-size" == "3" in DispatcherQueryRunner.createQueryRunner()
            IntStream.range(1, 4).forEach(value -> assertUpdate(format("INSERT INTO bt(int_1, bigint_2, smallint_3, tinyint_4, char_5) VALUES (%d, %d, %d, %d, '%09d')",
                    value, value, value, value, value), 1));

            createWarmupRules(DEFAULT_SCHEMA,
                    "bt",
                    Map.of("int_1", Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, DEFAULT_PRIORITY, DEFAULT_TTL),
                                    new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BASIC, DEFAULT_PRIORITY, DEFAULT_TTL)),
                            "bigint_2", Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, DEFAULT_PRIORITY, DEFAULT_TTL),
                                    new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BASIC, DEFAULT_PRIORITY, DEFAULT_TTL))));

            @Language("SQL") String query = "SELECT * FROM bt";
            warmAndValidate(query, false, 12 /* 2 cols * 2 types * 3 rows */, 3);

            Map<String, Long> expectedQueryStats = Map.of(
                    CACHED_TOTAL_ROWS, 3L,
                    VARADA_MATCH_COLUMNS_STAT, 0L,
                    VARADA_COLLECT_COLUMNS_STAT, 6L /* 2 cols * 3 rows */,
                    EXTERNAL_MATCH_STAT, 0L,
                    EXTERNAL_COLLECT_STAT, 9L /* 3 cols * 3 rows */);
            validateQueryStats(query, getSession(), expectedQueryStats);

            expectedQueryStats = Map.of(
                    CACHED_TOTAL_ROWS, 1L,
                    VARADA_MATCH_COLUMNS_STAT, 1L,
                    VARADA_COLLECT_COLUMNS_STAT, 1L,
                    PREFILLED_COLUMNS_STAT, 1L,
                    EXTERNAL_MATCH_STAT, 0L,
                    EXTERNAL_COLLECT_STAT, 3L);
            validateQueryStats("SELECT * FROM bt WHERE int_1 = 1", getSession(), expectedQueryStats);
        }
        finally {
            computeActual("DROP TABLE IF EXISTS bt");
        }
    }

    @Test
    public void testSinglePartitionMultipleSplits()
    {
        try {
            computeActual("CREATE TABLE pt(id integer, a varchar, date_date date) " +
                    "WITH (format='PARQUET', partitioned_by = ARRAY['date_date'])");
            IntStream.range(1, 9).forEach(value -> assertUpdate(format("INSERT INTO pt(id, a, date_date) VALUES(%d, 'a-%d',CAST('2020-04-0%d' AS date))", value, value, value), 1));

            MaterializedResult materializedRows = computeActual("SELECT count(a) FROM pt WHERE date_date=CAST('2020-04-01' AS date)");
            assertThat(materializedRows.getMaterializedRows().getFirst().getField(0)).isEqualTo(1L);
        }
        finally {
            computeActual("DROP TABLE IF EXISTS pt");
        }
    }

    @Test
    public void testMultiplePartitionsMultipleSplits()
            throws IOException
    {
        String table = "pt";
        String aCol = "a";
        String dateIntCol = "date_int";
        String dateDateCol = "date_date";
        try {
            computeActual(format("CREATE TABLE %s(%s varchar, %s integer, %s date) " +
                            "WITH (format='PARQUET', partitioned_by = ARRAY['%s', '%s'])",
                    table, aCol, dateIntCol, dateDateCol, dateIntCol, dateDateCol));

            IntStream.range(0, 2).forEach(indexDateInt -> IntStream.range(1, 3).forEach(indexDateDate -> assertUpdate(format("INSERT INTO %s(%s, %s, %s) VALUES('a-%d', 2019031%d, CAST('2020-04-%d%d' AS date))",
                            table, aCol, dateIntCol, dateDateCol, indexDateDate, indexDateInt, indexDateInt, indexDateDate),
                    1)));

            createWarmupRules(DEFAULT_SCHEMA,
                    table,
                    Map.of(aCol,
                            Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, 2, DEFAULT_TTL),
                                    new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BASIC, 2, DEFAULT_TTL)),
                            dateIntCol,
                            Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, 2, DEFAULT_TTL),
                                    new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BASIC, 2, DEFAULT_TTL)),
                            dateDateCol,
                            Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, 2, DEFAULT_TTL),
                                    new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BASIC, 2, DEFAULT_TTL))));

            warmAndValidate(format("select %s, %s, %s from %s", aCol, dateIntCol, dateDateCol, table),
                    Session.builder(getSession())
                            .setSystemProperty(catalog + "." + ENABLE_DEFAULT_WARMING, Boolean.FALSE.toString())
                            .build(),
                    24,
                    4,
                    Optional.empty());

            @Language("SQL") String query = format("SELECT %s, %s FROM %s WHERE %s=20190311 AND %s='a-1'",
                    aCol, dateIntCol, table, dateIntCol, aCol);
            Map<String, Long> expectedQueryStats = Map.of(
                    CACHED_TOTAL_ROWS, 2L,
                    VARADA_MATCH_COLUMNS_STAT, 2L,
                    VARADA_COLLECT_COLUMNS_STAT, 0L,
                    PREFILLED_COLUMNS_STAT, 4L,
                    EXTERNAL_MATCH_STAT, 0L,
                    EXTERNAL_COLLECT_STAT, 0L);
            int expectedSplits = 2;
            validateQueryStats(query, getSession(), expectedQueryStats, OptionalInt.of(expectedSplits));

            query = format("SELECT %s, %s FROM %s WHERE %s=20190311 AND %s=CAST('2020-04-12' AS date) AND %s='a-1'",
                    aCol, dateIntCol, table, dateIntCol, dateDateCol, aCol);
            expectedQueryStats = Map.of(
                    CACHED_TOTAL_ROWS, 1L,
                    VARADA_MATCH_COLUMNS_STAT, 1L,
                    VARADA_COLLECT_COLUMNS_STAT, 0L,
                    PREFILLED_COLUMNS_STAT, 2L,
                    EXTERNAL_MATCH_STAT, 0L,
                    EXTERNAL_COLLECT_STAT, 0L);
            expectedSplits = 1;
            validateQueryStats(query, getSession(), expectedQueryStats, OptionalInt.of(expectedSplits));
        }
        finally {
            computeActual("DROP TABLE IF EXISTS pt");
        }
    }

    @Test
    public void testSimple_withoutWarm_ReturnHive()
    {
        computeActual("INSERT INTO t VALUES (1, 'shlomi')");
        MaterializedResult materializedRows = computeActual(format("SELECT %s FROM t WHERE %s = 1", C2, C1));
        assertThat(materializedRows.getRowCount()).isEqualTo(1);
    }

    @Test
    public void test_CTAS()
    {
        computeActual("CREATE TABLE t2 AS SELECT * FROM t");
        computeActual("DROP TABLE t2");
    }

    @Test
    public void testWarmCharArray()
            throws IOException
    {
        computeActual("CREATE TABLE array_test (dummy ARRAY(CHAR(5)))");
        computeActual("INSERT INTO array_test values (ARRAY ['1','2','3','4'])");
        createWarmupRules(DEFAULT_SCHEMA,
                "array_test",
                Map.of("dummy", Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_LUCENE, DEFAULT_PRIORITY, DEFAULT_TTL))));
        Session session = buildSession(false, false);
        warmAndValidate("select * from array_test", session, 1, 1, 0);
        Map<String, Long> expectedJmxQueryStats = Map.of(
                "varada_match_columns", 1L,
                "external_collect_columns", 1L,
                "external_match_columns", 0L);
        validateQueryStats("select * from array_test where contains(dummy, '5')", session, expectedJmxQueryStats);
        expectedJmxQueryStats = Map.of(
                "varada_match_columns", 0L,
                "external_collect_columns", 1L,
                "external_match_columns", 0L);
        validateQueryStats("select * from array_test where element_at(dummy,1) = '1'", session, expectedJmxQueryStats);
        computeActual("DROP TABLE array_test");
    }

    @Test
    public void testWarmLuceneVarcharArray()
            throws IOException
    {
        computeActual("CREATE TABLE array_test (dummy ARRAY(VARCHAR(5)))");
        computeActual("INSERT INTO array_test values (ARRAY ['1','2','3','4'])");
        createWarmupRules(DEFAULT_SCHEMA,
                "array_test",
                Map.of("dummy", Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_LUCENE, DEFAULT_PRIORITY, DEFAULT_TTL))));

        Session session = buildSession(false, false);
        warmAndValidate("select * from array_test", session, 1, 1, 0);
        Map<String, Long> expectedJmxQueryStats = Map.of(
                "varada_match_columns", 1L,
                "external_match_columns", 0L);
        validateQueryStats("select * from array_test where contains(dummy, '1')", session, expectedJmxQueryStats);
        expectedJmxQueryStats = Map.of(
                "varada_match_columns", 0L,
                "external_match_columns", 0L);
        validateQueryStats("select * from array_test where element_at(dummy,1) = '1'", session, expectedJmxQueryStats);
        computeActual("DROP TABLE array_test");
    }

    @Test
    public void testWarmBooleanArray()
    {
        computeActual("CREATE TABLE array_test (dummy ARRAY(BOOLEAN))");
        computeActual("INSERT INTO array_test values (ARRAY [true, false, true, false])");
        Session session = buildSession(true, false);
        warmAndValidate("select * from array_test", session, 1, 1, 0);
        Map<String, Long> expectedJmxQueryStats = Map.of(
                "varada_match_columns", 0L,
                "external_match_columns", 0L);
        validateQueryStats("select * from array_test where contains(dummy, false)", session, expectedJmxQueryStats);
        computeActual("DROP TABLE array_test");
    }

    @Test
    public void testWarmDateArray()
    {
        computeActual("CREATE TABLE array_test (dummy ARRAY(DATE))");
        computeActual("INSERT INTO array_test values (ARRAY [DATE '2022-02-02'])");
        Session session = buildSession(true, false);
        warmAndValidate("select * from array_test", session, 1, 1, 0);
        Map<String, Long> expectedJmxQueryStats = Map.of(
                "varada_match_columns", 0L,
                "external_match_columns", 0L);
        validateQueryStats("select * from array_test where contains(dummy, CAST('2002-04-29' as date))", session, expectedJmxQueryStats);
        computeActual("DROP TABLE array_test");
    }

    @Test
    public void testWarmTimestampArray()
    {
        computeActual("CREATE TABLE array_test (dummy ARRAY(TIMESTAMP))");
        computeActual("INSERT INTO array_test values (ARRAY [current_timestamp])");
        Session session = buildSession(true, false);
        warmAndValidate("select * from array_test", session, 1, 1, 0);
        computeActual("DROP TABLE array_test");
    }

    @Test
    // TODO stuck in endless loop
    public void testRowDereference()
    {
        computeActual("CREATE TABLE evolve_test (dummy bigint, a row(b bigint, c varchar), d bigint)");
        computeActual("INSERT INTO evolve_test values (1, row(1, 'abc'), 1)");
        computeActual(getSession(), "select * from evolve_test where a[1] > 1");
        computeActual("DROP TABLE evolve_test");
    }

    @Test
    public void testSimple_Warm()
            throws IOException
    {
        computeActual(getSession(), "INSERT INTO t VALUES (1, 'shlomi')");

        createWarmupRules(DEFAULT_SCHEMA,
                "t",
                Map.of(C1, Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, DEFAULT_PRIORITY, DEFAULT_TTL)),
                        C2, Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BASIC, DEFAULT_PRIORITY, DEFAULT_TTL))));
        warmAndValidate("select * from t", false, 2, 1);
        RowGroupCountResult ret = getRowGroupCount();
        assertThat(ret.warmupColumnNames().size()).isEqualTo(2);

        Integer countC1 = ret.warmupColumnCount().entrySet()
                .stream()
                .filter(entry -> entry.getKey().endsWith(format("%s.%s", C1, WarmUpType.WARM_UP_TYPE_DATA)))
                .findAny()
                .orElseThrow()
                .getValue();
        assertThat(countC1).isGreaterThan(0);
        computeActual(getSession(), "select * from t where v1 <> 'afsa'");

        Session warmSession = Session.builder(getSession())
                .setSystemProperty(catalog + "." + "simplify_predicate_threshold", "1")
                .build();
        //validate simplified result
        computeActual(warmSession, "select count(*) from t where (int1<=-461887229 and int1>=-994082834) or (int1<=7777777 and int1>=0)");
    }

    @Test
    public void testUnsupportedArrayOperation()
    {
        computeActual("CREATE TABLE t0(varcharColumn varchar, arrayColumn array(varchar))");
        computeActual("INSERT INTO t0 values ('a', ARRAY ['1','2','3','4'])");
        warmAndValidate("SELECT arrayColumn from t0 WHERE arrayColumn = ARRAY['a', 'b']", true, 1, 1); //only DATA
        warmAndValidate("SELECT * from t0 WHERE arrayColumn = ARRAY['a', 'b'] OR varcharColumn='a'", true, 1, 1); //DATA for varcharColumn

        Map<String, Long> expectedQueryStats = Map.of(
                "varada_collect_columns", 2L,
                "external_collect_columns", 0L,
                "varada_match_columns", 0L,
                "external_match_columns", 0L);
        validateQueryStats("SELECT * from t0 WHERE arrayColumn = ARRAY['a', 'b'] OR varcharColumn='a'", getSession(), expectedQueryStats);
        validateQueryStats("SELECT * from t0 WHERE contains(split('%, NULL', ', '), varcharColumn)", getSession(), expectedQueryStats);
    }

    /**
     * Trino doesn't pushdown element_at predicate for  map[key] = value predicate.
     * currently we don't support composite map expression
     */
    @Test
    public void testCompositeMap()
    {
        assertUpdate(" CREATE TABLE maps_table (" +
                "int1 integer," +
                "map_column_integer map(integer, map(integer, integer))" +
                ")  WITH (format='PARQUET', partitioned_by = ARRAY[])");
        computeActual("INSERT INTO maps_table " +
                "VALUES " +
                "  (1, " +
                "MAP(ARRAY[1], ARRAY[MAP(ARRAY[(2)], ARRAY[(3)])])" +
                "  )");
        @Language("SQL") String query = "select int1 from maps_table where element_at(map_column_integer[1], 2) = 3";
        warmAndValidate(query, true, 1, 1);
        Map<String, Long> expectedQueryStats = Map.of(
                "varada_collect_columns", 1L,
                "external_collect_columns", 1L,
                "varada_match_columns", 0L,
                "external_match_columns", 0L);
        validateQueryStats(query, getSession(), expectedQueryStats);
        query = "select int1 from maps_table where element_at(element_at(map_column_integer,1),2) = 3";
        validateQueryStats(query, getSession(), expectedQueryStats);
    }

    @Test
    public void testMapMultipleMapTypes()
            throws IOException
    {
        assertUpdate(" CREATE TABLE maps_table (" +
                "int1 integer," +
                "varchar1 varchar, " +
                "smallint_integer map(smallint, integer)," +
                "integer_real map(integer, real)," +
                "real_integer map(real, integer)," +
                "integer_smallint map(integer, smallint)," +
                "varchar_double map(varchar, double)," +
                "varchar_integer map(varchar, integer)," +
                "varchar_varchar map(varchar, varchar)," +
                "integer_varchar map(integer, varchar)," +
                "integer_bigint map(integer, bigint)," +
                "integer_array_not_support map(integer, ARRAY(integer)), " +
                "bigint_decimal_not_support map(bigint, decimal(38,0))" +
                ")  WITH (format='PARQUET', partitioned_by = ARRAY[])");
        computeActual("INSERT INTO maps_table " +
                "VALUES " +
                "  (1, " +
                "   'bla', " +
                "   MAP(ARRAY[1, 2], ARRAY[1, 2]), " +
                "   MAP(ARRAY[1], ARRAY[10.3e0]), " +
                "   MAP(ARRAY[10.3e0], ARRAY[1]), " +
                "   MAP(ARRAY[1, 2], ARRAY[1, 2]), " +
                "   MAP(ARRAY['key1', 'key2'], ARRAY[1.0, 2.0]), " +
                "   MAP(ARRAY['key1', 'key2', 'key3'], ARRAY[null, 2, 3]), " +
                "   MAP(ARRAY['key7', 'key3', 'key9'], ARRAY['val7', 'val3', 'val9' ]), " +
                "   MAP(ARRAY[1, 2], ARRAY['val1', 'val2']), " +
                "   MAP(ARRAY[1, 2, 13], ARRAY[1000000000000, 2000000000000, 3000000000000]), " +
                "   MAP(ARRAY[1], ARRAY[ARRAY[2]])," +
                "   MAP(ARRAY[4080364000929075947], ARRAY[-8256833086]) " +
                "  ), " +
                "  (1, " +
                "   'bla', " +
                "   MAP(ARRAY[1, 2], ARRAY[1, 2]), " +
                "   MAP(ARRAY[1], ARRAY[10.3e0]), " +
                "   MAP(ARRAY[10.3e0], ARRAY[1]), " +
                "   MAP(ARRAY[1, 2], ARRAY[1, 2]), " +
                "   MAP(ARRAY['key1'], ARRAY[3.0]), " +
                "   MAP(ARRAY['key4', 'key5', 'key6'], ARRAY[4, 5, 6]), " +
                "   MAP(ARRAY['key7', 'key3', 'key9'], ARRAY['val7', 'val3', 'val9' ]), " +
                "   MAP(ARRAY[4, 5, 6], ARRAY['val4', 'val5', 'val6']), " +
                "   MAP(ARRAY[4, 5, 6], ARRAY[4000000000000, 5000000000000, 6000000000000]), " +
                "   MAP(ARRAY[1], ARRAY[ARRAY[2]])," +
                "   MAP(ARRAY[4080364000929075947], ARRAY[-8256833086]) " +
                "  ), " +
                "  (1, " +
                "   'bla', " +
                "   MAP(ARRAY[1, 2], ARRAY[1, 2]), " +
                "   MAP(ARRAY[1], ARRAY[10.3e0]), " +
                "   MAP(ARRAY[10.3e0], ARRAY[1]), " +
                "   MAP(ARRAY[1, 2, 5], ARRAY[3, 2, 6]), " +
                "   MAP(ARRAY['key1', 'key3'], ARRAY[1.0, 3.0]), " +
                "   MAP(ARRAY['key7', 'key3', 'key9'], ARRAY[7, 3, 9]), " +
                "   MAP(ARRAY['key7', 'key3', 'key9'], ARRAY['val7', 'val3', 'val9' ]), " +
                "   MAP(ARRAY[8, 9, 3], ARRAY['val8', 'val9', 'val3']), " +
                "   MAP(ARRAY[7, 8, 9], ARRAY[7000000000000, 8000000000000, 9000000000000]), " +
                "   MAP(ARRAY[1], ARRAY[ARRAY[2]]), " +
                "   MAP(ARRAY[4080364000929075947], ARRAY[-8256833086]) " +
                "  ), " +
                "  (1, " +
                "   'bla', " +
                "   MAP(ARRAY[1, 2], ARRAY[1, 2]), " +
                "   MAP(ARRAY[1], ARRAY[10.3e0]), " +
                "   MAP(ARRAY[10.3e0], ARRAY[1]), " +
                "   MAP(ARRAY[1, 2], ARRAY[1, 3]), " +
                "   MAP(ARRAY['key5'], ARRAY[5.0]), " +
                "   MAP(ARRAY['key10', 'key11', 'key12'], ARRAY[10, 11, 12]), " +
                "   MAP(ARRAY['key7', 'key3', 'key9'], ARRAY['val7', 'val3', 'val9' ]), " +
                "   MAP(ARRAY[10, 3, 11], ARRAY['val10', 'val12', 'val11']), " +
                "   MAP(ARRAY[10, 11, 12], ARRAY[10000000000000, 11000000000000, 12000000000000]), " +
                "   MAP(ARRAY[1], ARRAY[ARRAY[2]]), " +
                "   MAP(ARRAY[4080364000929075947], ARRAY[-8256833086]) " +
                "  ), " +
                "  (1, " +
                "   'bla', " +
                "   MAP(ARRAY[1, 2], ARRAY[1, 2]), " +
                "   MAP(ARRAY[1], ARRAY[10.3e0]), " +
                "   MAP(ARRAY[10.3e0], ARRAY[1]), " +
                "   MAP(ARRAY[1, 4], ARRAY[1, 4]), " +
                "   MAP(ARRAY['key1', 'key3', 'key9'], ARRAY[1.0, 3.0, 9.0]), " +
                "   MAP(ARRAY['key13', 'key14', 'key15'], ARRAY[13, 14, 15]), " +
                "   MAP(ARRAY['key7', 'key3', 'key9'], ARRAY['val7', 'val3', 'val9' ]), " +
                "   MAP(ARRAY[13, 14, 1], ARRAY[null, 'val14', 'val15']), " +
                "   MAP(ARRAY[13, 14, 15], ARRAY[13000000000000, 14000000000000, 15000000000000]), " +
                "   MAP(ARRAY[1], ARRAY[ARRAY[2]]), " +
                "   MAP(ARRAY[4080364000929075947], ARRAY[-8256833086]) " +
                "  )");
        List<String> elementAtPredicates = List.of(
                "element_at(integer_bigint, 3) > 100000 and element_at(integer_bigint, 3) = 100001",
                "element_at(varchar_varchar, 'key3') = 'val3' or element_at(varchar_varchar, 'key3') = 'val7'",
                "element_at(varchar_double, 'key1') = 1.0",
                "element_at(integer_real, 1) = real '10.3e0'",
                "element_at(real_integer, real '10.3e0') = 1",
                "element_at(varchar_varchar, 'key9') = 'val9'",
                "element_at(smallint_integer, smallint '1') = 2",
                "element_at(integer_smallint, 1) = 1",
                "element_at(varchar_integer, 'key2') = 2",
                "element_at(integer_bigint, 13) = 13000000000000",
                "element_at(integer_bigint, 1) > 100000",
                "element_at(integer_bigint, 2) > 100000 or element_at(integer_bigint, 2) = 10",
                "element_at(integer_varchar,  3) = 'val3'",
                "element_at(varchar_integer, 'key13') <= 13 or element_at(varchar_integer, 'key13') = 2",
                "element_at(integer_varchar, 1) = 'val1'");
        for (String elementAtPredicate : elementAtPredicates) {
            @Language("SQL") String query = format("select count(*) from maps_table where %s", elementAtPredicate);
            warmAndValidate(query, true, 1, 1);
            Map<String, Long> expectedQueryStats = Map.of(
                    "external_collect_columns", 1L,
                    "varada_match_columns", 1L,
                    "external_match_columns", 0L);
            validateQueryStats(query, getSession(), expectedQueryStats);
        }

        //'key9' is warmed, 'key1' not
        @Language("SQL") String query = "select count(*) from maps_table where element_at(varchar_varchar, 'key9') = 'val9' and element_at(varchar_varchar, 'key1') = 'val1'";
        Map<String, Long> expectedQueryStats = Map.of(
                "external_collect_columns", 1L,
                "varada_match_columns", 1L,
                "external_match_columns", 0L);
        validateQueryStats(query, getSession(), expectedQueryStats);
        query = "select count(*) from maps_table where element_at(varchar_varchar, 'c0sy8It%$R') = varchar1";
        //2 columns in a leaf is not supported
        expectedQueryStats = Map.of(
                "external_collect_columns", 2L,
                "varada_match_columns", 0L,
                "external_match_columns", 0L);
        validateQueryStats(query, getSession(), expectedQueryStats);

        query = "select count(*) from maps_table where element_at(varchar_varchar, 'c0sy8It%$R') = varchar1 or element_at(integer_varchar,  3) = 'val3'";
        //2 columns in a leaf is not supported
        expectedQueryStats = Map.of(
                "external_collect_columns", 3L,
                "varada_match_columns", 0L,
                "external_match_columns", 0L);

        validateQueryStats(query, getSession(), expectedQueryStats);

        query = "select count(*) from maps_table where element_at(varchar_varchar, 'c0sy8It%$R') = varchar1 and element_at(integer_varchar,  3) = 'val3'";
        expectedQueryStats = Map.of(
                "external_collect_columns", 3L,
                "varada_match_columns", 1L,
                "external_match_columns", 0L);
        validateQueryStats(query, getSession(), expectedQueryStats);

        // both keys ('key13', 'key2') are warmed
        query = "select count(*) from maps_table where element_at(varchar_integer, 'key13') = 13 and element_at(varchar_integer, 'key9') = 2";
        expectedQueryStats = Map.of(
                "external_collect_columns", 1L,
                "varada_match_columns", 1L,
                "external_match_columns", 0L);
        validateQueryStats(query, getSession(), expectedQueryStats);

        //2 warmed WE on the same column with or between them
        query = "select count(*) from maps_table where element_at(varchar_integer, 'key13') = 13 or element_at(varchar_integer, 'key2') = 2";
        expectedQueryStats = Map.of(
                "external_collect_columns", 1L,
                "varada_match_columns", 1L,
                "external_match_columns", 0L);
        validateQueryStats(query, getSession(), expectedQueryStats);

        //only 1 warmed WE - external_match increased
        query = "select count(*) from maps_table where element_at(varchar_integer, 'key13') = 13 or element_at(varchar_integer, 'key_not_warmed') = 2";
        expectedQueryStats = Map.of(
                "external_collect_columns", 1L,
                "varada_match_columns", 0L,
                "external_match_columns", 1L);
        validateQueryStats(query, getSession(), expectedQueryStats);

        query = "select count(*) from maps_table where element_at(varchar_varchar,  'key5') = 'val5'";
        Session session = Session.builder(getSession())
                .setSystemProperty(catalog + "." + ENABLE_DEFAULT_WARMING, "true")
                .setSystemProperty(catalog + "." + ENABLE_DEFAULT_WARMING_INDEX, "true")
                .build();
        warmAndValidate(query, session, 1, 1, 0);
        expectedQueryStats = Map.of(
                "varada_match_columns", 1L,
                "external_collect_columns", 1L,
                "external_match_columns", 0L);
        validateQueryStats(query, session, expectedQueryStats);
        query = "select int1, varchar_integer from maps_table"; //validate that we don't warm mapColumn without supported function in predicate. int1 will warm DATA + BASIC
        warmAndValidate(query, session, 2, 1, 0);

        query = "select int1, integer_array_not_support from maps_table where element_at(integer_array_not_support, 1) = array[1]";
        expectedQueryStats = Map.of(
                "varada_match_columns", 0L,
                "varada_collect_columns", 1L,
                "external_collect_columns", 1L,
                "external_match_columns", 0L);
        validateQueryStats(query, session, expectedQueryStats);

        query = "select bigint_decimal_not_support from maps_table where element_at(bigint_decimal_not_support, 4080364000929075947) = 5";
        expectedQueryStats = Map.of(
                "varada_match_columns", 0L,
                "varada_collect_columns", 0L,
                "external_collect_columns", 1L,
                "external_match_columns", 0L);
        validateQueryStats(query, session, expectedQueryStats);

        String cachedRowGroupsStr = executeRestCommand(CACHED_ROW_GROUP, "", null, HttpMethod.GET, HttpURLConnection.HTTP_OK);
        assertThat(cachedRowGroupsStr.indexOf("TransformedColumn")).isNotNegative();
        RowGroupCountResult rowGroupCount = getRowGroupCount();
        List<String> expectedWarmedColumns = List.of("integer_bigint.ELEMENT_AT.13",
                "integer_bigint.ELEMENT_AT.2",
                "real_integer.ELEMENT_AT.1092930765",
                "varchar_varchar.ELEMENT_AT.key5",
                "varchar_varchar.ELEMENT_AT.key9");
        for (String expectedWarmedColumn : expectedWarmedColumns) {
            assertThat(rowGroupCount.warmupColumnNames()).contains("schema.maps_table." + expectedWarmedColumn + ".WARM_UP_TYPE_BASIC");
        }

        query = "select count(*) from maps_table where int1 = element_at(varchar_integer, 'key1')";
        expectedQueryStats = Map.of(
                "varada_collect_columns", 1L,
                "varada_match_columns", 0L,
                "external_match_columns", 0L,
                "external_collect_columns", 1L);
        validateQueryStats(query, getSession(), expectedQueryStats);
    }

    @Test
    void testTransformedWarmupRule()
            throws IOException
    {
        computeActual(getSession(), "INSERT INTO t VALUES (1, 'shlomi')");

        createWarmupRules(DEFAULT_SCHEMA,
                "t",
                Map.of(C1, Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, DEFAULT_PRIORITY, DEFAULT_TTL)),
                        C2, Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BASIC, DEFAULT_PRIORITY, DEFAULT_TTL,
                                new TransformFunctionData(TransformFunctionData.TransformType.LOWER)))));

        @Language("SQL") String query = "select * from t where lower(v1) = 'shlomi'";
        warmAndValidate(query, false, 2, 1);

        Map<String, Long> expectedQueryStats = Map.of(
                "varada_collect_columns", 1L,
                "external_collect_columns", 1L,
                "varada_match_columns", 1L,
                "external_match_columns", 0L);
        validateQueryStats(query, getSession(), expectedQueryStats);

        query = "select v1 from t where lower(v1) = 'shlomi'";
        expectedQueryStats = Map.of(
                "varada_collect_columns", 0L,
                "external_collect_columns", 1L,
                "varada_match_columns", 1L,
                "external_match_columns", 0L);
        validateQueryStats(query, getSession(), expectedQueryStats);
    }

    private static Stream<Arguments> provideJsonExtractScalarTestParams()
    {
        return Stream.of(
                Arguments.arguments(2, 2, 1, 1, "json_extract_scalar(varchar1, '$.middle') = 'K'"),
                Arguments.arguments(2, 2, 1, 1, "json_extract_scalar(varchar1, '$.id') > '1001'"),
                Arguments.arguments(2, 2, 1, 1, "json_extract_scalar(varchar1, '$.value') = 'null'"),
                Arguments.arguments(2, 2, 1, 1, "json_extract_scalar(varchar2, '$.children[1].age') < '12'"),
                Arguments.arguments(2, 2, 1, 1, "json_extract_scalar(varchar1, '$.people[0].name') in ('John Smith', 'John Johnson')"),
                Arguments.arguments(2, 2, 1, 1, "json_extract_scalar(varchar1, '$.flag') in ('true', 'false')"),
                Arguments.arguments(2, 2, 1, 1, "json_extract_scalar(varchar2, '$.number') in ('12.345678', '98.7654321')"),
                Arguments.arguments(3, 2, 1, 1, "json_extract_scalar(varchar1, '$.id') = '1001' and json_extract_scalar(varchar1, '$.type') = 'Regular'"),
                Arguments.arguments(3, 2, 1, 1, "json_extract_scalar(varchar1, '$.first') = 'John' or json_extract_scalar(varchar1, '$.people[0].name') = 'Sally Brown'"),
                // expression: Call[functionName=name='$equal', arguments=[Call[functionName=name='json_extract_scalar', arguments=[varchar1::varchar, $.value::JsonPath]], Call[functionName=name='json_extract_scalar', arguments=[varchar1::varchar, $.id::JsonPath]]]]
                // More than one predicate appears in the expression, not supported (ExpressionService.getColumnHandle)
                Arguments.arguments(1, 1, 1, 0, "json_extract_scalar(varchar1, '$.value') = json_extract_scalar(varchar1, '$.id')"),
                // expression: Call[functionName=name='$equal', arguments=[Call[functionName=name='json_extract_scalar', arguments=[varchar1::varchar, $.value::JsonPath]], Call[functionName=name='json_extract_scalar', arguments=[varchar2::varchar, $.value::JsonPath]]]]
                // More than one predicate appears in the expression, not supported (ExpressionService.getColumnHandle)
                Arguments.arguments(2, 1, 2, 0, "json_extract_scalar(varchar1, '$.value') = json_extract_scalar(varchar2, '$.value')"),
                // expression: Call[functionName=name='$not_equal', arguments=[Call[functionName=name='json_extract_scalar', arguments=[Call[functionName=name='$cast', arguments=[varchar2::varchar]], $.father::JsonPath]], Slice[hash=-296188857,length=4]::varchar]]
                // CAST to JSON is not supported (CastRewriter.supportedCastTypes)
                Arguments.arguments(1, 1, 1, 0, "json_extract_scalar(cast(varchar2 as json), '$.father') <> 'John'"));
    }

    @ParameterizedTest
    @MethodSource("provideJsonExtractScalarTestParams")
    void testJsonExtractScalar(int expectedWarmupElements, int expectedWarmFinished, long varadaCollectColumns, long varadaMatchColumns, String jsonExtractScalarPredicate)
    {
        assertUpdate(" CREATE TABLE json_test_table (" +
                "varchar1 varchar, " +
                "varchar2 varchar" +
                ") WITH (format='PARQUET', partitioned_by = ARRAY[])");
        computeActual("INSERT INTO json_test_table " +
                "VALUES " +
                "  ( '{ \"first\" : \"John\" , \"middle\" : \"K\", \"last name\" : \"Doe\" }', '{ \"father\": \"John\", \"mother\": \"Mary\", \"children\": [ { \"age\": 12 }, { \"age\": 10 } ] }' ), " +
                "  ( null, '{ \"father\": \"Paul\", \"mother\": \"Laura\", \"children\": [{\"age\": 9},{\"age\": 3}] }' ), " +
                "  ( '{ \"people\": [ { \"name\":\"John Smith\" }, { \"name\":\"Sally Brown\" }, { \"name\":\"John Johnson\" } ] }', null ), " +
                "  ( '{ \"id\": \"1001\", \"type\": \"Regular\", \"flag\": true, \"value\": null }', '{ \"id\": \"5001\", \"type\": \"None\", \"number\": 12.345678 }' )");

        @Language("SQL") String query = format("select count(*) from json_test_table where %s", jsonExtractScalarPredicate);
        warmAndValidate(query, true, expectedWarmupElements, expectedWarmFinished);

        Map<String, Long> expectedQueryStats = Map.of(
                "varada_collect_columns", varadaCollectColumns,
                "external_collect_columns", 0L,
                "varada_match_columns", varadaMatchColumns,
                "external_match_columns", 0L);
        validateQueryStats(query, getSession(), expectedQueryStats);
    }

    @Test
    void testJsonExtractScalar()
            throws IOException
    {
        assertUpdate(" CREATE TABLE json_test_table (" +
                "varchar1 varchar, " +
                "varchar2 varchar" +
                ") WITH (format='PARQUET', partitioned_by = ARRAY[])");
        computeActual("INSERT INTO json_test_table " +
                "VALUES " +
                "  ( '{ \"first\" : \"John\" , \"middle\" : \"K\", \"last name\" : \"Doe\" }', '{ \"father\": \"John\", \"mother\": \"Mary\", \"children\": [ { \"age\": 12 }, { \"age\": 10 } ] }' ), " +
                "  ( null, '{ \"father\": \"Paul\", \"mother\": \"Laura\", \"children\": [{\"age\": 9},{\"age\": 3}] }' ), " +
                "  ( '{ \"people\": [ { \"name\":\"John Smith\" }, { \"name\":\"Sally Brown\" }, { \"name\":\"John Johnson\" } ] }', null ), " +
                "  ( '{ \"id\": \"1001\", \"type\": \"Regular\", \"flag\": true, \"value\": null }', '{ \"id\": \"5001\", \"type\": \"None\", \"number\": 12.345678 }' )");

        createWarmupRules(DEFAULT_SCHEMA,
                "json_test_table",
                Map.ofEntries(
                        entry("varchar1", Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BASIC, DEFAULT_PRIORITY, DEFAULT_TTL,
                                new TransformFunctionData(TransformFunctionData.TransformType.JSON_EXTRACT_SCALAR,
                                        ImmutableList.of(new VaradaPrimitiveConstantData("$.middle", VaradaPrimitiveConstantData.Type.VARCHAR)))))),
                        entry("varchar2", Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BASIC, DEFAULT_PRIORITY, DEFAULT_TTL,
                                new TransformFunctionData(TransformFunctionData.TransformType.JSON_EXTRACT_SCALAR,
                                        ImmutableList.of(new VaradaPrimitiveConstantData("$.children[1].age", VaradaPrimitiveConstantData.Type.VARCHAR))))))));

        @Language("SQL") String query = "select count(*) from json_test_table " +
                "where json_extract_scalar(varchar1, '$.middle') = 'K' " +
//                "and json_extract_scalar(varchar1, '$.id') = '1001' " +
                "and json_extract_scalar(varchar2, '$.children[1].age') = '12'";
        warmAndValidate(query, false, 2, 1);

        Map<String, Long> expectedQueryStats = Map.of(
                "varada_collect_columns", 0L,
                "external_collect_columns", 2L,
                "varada_match_columns", 2L,
                "external_match_columns", 0L);
        validateQueryStats(query, getSession(), expectedQueryStats);
    }

    @Test
    void testJsonExtractScalarFunction()
    {
        assertUpdate(" CREATE TABLE json_test_table (" +
                "varchar1 varchar, " +
                "varchar2 varchar" +
                ") WITH (format='PARQUET', partitioned_by = ARRAY[])");
        computeActual("INSERT INTO json_test_table " +
                "VALUES " +
                "  ( '{ \"first\" : \"John\" , \"middle\" : \"K\", \"last name\" : \"Doe\" }', '{ \"father\": \"John\", \"mother\": \"Mary\", \"children\": [ { \"age\": 12 }, { \"age\": 10 } ] }' ), " +
                "  ( null, '{ \"father\": \"Paul\", \"mother\": \"Laura\", \"children\": [{\"age\": 9},{\"age\": 3}] }' ), " +
                "  ( '{ \"people\": [ { \"name\":\"John Smith\" }, { \"name\":\"Sally Brown\" }, { \"name\":\"John Johnson\" } ] }', null ), " +
                "  ( '{ \"id\": \"1001\", \"type\": \"Regular\", \"flag\": true, \"value\": null }', '{ \"id\": \"5001\", \"type\": \"None\", \"number\": 12.345678 }' )");

        @Language("SQL") String query = "select * from json_test_table";
        warmAndValidate(query, true, 2, 1);

        // expression: Call[functionName=name='$equal', arguments=[Call[functionName=name='json_extract_scalar', arguments=[varchar1::varchar, Call[functionName=name='$cast', arguments=[Slice[hash=1773388407,length=13]::varchar(13)]]]], Slice[hash=-122647857,length=3]::varchar]]
        // Call must contain Variable, and we don't support 2 Variables in a single predicate (GenericRewriter.rewrite)
        query = "select count(*) from json_test_table where json_extract_scalar(varchar1, '$.\"last name\"') = 'Doe'";
        Map<String, Long> expectedQueryStats = Map.of(
                "varada_collect_columns", 1L,
                "external_collect_columns", 0L,
                "varada_match_columns", 0L,
                "external_match_columns", 0L);
        validateQueryStats(query, getSession(), expectedQueryStats);
    }

    /**
     * default warming:
     * for string values - prefer basic over Lucene
     * for string ranges - prefer Lucene over Basic
     * query decision:
     * for string values - prefer basic over Lucene, but use lucene if we don’t have basic.
     * for string ranges - prefer Lucene over Basic,  but use basic if we don’t have Lucene.
     */
    @Test
    public void testDefaultWarmingWithQueriesForVarchar()
            throws IOException
    {
        computeActual(getSession(), "INSERT INTO t VALUES (1, 'shlomi')");
        List<String> valuesQueries = List.of(
                "select v1 from t where v1 <> 'shlomi'",
                "select v1 from t where v1 = 'shlomi'",
                "select v1 from t where v1 in ('shlomi', 'tzachi')",
                "select v1 from t where v1 = 'shlomi' or v1 = 'tzachi'");
        Session session = Session.builder(getSession())
                .setSystemProperty(catalog + "." + ENABLE_DEFAULT_WARMING, "false")
                .build();
        for (@Language("SQL") String query : valuesQueries) {
            warmAndValidate(query, true, 2, 1);
            List<io.trino.plugin.warp.gen.constants.WarmUpType> warmedElementTypes = getWarmedElements();
            assertThat(warmedElementTypes).containsExactlyInAnyOrder(
                    io.trino.plugin.warp.gen.constants.WarmUpType.WARM_UP_TYPE_BASIC,
                    io.trino.plugin.warp.gen.constants.WarmUpType.WARM_UP_TYPE_DATA);
            boolean filter = query.contains("in") || query.contains("or") || query.contains("<>");
            long prefilledCollectColumns = filter ? 0 : 1;
            long collectColumns = filter ? 1 : 0;
            Map<String, Long> expectedQueryStats = Map.of(
                    "external_match_columns", 0L,
                    "external_collect_columns", 0L,
                    "prefilled_collect_columns", prefilledCollectColumns,
                    "varada_match_columns", 1L,
                    "varada_collect_columns", collectColumns);
            int expectedLuceneReadColumns = 0;
            validateQueryStats(query, session, expectedQueryStats, expectedLuceneReadColumns);
            executeRestCommand(RowGroupTask.ROW_GROUP_PATH, RowGroupTask.ROW_GROUP_RESET_TASK_NAME, null, HttpMethod.POST, HttpURLConnection.HTTP_NO_CONTENT);
            warmedElementTypes = getWarmedElements();
            assertThat(warmedElementTypes).isEmpty();
        }

        List<String> rangeQueries = List.of(
                "select v1 from t where v1 > 'shlomi'",
                "select v1 from t where v1 >= 'shlomi'",
                "select v1 from t where v1 < 'shlomi'",
                "select v1 from t where v1 >= 'shlomi'");
        for (@Language("SQL") String query : rangeQueries) {
            warmAndValidate(query, true, 2, 1);
            List<io.trino.plugin.warp.gen.constants.WarmUpType> warmedElementTypes = getWarmedElements();
            assertThat(warmedElementTypes).containsExactlyInAnyOrder(
                    io.trino.plugin.warp.gen.constants.WarmUpType.WARM_UP_TYPE_LUCENE,
                    io.trino.plugin.warp.gen.constants.WarmUpType.WARM_UP_TYPE_DATA);
            Map<String, Long> expectedQueryStats = Map.of(
                    "external_match_columns", 0L,
                    "external_collect_columns", 0L,
                    "prefilled_collect_columns", 0L,
                    "varada_match_columns", 1L,
                    "varada_collect_columns", 1L);
            int expectedLuceneReadColumns = 1;
            validateQueryStats(query, session, expectedQueryStats, expectedLuceneReadColumns);

            executeRestCommand(RowGroupTask.ROW_GROUP_PATH, RowGroupTask.ROW_GROUP_RESET_TASK_NAME, null, HttpMethod.POST, HttpURLConnection.HTTP_NO_CONTENT);
            warmedElementTypes = getWarmedElements();
            assertThat(warmedElementTypes).isEmpty();
        }
    }

    @Test
    public void testFilterRanges()
            throws IOException
    {
        assertUpdate("CREATE TABLE filterRanges(" +
                "double1 double, " +
                "double2 double, " +
                "n_long_decimal decimal(30, 2), " +
                "s_real real, " +
                "bingint1 bigint, " +
                "int_1 integer,  " +
                "date_col date, " +
                "timestamp_col TIMESTAMP(3), " +
                "small_int_col smallint, " +
                "tiny_int_col TINYINT, " +
                "varchar_col varchar, " +
                "varchar_10_col varchar(10), " +
                "char_8_col char(8), " +
                "not_warm_double double) WITH (format='PARQUET', partitioned_by = ARRAY[])");

        computeActual(getSession(), "INSERT INTO filterRanges(int_1, tiny_int_col, small_int_col, varchar_col) VALUES(1 ,2, 3, '2'), (NULL, NULL, NULL, NULL)");
        createWarmupRules(DEFAULT_SCHEMA,
                "filterRanges",
                Map.ofEntries(
                        entry("int_1", Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BASIC, DEFAULT_PRIORITY, DEFAULT_TTL),
                                new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, DEFAULT_PRIORITY, DEFAULT_TTL))),
                        entry("tiny_int_col", Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BASIC, DEFAULT_PRIORITY, DEFAULT_TTL),
                                new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, DEFAULT_PRIORITY, DEFAULT_TTL))),
                        entry("small_int_col", Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, DEFAULT_PRIORITY, DEFAULT_TTL))),
                        entry("varchar_col", Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, DEFAULT_PRIORITY, DEFAULT_TTL)))));
        warmAndValidate("select int_1, tiny_int_col, small_int_col, varchar_col from filterRanges where int_1 = 1 and tiny_int_col = 2", false, 6, 1);
        Session session = Session.builder(getSession())
                .setSystemProperty(catalog + "." + ENABLE_DEFAULT_WARMING, "false")
                .build();
        @Language("SQL") String query = "select not_warm_double ,int_1 from filterRanges where int_1 > 0";
        Map<String, Long> expectedQueryStats = Map.of(
                "filtered_by_predicate", 0L,
                "varada_match_columns", 1L,
                "varada_collect_columns", 1L,
                "external_collect_columns", 1L);
        validateQueryStats(query, session, expectedQueryStats, true);

        query = "select not_warm_double ,int_1 from filterRanges where int_1 > 1";
        expectedQueryStats = Map.of(
                "filtered_by_predicate", 1L,
                "varada_match_columns", 1L,
                "varada_collect_columns", 2L,
                "external_collect_columns", 0L);
        validateQueryStats(query, session, expectedQueryStats, true);

        query = "select not_warm_double ,int_1 from filterRanges where int_1 > 1 and tiny_int_col > 5";
        expectedQueryStats = Map.of(
                "filtered_by_predicate", 1L,
                "varada_match_columns", 2L,
                "varada_collect_columns", 2L,
                "external_collect_columns", 0L);
        validateQueryStats(query, session, expectedQueryStats, true);

        query = "select small_int_col from filterRanges where small_int_col > 5";
        expectedQueryStats = Map.of(
                "filtered_by_predicate", 1L,
                "varada_match_columns", 1L,
                "varada_collect_columns", 1L,
                "external_collect_columns", 0L,
                "external_match_columns", 0L);
        validateQueryStats(query, session, expectedQueryStats, true);

        query = "select small_int_col, varchar_col from filterRanges where small_int_col > 5 and varchar_col > '6'";
        expectedQueryStats = Map.of(
                "filtered_by_predicate", 1L,
                "varada_match_columns", 2L,
                "varada_collect_columns", 2L,
                "external_collect_columns", 0L,
                "external_match_columns", 0L);
        validateQueryStats(query, session, expectedQueryStats, true);

        query = "select not_warm_double ,int_1 from filterRanges where int_1 is null";
        expectedQueryStats = Map.of(
                "filtered_by_predicate", 0L,
                "varada_match_columns", 1L,
                "prefilled_collect_columns", 1L,
                "varada_collect_columns", 0L,
                "external_collect_columns", 1L);
        validateQueryStats(query, session, expectedQueryStats, true);

        query = "select not_warm_double ,int_1 from filterRanges where int_1 > 0 or int_1 is null";
        expectedQueryStats = Map.of(
                "filtered_by_predicate", 0L,
                "varada_match_columns", 1L,
                "varada_collect_columns", 1L,
                "external_collect_columns", 1L);
        validateQueryStats(query, session, expectedQueryStats, true);

        query = "select not_warm_double ,tiny_int_col from filterRanges where tiny_int_col > 3";
        expectedQueryStats = Map.of(
                "filtered_by_predicate", 1L,
                "varada_match_columns", 1L,
                "varada_collect_columns", 2L,
                "external_collect_columns", 0L);
        validateQueryStats(query, session, expectedQueryStats, true);
    }

    @Test
    public void testWarmupTypes()
            throws IOException
    {
        computeActual(getSession(), "INSERT INTO t VALUES (1, 'shlomi')");
        warmAndValidate("select int1, v1 from t where v1 = 'shlomi' or int1 = 1", true, 4, 1);
        List<io.trino.plugin.warp.gen.constants.WarmUpType> warmedElementTypes = getWarmedElements();
        assertThat(warmedElementTypes).containsExactlyInAnyOrder(
                io.trino.plugin.warp.gen.constants.WarmUpType.WARM_UP_TYPE_BASIC,
                io.trino.plugin.warp.gen.constants.WarmUpType.WARM_UP_TYPE_BASIC,
                io.trino.plugin.warp.gen.constants.WarmUpType.WARM_UP_TYPE_DATA,
                io.trino.plugin.warp.gen.constants.WarmUpType.WARM_UP_TYPE_DATA);
        Session session = Session.builder(getSession())
                .setSystemProperty(catalog + "." + ENABLE_DEFAULT_WARMING, "false")
                .build();

        @Language("SQL") String nativeQuery = "select v1 from t where v1 = 'shlomi'";
        Map<String, Long> expectedQueryStats = Map.of(
                "external_match_columns", 0L,
                "external_collect_columns", 0L,
                "prefilled_collect_columns", 1L,
                "varada_match_columns", 1L,
                "varada_collect_columns", 0L);
        int expectedLuceneReadColumns = 0;
        validateQueryStats(nativeQuery, session, expectedQueryStats, expectedLuceneReadColumns);
        @Language("SQL") String luceneQuery = "select v1 from t where v1 like '%shlomi%'";
        expectedQueryStats = Map.of(
                "external_match_columns", 1L,
                "external_collect_columns", 0L,
                "prefilled_collect_columns", 0L,
                "varada_match_columns", 0L,
                "varada_collect_columns", 1L);
        validateQueryStats(luceneQuery, session, expectedQueryStats, expectedLuceneReadColumns);

        warmAndValidate(luceneQuery, true, 1, 1);
        warmedElementTypes = getWarmedElements();
        assertThat(warmedElementTypes).containsExactlyInAnyOrder(
                io.trino.plugin.warp.gen.constants.WarmUpType.WARM_UP_TYPE_BASIC,
                io.trino.plugin.warp.gen.constants.WarmUpType.WARM_UP_TYPE_BASIC,
                io.trino.plugin.warp.gen.constants.WarmUpType.WARM_UP_TYPE_DATA,
                io.trino.plugin.warp.gen.constants.WarmUpType.WARM_UP_TYPE_DATA,
                io.trino.plugin.warp.gen.constants.WarmUpType.WARM_UP_TYPE_LUCENE);

        expectedQueryStats = Map.of(
                "external_match_columns", 0L,
                "external_collect_columns", 0L,
                "prefilled_collect_columns", 0L,
                "varada_match_columns", 1L,
                "varada_collect_columns", 1L);
        expectedLuceneReadColumns = 1;
        validateQueryStats(luceneQuery, session, expectedQueryStats, expectedLuceneReadColumns);

        expectedQueryStats = Map.of(
                "external_match_columns", 0L,
                "external_collect_columns", 0L,
                "prefilled_collect_columns", 1L,
                "varada_match_columns", 1L,
                "varada_collect_columns", 0L);
        expectedLuceneReadColumns = 0;
        validateQueryStats(nativeQuery, session, expectedQueryStats, expectedLuceneReadColumns);

        validateQueryStats("select v1 from t where v1 = 'shlomi'", session, expectedQueryStats, expectedLuceneReadColumns);

        List<String> valuesQueries = List.of(
                "select v1 from t where v1 = 'shlomi' or int1 > 5",
                "select v1 from t where v1 in ('shlomi', 'tzachi') or int1 > 5");
        for (@Language("SQL") String query : valuesQueries) {
            expectedQueryStats = Map.of(
                    "external_match_columns", 0L,
                    "external_collect_columns", 0L,
                    "prefilled_collect_columns", 0L,
                    "varada_match_columns", 2L,
                    "varada_collect_columns", 2L);
            validateQueryStats(query, session, expectedQueryStats, expectedLuceneReadColumns);
        }

        @Language("SQL") String query = "select v1 from t where int1 > 5 and (v1 = 'shlomi' or v1 = 'tzachi')";
        expectedQueryStats = Map.of(
                "external_match_columns", 0L,
                "external_collect_columns", 0L,
                "prefilled_collect_columns", 0L,
                "varada_match_columns", 2L,
                "varada_collect_columns", 1L);
        validateQueryStats(query, session, expectedQueryStats, 0);

        List<String> rangeQueries = List.of(
                // "select v1 from t where v1 <> 'shlomi' or int1 > 5", //todo: fix not equal
                "select v1 from t where v1 > 'shlomi'",
                "select v1 from t where v1 >= 'shlomi'",
                "select v1 from t where v1 < 'shlomi'",
                "select v1 from t where v1 >= 'shlomi'");
        for (@Language("SQL") String rangeQuery : rangeQueries) {
            expectedQueryStats = Map.of(
                    "external_match_columns", 0L,
                    "external_collect_columns", 0L,
                    "prefilled_collect_columns", 0L,
                    "varada_match_columns", 1L,
                    "varada_collect_columns", 1L);
            expectedLuceneReadColumns = 1;
            validateQueryStats(rangeQuery, session, expectedQueryStats, expectedLuceneReadColumns);
        }
    }

    @Test
    public void testTransformColumn()
    {
        computeActual(getSession(), "INSERT INTO t VALUES (1, 'shlomi')");

        @Language("SQL") String query = "select v1 from t where v1 = 'shlomi'";
        warmAndValidate(query, true, 2, 1);

        Session querySession = Session.builder(getSession())
                .setSystemProperty(catalog + "." + ENABLE_DEFAULT_WARMING, "false")
                .build();

        Map<String, Long> expectedQueryStats = Map.of(
                "transformed_column", 0L,
                "varada_match_columns", 1L,
                "external_collect_columns", 0L,
                "external_match_columns", 0L);
        validateQueryStats(query, querySession, expectedQueryStats);

        expectedQueryStats = Map.of(
                "transformed_column", 1L,
                "varada_match_columns", 1L,
                "external_collect_columns", 0L,
                "external_match_columns", 0L);

        query = "select v1 from t where lower(v1) = 'shlomi'";
        warmAndValidate(query, true, 1, 1);

        validateQueryStats(query, querySession, expectedQueryStats);

        query = "select v1 from t where upper(v1) = 'shlomi'";
        warmAndValidate(query, true, 1, 1);
        validateQueryStats(query, querySession, expectedQueryStats);
    }

    @Test
    public void testTransformColumnDate()
    {
        try {
            assertUpdate("CREATE TABLE transform_data(" +
                    "var_date_col varchar) WITH (format='PARQUET', partitioned_by = ARRAY[])");

            computeActual(getSession(), "INSERT INTO transform_data VALUES ('2002-04-29')");
            Session warmSession = Session.builder(getSession())
                    .setSystemProperty(catalog + "." + ENABLE_DEFAULT_WARMING, "true")
                    .build();
            warmAndValidate("select var_date_col from transform_data where var_date_col = 'a'", warmSession, 2, 1, 0);

            //now warm with transform column
            warmAndValidate("select var_date_col from transform_data where day_of_week(CAST(var_date_col as date)) = 2012", warmSession, 1, 1, 0);

            String baseQuery = "select var_date_col from transform_data where %s(CAST(var_date_col as date)) = 5";
            Map<String, Long> expectedQueryStats = Map.of(
                    "transformed_column", 1L,
                    "varada_collect_columns", 1L,
                    "varada_match_columns", 1L,
                    "external_match_columns", 0L);
            for (FunctionName dateFunction : SupportedFunctions.DATE_FUNCTIONS) {
                @Language("SQL") String dateFunctionQuery = format(baseQuery, dateFunction.getName());
                validateQueryStats(dateFunctionQuery, getSession(), expectedQueryStats);
            }
            baseQuery = "select var_date_col from transform_data where %s(date(var_date_col)) = 5";
            for (FunctionName dateFunction : SupportedFunctions.DATE_FUNCTIONS) {
                @Language("SQL") String dateFunctionQuery = format(baseQuery, dateFunction.getName());
                validateQueryStats(dateFunctionQuery, getSession(), expectedQueryStats);
            }
            List<String> queriesWithoutDateFunction = List.of(
                    "select var_date_col from transform_data where date(var_date_col) = date('2002-04-29')",
                    "select var_date_col from transform_data where CAST(var_date_col as date) = CAST('2002-04-29' as date)",
                    "select var_date_col from transform_data where CAST(var_date_col as date) in (CAST('2002-04-29' as date), CAST('2002-05-29' as date))",
                    "select var_date_col from transform_data where CAST(var_date_col as date) = CAST('2002-04-29' as date) or CAST(var_date_col as date) = CAST('2002-04-30' as date)");
            for (@Language("SQL") String query : queriesWithoutDateFunction) {
                validateQueryStats(query, getSession(), expectedQueryStats);
            }
        }
        finally {
            computeActual("DROP TABLE IF EXISTS transform_data");
        }
    }

    @Test
    public void testTransformColumnDateInvalidSplit()
    {
        try {
            assertUpdate("CREATE TABLE transform_data(" +
                    "var_date_col varchar) WITH (format='PARQUET', partitioned_by = ARRAY[])");

            computeActual(getSession(), "INSERT INTO transform_data (var_date_col) VALUES ('2002-04-29'), ('2002-04-29bla')");
            Session warmSession = Session.builder(getSession())
                    .setSystemProperty(catalog + "." + ENABLE_DEFAULT_WARMING, "true")
                    .build();
            warmAndValidate("select var_date_col from transform_data where var_date_col = 'a'", warmSession, 2, 1, 0);

            //now warm with transform column - expected to fail
            @Language("SQL") String query = "select var_date_col from transform_data where day_of_week(CAST(var_date_col as date)) = 2012";
            warmAndValidate(query, warmSession, 0, 1, 1);
            Map<String, Long> expectedQueryStats = Map.of(
                    "transformed_column", 0L,
                    "varada_collect_columns", 1L,
                    "varada_match_columns", 0L,
                    "external_match_columns", 1L);
            validateQueryStats(query, getSession(), expectedQueryStats);
        }
        finally {
            computeActual("DROP TABLE IF EXISTS transform_data");
        }
    }

    @Test
    public void testOrPredicates()
            throws IOException
    {
        try {
            assertUpdate("CREATE TABLE functionsTable(" +
                    "double1 double, " +
                    "double2 double, " +
                    "n_long_decimal decimal(30, 2), " +
                    "s_real real, " +
                    "bingint1 bigint, " +
                    "int_1 integer,  " +
                    "date_col date, " +
                    "timestamp_col TIMESTAMP(3), " +
                    "small_int_col smallint, " +
                    "tiny_int_col TINYINT, " +
                    "varchar_col varchar, " +
                    "varchar_10_col varchar(10), " +
                    "char_8_col char(8), " +
                    "not_warm_double double) WITH (format='PARQUET', partitioned_by = ARRAY[])");

            computeActual(getSession(), "INSERT INTO functionsTable VALUES (" +
                    "1.2, " +
                    "12.3, " +
                    "5.42, " +
                    "10.3e0, " +
                    "2147483649, " +
                    "2147483647, " +
                    "CAST('2002-04-29' as date), " +
                    "CAST('2002-04-29' as TIMESTAMP), " +
                    "9, " +
                    "7, " +
                    "'shlomi', " +
                    "'assaf', " +
                    "'aaaaaaaa', " +
                    "4.0)");

            createWarmupRules(DEFAULT_SCHEMA,
                    "functionsTable",
                    Map.ofEntries(
                            entry("bingint1", Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BASIC, DEFAULT_PRIORITY, DEFAULT_TTL))),
                            entry("int_1", Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BASIC, DEFAULT_PRIORITY, DEFAULT_TTL))),
                            entry("n_long_decimal", Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BASIC, DEFAULT_PRIORITY, DEFAULT_TTL))),
                            entry("tiny_int_col", Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BASIC, DEFAULT_PRIORITY, DEFAULT_TTL))),
                            entry("varchar_col", Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BASIC, DEFAULT_PRIORITY, DEFAULT_TTL))),
                            entry("char_8_col", Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BASIC, DEFAULT_PRIORITY, DEFAULT_TTL))),
                            entry("varchar_10_col", Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BASIC, DEFAULT_PRIORITY, DEFAULT_TTL))),
                            entry("small_int_col", Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BASIC, DEFAULT_PRIORITY, DEFAULT_TTL))),
                            entry("timestamp_col", Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BASIC, DEFAULT_PRIORITY, DEFAULT_TTL))),
                            entry("date_col", Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BASIC, DEFAULT_PRIORITY, DEFAULT_TTL))),
                            entry("s_real", Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BASIC, DEFAULT_PRIORITY, DEFAULT_TTL)))));
            Session warmSession = Session.builder(getSession())
                    .setSystemProperty(catalog + "." + ENABLE_DEFAULT_WARMING, "false")
                    .build();
            warmAndValidate("select * from functionsTable", warmSession, 11, 1, 0);
            warmSession = Session.builder(getSession())
                    .setSystemProperty(catalog + "." + ENABLE_DEFAULT_WARMING, "true")
                    .build();
            warmAndValidate("select count(*) from functionsTable where double1 > 2 or double2 > 3 or varchar_10_col like '%warm lucene%'", warmSession, 6, 1, 0);

            Session session = Session.builder(getSession())
                    .setSystemProperty(catalog + "." + ENABLE_DEFAULT_WARMING, "false")
                    .build();

            @Language("SQL") String query = "select count(*) from functionsTable where double1 > 2 OR n_long_decimal > 3"; ////todo bug in serialization of LongDecimal type
            Map<String, Long> expectedQueryStats = Map.of(
                    "varada_match_columns", 0L,
                    "external_match_columns", 0L);
            validateQueryStats(query, session, expectedQueryStats);

            query = "select * from functionsTable where cast(n_long_decimal as varchar) = '5.00'";
            validateQueryStats(query, session, expectedQueryStats);

            query = "select count(*) from functionsTable where double1 is null or ceil(double1) > 3";
            expectedQueryStats = Map.of(
                    "varada_match_columns", 1L,
                    "external_match_columns", 0L);
            validateQueryStats(query, session, expectedQueryStats);

            query = "select count(*) from functionsTable where ceil(double1) > 2 and ceil(double2) > 3";
            expectedQueryStats = Map.of(
                    "varada_match_columns", 2L,
                    "external_match_columns", 0L);
            validateQueryStats(query, session, expectedQueryStats);
            query = "select count(*) from functionsTable where ceil(double1) > 2 and ceiling(double2) > 3";
            expectedQueryStats = Map.of(
                    "varada_match_columns", 2L,
                    "external_match_columns", 0L);
            validateQueryStats(query, session, expectedQueryStats);

            query = "select count(*) from functionsTable where double1 > 2 or double2 > 3 or varchar_10_col like '%lucene%'";
            expectedQueryStats = Map.of(
                    "varada_match_columns", 3L,
                    "external_match_columns", 0L);
            validateQueryStats(query, session, expectedQueryStats);

            query = "select count(*) from functionsTable where ceil(double1) > 2 or ceil(double2) > 3";
            expectedQueryStats = Map.of(
                    "varada_match_columns", 2L,
                    "external_match_columns", 0L);
            validateQueryStats(query, session, expectedQueryStats);

            query = "select count(*) from functionsTable where double1 is null or double2 > 3";
            expectedQueryStats = Map.of(
                    "varada_match_columns", 2L,
                    "external_match_columns", 0L);
            validateQueryStats(query, session, expectedQueryStats);

            query = "select count(*) from functionsTable where double1 is null or double2 is null";
            expectedQueryStats = Map.of(
                    "varada_match_columns", 2L,
                    "external_match_columns", 0L);
            validateQueryStats(query, session, expectedQueryStats);

            query = "select count(*) from functionsTable where ceil(double1) = 2 or ceil(double1) = 3";
            expectedQueryStats = Map.of(
                    "varada_match_columns", 1L,
                    "external_match_columns", 0L);
            validateQueryStats(query, session, expectedQueryStats);

            query = "select count(*) from functionsTable where ceil(double1) = 2 or ceil(double1) = 3 or ceil(double2) =9";
            expectedQueryStats = Map.of(
                    "varada_match_columns", 2L,
                    "external_match_columns", 0L);
            validateQueryStats(query, session, expectedQueryStats);

            query = "select count(*) from functionsTable where double1 is null or double2 is not null";
            //not x is unsupported function
            expectedQueryStats = Map.of(
                    "varada_match_columns", 0L,
                    "external_match_columns", 0L);
            validateQueryStats(query, session, expectedQueryStats);

            query = "select count(*) from functionsTable where double1 is null or double1 > 3 or double2 > 5";
            expectedQueryStats = Map.of(
                    "varada_match_columns", 2L,
                    "external_match_columns", 0L);
            validateQueryStats(query, session, expectedQueryStats);

            query = "select count(*) from functionsTable where double1 > 5 AND (not_warm_double > 9 or double2 > 3)";
            ////a and (c or b) -> double1 is part of tupleDomain and not part of ConnectorExpression, currently we drop the OR section
            expectedQueryStats = Map.of(
                    "varada_match_columns", 1L,
                    "external_match_columns", 2L);
            validateQueryStats(query, session, expectedQueryStats);

            query = "select count(*) from functionsTable where ceil(double1) > 5 AND (not_warm_double > 9 or double2 > 3)";
            ////ceil(a) and (c or b) -> all columns exist in ConnectorExpression
            expectedQueryStats = Map.of(
                    "varada_match_columns", 1L,
                    "external_match_columns", 2L);
            validateQueryStats(query, session, expectedQueryStats);

            query = "select count(*) from functionsTable where ceil(double1) > 5 AND (bingint1 > 9 or double2 > 3)";
            expectedQueryStats = Map.of(
                    "varada_match_columns", 3L,
                    "external_match_columns", 0L);
            validateQueryStats(query, session, expectedQueryStats);

            query = "select count(*) from functionsTable where ceil(double1) > 5 OR (varchar_10_col like '%warm lucene%' and ceil(double2) > 3)";
            expectedQueryStats = Map.of(
                    "varada_match_columns", 3L,
                    "external_match_columns", 0L);
            validateQueryStats(query, session, expectedQueryStats);

            query = "select count(*) from functionsTable where not_warm_double > 5 or double1 > 3";
            expectedQueryStats = Map.of(
                    "varada_match_columns", 0L,
                    "external_match_columns", 2L);
            validateQueryStats(query, session, expectedQueryStats);

            query = "select count(*) from functionsTable where int_1 in (1,2) and (varchar_col = 'str3' or varchar_col like '%str%')";
            expectedQueryStats = Map.of(
                    "varada_match_columns", 1L,
                    "external_match_columns", 1L);
            validateQueryStats(query, session, expectedQueryStats);

            query = "select count(*) from functionsTable where  (s_real > 0 AND    (double1 < 5 or double2 > 2) AND (bingint1 > 3 or int_1 > 4) OR varchar_col = 'f')";
            //      ( s_real  AND (double1 OR double2 ) AND (bingint1 OR int_1) OR varchar_col ) =>
            //     ( (varchar_col OR  s_real) AND ( varchar_col OR double1 OR double2) AND (varchar_col OR bingint1 OR int_1)
            expectedQueryStats = Map.of(
                    "varada_match_columns", 6L,
                    "external_match_columns", 0L);
            validateQueryStats(query, session, expectedQueryStats);

            query = "select count(*) from functionsTable where  (not_warm_double > 0 AND    (double1 < 5 or double2 > 2) AND (bingint1 > 3 or int_1 > 4) OR varchar_col = 'f')";
            //      ( not_warm_double  AND (double1 OR double2 ) AND (bingint1 OR int_1) OR varchar_col ) =>
            //     ( (varchar_col OR  not_warm_double) AND ( not_warm_double OR double1 OR double2) AND (not_warm_double OR bingint1 OR int_1)
            expectedQueryStats = Map.of(
                    "varada_match_columns", 5L,
                    "external_match_columns", 1L);
            validateQueryStats(query, session, expectedQueryStats);

            query = "select count(*) from functionsTable where  (s_real > 0 AND    (double1 < 5 or double2 > 2) AND (bingint1 > 3 or int_1 > 4) OR not_warm_double = 5.0)";
            //      ( s_real  AND (double1 OR double2 ) AND (bingint1 OR int_1) OR not_warm_double ) =>
            //     ( (varchar_col OR  s_real) AND ( not_warm_double OR double1 OR double2) AND (not_warm_double OR bingint1 OR int_1)
            expectedQueryStats = Map.of(
                    "varada_match_columns", 0L,
                    "external_match_columns", 6L);
            validateQueryStats(query, session, expectedQueryStats);

            query = "select count(*) from functionsTable where  (s_real > 0 AND    (not_warm_double < 5 or double2 > 2) AND (bingint1 > 3 or int_1 > 4) OR varchar_col = 'f')";
            //      ( s_real  AND (not_warm_double OR double2 ) AND (bingint1 OR int_1) OR varchar_col ) =>
            //     ( (varchar_col OR  s_real) AND ( varchar_col OR not_warm_double OR double2) AND (varchar_col OR bingint1 OR int_1)
            expectedQueryStats = Map.of(
                    "varada_match_columns", 4L,
                    "external_match_columns", 2L);
            validateQueryStats(query, session, expectedQueryStats);

            query = "select count(*) from functionsTable where (double1 = 2 AND double2 = 3) OR (double1 = 4 AND double2 = 5)";
            //(A = 2 AND B = 3) OR (A = 4 AND B = 5) => (A = 2 OR B = 5 ) AND (A = 4 OR B = 3). Domain will be A[2, 4], B[3,5]
            expectedQueryStats = Map.of(
                    "varada_match_columns", 2L,
                    "external_match_columns", 0L);
            validateQueryStats(query, session, expectedQueryStats);

            query = "select count(*) from functionsTable where  (((s_real > 5 AND ceil(double1) = 1)) OR    (s_real < 0 and ceil(double2) = 2))";
            // (s_real' AND double1 ) OR (s_real'' AND double2) =>
            // ((s_real' OR double2) AND (double1 OR s_real'') AND (double1 OR double2)) // Domain will be s_real( [infinte,0), (5, infinte])
            expectedQueryStats = Map.of(
                    "varada_match_columns", 3L,
                    "external_match_columns", 0L);
            validateQueryStats(query, session, expectedQueryStats);

            query = "select count(*) from functionsTable where cast(timestamp_col as varchar) = '2021-04-12 00:00:00.000' or ceil(double2) > 3";
            expectedQueryStats = Map.of(
                    "varada_match_columns", 2L,
                    "external_match_columns", 0L);
            validateQueryStats(query, session, expectedQueryStats);

            query = "select count(*) from functionsTable where ((ceil(double1) > 5 AND ceil(double2) > 5) OR  (ceil(double1) < 5 AND ceil(double2) < 5))";
            //domain is translated to double1 ( [infinte,5), (5, infinte]), double2 ( [infinte,5), (5, infinte])
            expectedQueryStats = Map.of(
                    "varada_match_columns", 2L,
                    "external_match_columns", 0L);
            validateQueryStats(query, session, expectedQueryStats);
        }
        finally {
            computeActual("DROP TABLE IF EXISTS functionsTable");
        }
    }

    @Test
    public void testLuceneFunctions()
            throws IOException
    {
        computeActual(getSession(), "INSERT INTO t VALUES (1, 'shlomi')");
        warmAndValidate("select v1 from t where trim(v1) = 'sh'", true, 2, 1);
        List<io.trino.plugin.warp.gen.constants.WarmUpType> warmedElementTypes = getWarmedElements();
        assertThat(warmedElementTypes).containsExactlyInAnyOrder(
                io.trino.plugin.warp.gen.constants.WarmUpType.WARM_UP_TYPE_LUCENE,
                io.trino.plugin.warp.gen.constants.WarmUpType.WARM_UP_TYPE_DATA);
        List<String> queries = List.of(
                "select count(*) from t where v1 is NULL or v1 like '%new%'",
                "select count(*) from t where v1 like 'str%' and v1 >  'str'",
                "select count(*) from t where trim(v1) = 'trim'",
                "select count(*) from t where ltrim(v1) = 'left trim'",
                "select count(*) from t where ltrim(v1) in ('left trim', 'bla bla')",
                "select count(*) from t where rtrim(v1) = 'right trim'",
                "select count(*) from t where split_part(v1, '1', 1) = 'value to match'",
                "select count(*) from t where split_part(v1, '1', 1) in ('tzachi', 'roman')",
                "select count(*) from t where substr(v1, -3, 2) IN ('ZD','4R') or strpos(v1, 'shlomi') = 2",
                "select count(*) from t where substr(v1, -3, 2) IN ('ZD','4R')",
                "select count(*) from t where substring(v1, -3, 2) IN ('ZD','4R')",
                "select count(*) from t where substr(v1, 2) IN ('ZD','4R')",
                "select count(*) from t where substring(v1, 3) IN ('ZD','4R')",
                "select count(*) from t where v1 not like '%bla%'",
                "select count(*) from t where substring(v1, 1) = 'bla'",
                "select count(*) from t where substring(v1, 1, 3) = 'bla'",
                "select count(*) from t where substr(v1, 6) = 'bla'",
                "select count(*) from t where substr(v1, 6, 3) = 'bla'",
                "select count(*) from t where strpos(v1, 'shlomi') = 0", //means not-equal
                "select count(*) from t where strpos(v1, 'shlomi', 3) = 2");

        Map<String, Long> expectedJmxQueryStats = Map.of(
                "varada_match_columns", 1L,
                "varada_collect_columns", 1L,
                "external_match_columns", 0L);
        for (@Language("SQL") String query : queries) {
            int expectedLuceneReadColumns = query.contains("or") || query.contains("and") ? 2 : 1;
            validateQueryStats(query, getSession(), expectedJmxQueryStats, expectedLuceneReadColumns);
        }
        expectedJmxQueryStats = Map.of(
                "varada_match_columns", 1L,
                "varada_collect_columns", 1L,
                "external_collect_columns", 1L,
                "external_match_columns", 1L);
        validateQueryStats("select count(*) from t where v1 like '%Santa%' and int1 > 1000", getSession(), expectedJmxQueryStats);

        expectedJmxQueryStats = Map.of(
                "varada_match_columns", 0L,
                "varada_collect_columns", 1L,
                "external_collect_columns", 1L,
                "external_match_columns", 2L);
        validateQueryStats("select count(*) from t where v1 like '%Santa%' or int1 > 1000", getSession(), expectedJmxQueryStats);

        expectedJmxQueryStats = Map.of(
                "varada_match_columns", 1L,
                "varada_collect_columns", 1L,
                "external_collect_columns", 0L,
                "external_match_columns", 0L);
        //translated by Trino to expression (v1 like '%Mo%' and v1 like 'San%'), domain ([San, Sao))
        validateQueryStats("select count(*) from t where v1 like 'San%' and v1 like '%Mo%'", getSession(), expectedJmxQueryStats);
    }

    @Test
    public void testTripsDataTableCase4()
            throws IOException
    {
        assertUpdate("CREATE TABLE trips_data_table(" +
                "driver_age integer, " +
                "ts timestamp(3), " +
                "driver_last varchar(32), " +
                "driver_first varchar(32)) WITH (format='PARQUET', partitioned_by = ARRAY[])");

        computeActual(getSession(), "INSERT INTO trips_data_table VALUES (" +
                "1, " +
                "CAST('2002-04-29' as TIMESTAMP), " +
                "'aaaaaaaa', " +
                "'aaaaaaaa')");
        createWarmupRules(DEFAULT_SCHEMA,
                "trips_data_table",
                Map.ofEntries(
                        entry("driver_age", Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BASIC, DEFAULT_PRIORITY, DEFAULT_TTL),
                                new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, DEFAULT_PRIORITY, DEFAULT_TTL))),
                        entry("ts", Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BASIC, DEFAULT_PRIORITY, DEFAULT_TTL))),
                        entry("driver_first", Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, DEFAULT_PRIORITY, DEFAULT_TTL)))));
        warmAndValidate("select * from trips_data_table", false, 4, 1);

        @Language("SQL") String query = "select MAX(driver_age) from trips_data_table where driver_first='Grant' AND driver_last='JACKSON' AND ts<=CAST('2018-01-03 10:50:35.000' as timestamp)";
        Map<String, Long> expectedJmxQueryStats = Map.of(
                "varada_match_columns", 1L,
                "varada_collect_columns", 3L,
                "prefilled_collect_columns", 0L,
                "external_match_columns", 2L,
                "external_collect_columns", 1L);
        validateQueryStats(query, getSession(), expectedJmxQueryStats);

        //trips_data_table_case4_q02
        query = "select MIN(driver_first) from trips_data_table where driver_age=20 OR driver_age=19 OR ts<=CAST('2018-01-03 10:50:35.000' as timestamp)";
        expectedJmxQueryStats = Map.of(
                "varada_match_columns", 2L,
                "varada_collect_columns", 2L,
                "external_match_columns", 0L,
                "external_collect_columns", 1L);
        validateQueryStats(query, getSession(), expectedJmxQueryStats);

        //trips_data_table_case4_q04
        query = "select MAX(driver_age) from trips_data_table where driver_first='Grant' OR driver_last='JACKSON' OR ts<=CAST('2018-01-03 10:50:35.000' as timestamp)";
        expectedJmxQueryStats = Map.of(
                "varada_match_columns", 0L,
                "varada_collect_columns", 2L,
                "external_match_columns", 3L,
                "external_collect_columns", 2L);
        validateQueryStats(query, getSession(), expectedJmxQueryStats);
    }

    @Test
    public void testTripsDataTableCase5()
            throws IOException
    {
        assertUpdate("CREATE TABLE trips_data_table(" +
                "driver_age integer, " +
                "ts timestamp(3), " +
                "driver_gender char(1), " +
                "driver_last varchar(32), " +
                "driver_first varchar(32)) WITH (format='PARQUET', partitioned_by = ARRAY[])");

        computeActual(getSession(), "INSERT INTO trips_data_table VALUES (" +
                "1, " +
                "CAST('2002-04-29' as TIMESTAMP), " +
                "'a', " +
                "'aaaaaaaa', " +
                "'aaaaaaaa')");
        createWarmupRules(DEFAULT_SCHEMA,
                "trips_data_table",
                Map.ofEntries(
                        entry("driver_first", Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_LUCENE, DEFAULT_PRIORITY, DEFAULT_TTL),
                                new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, DEFAULT_PRIORITY, DEFAULT_TTL))),
                        entry("driver_gender", Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_LUCENE, DEFAULT_PRIORITY, DEFAULT_TTL),
                                new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, DEFAULT_PRIORITY, DEFAULT_TTL)))));
        warmAndValidate("select count(driver_first), count(driver_gender) from trips_data_table", false, 4, 1);

        //trips_data_table_case5_q02
        @Language("SQL") String query = "select MIN(driver_first) from trips_data_table where driver_gender LIKE '%F%' AND driver_age=20 OR driver_age=19 OR ts<=CAST('2018-01-03 10:50:35.000' as timestamp)";
        Map<String, Long> expectedJmxQueryStats = Map.of(
                "varada_match_columns", 0L,
                "varada_collect_columns", 2L,
                "external_match_columns", 3L,
                "external_collect_columns", 2L);
        validateQueryStats(query, getSession(), expectedJmxQueryStats);

        //trips_data_table_case5_q04
        query = "select MAX(driver_age) from trips_data_table where driver_gender>='M' AND (driver_first='Grant' OR driver_last='JACKSON' OR ts<=CAST('2018-01-03 10:50:35.000' as timestamp))";
        expectedJmxQueryStats = Map.of(
                "varada_match_columns", 1L,
                "varada_collect_columns", 2L,
                "external_match_columns", 3L,
                "external_collect_columns", 3L);
        validateQueryStats(query, getSession(), expectedJmxQueryStats);
    }

    @Test
    public void testQueriesWithFunctions()
            throws IOException
    {
        assertUpdate("CREATE TABLE functionsTable(" +
                "double1 double, " +
                "double2 double, " +
                "n_long_decimal decimal(30, 2), " +
                "s_real real, " +
                "bingint1 bigint, " +
                "int_1 integer,  " +
                "date_col date, " +
                "timestamp_col TIMESTAMP(3), " +
                "small_int_col smallint, " +
                "tiny_int_col TINYINT, " +
                "varchar_col varchar, " +
                "varchar_10_col varchar(10), " +
                "char_8_col char(8), " +
                "not_warm_double double) WITH (format='PARQUET', partitioned_by = ARRAY[])");

        computeActual(getSession(), "INSERT INTO functionsTable VALUES (" +
                "1.2, " +
                "12.3, " +
                "5.42, " +
                "10.3e0, " +
                "2147483649, " +
                "2147483647, " +
                "CAST('2002-04-29' as date), " +
                "CAST('2002-04-29' as TIMESTAMP), " +
                "9, " +
                "7, " +
                "'shlomi', " +
                "'assaf', " +
                "'aaaaaaaa', " +
                "4.0)");

        createWarmupRules(DEFAULT_SCHEMA,
                "functionsTable",
                Map.ofEntries(
                        entry("double1", Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BASIC, DEFAULT_PRIORITY, DEFAULT_TTL))),
                        entry("double2", Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BASIC, DEFAULT_PRIORITY, DEFAULT_TTL))),
                        entry("bingint1", Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BASIC, DEFAULT_PRIORITY, DEFAULT_TTL))),
                        entry("int_1", Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BASIC, DEFAULT_PRIORITY, DEFAULT_TTL))),
                        entry("n_long_decimal", Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BASIC, DEFAULT_PRIORITY, DEFAULT_TTL))),
                        entry("tiny_int_col", Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BASIC, DEFAULT_PRIORITY, DEFAULT_TTL))),
                        entry("varchar_col", Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BASIC, DEFAULT_PRIORITY, DEFAULT_TTL))),
                        entry("char_8_col", Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BASIC, DEFAULT_PRIORITY, DEFAULT_TTL))),
                        entry("varchar_10_col", Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BASIC, DEFAULT_PRIORITY, DEFAULT_TTL))),
                        entry("small_int_col", Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BASIC, DEFAULT_PRIORITY, DEFAULT_TTL))),
                        entry("timestamp_col", Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BASIC, DEFAULT_PRIORITY, DEFAULT_TTL))),
                        entry("date_col", Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BASIC, DEFAULT_PRIORITY, DEFAULT_TTL))),
                        entry("s_real", Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BASIC, DEFAULT_PRIORITY, DEFAULT_TTL)))));
        warmAndValidate("select * from functionsTable", false, 13, 1);

        Session session = Session.builder(getSession())
                .setSystemProperty(catalog + "." + ENABLE_DEFAULT_WARMING, "true")
                .build();

        //cast nonVarchar to varchar
        @Language("SQL") String query = "select * from functionsTable where cast(char_8_col as varchar) = 'aaaaaaaa'";
        Map<String, Long> expectedJmxQueryStats = Map.of(
                "varada_match_columns", 1L,
                "external_match_columns", 0L);
        validateQueryStats(query, session, expectedJmxQueryStats);

        query = "select * from functionsTable where cast(timestamp_col as varchar) = '2021-04-12 00:00:00.000'";
        validateQueryStats(query, session, expectedJmxQueryStats);

        query = "select * from functionsTable where cast(date_col as varchar) = '2021-04-12'";
        validateQueryStats(query, session, expectedJmxQueryStats);

        query = "select * from functionsTable where cast(s_real as varchar) = '7.2961776E7' or cast(s_real as varchar) = '7.296178E7'";
        validateQueryStats(query, session, expectedJmxQueryStats);

        query = "select * from functionsTable where cast(s_real as varchar) = '5.0E0'";
        validateQueryStats(query, session, expectedJmxQueryStats);

        query = "select * from functionsTable where cast(double1 as varchar) = '5.0E0'";
        validateQueryStats(query, session, expectedJmxQueryStats);

        query = "select * from functionsTable where cast(double1 as varchar) = '5.0E0'";
        validateQueryStats(query, session, expectedJmxQueryStats);

        query = "select * from functionsTable where cast(small_int_col as varchar) = '5'";
        validateQueryStats(query, session, expectedJmxQueryStats);

        query = "select * from functionsTable where cast(tiny_int_col as varchar) = '5'";
        validateQueryStats(query, session, expectedJmxQueryStats);

        query = "select * from functionsTable where cast(bingint1 as varchar) = '5'";
        validateQueryStats(query, session, expectedJmxQueryStats);

        query = "select * from functionsTable where cast(int_1 as varchar) = '5'";
        validateQueryStats(query, session, expectedJmxQueryStats);

        expectedJmxQueryStats = Map.of(
                "varada_match_columns", 0L,
                "varada_collect_columns", 1L,
                "external_match_columns", 0L);
        query = "select n_long_decimal from functionsTable where cast(n_long_decimal as varchar) = '5.00'";
        validateQueryStats(query, session, expectedJmxQueryStats);

        query = "select * from functionsTable where cast(double1 as varchar) > '5.0E0'";
        expectedJmxQueryStats = Map.of(
                "varada_match_columns", 0L,
                "external_match_columns", 1L);
        validateQueryStats(query, session, expectedJmxQueryStats);

        query = "select * from functionsTable where cast(varchar_col as varchar) = 'shlomi'";
        expectedJmxQueryStats = Map.of(
                "varada_match_columns", 1L,
                "external_match_columns", 0L);
        validateQueryStats(query, session, expectedJmxQueryStats);

        //cast to limited varchar
        query = "select * from functionsTable where cast(int_1 as varchar(20)) = '5'";
        expectedJmxQueryStats = Map.of(
                "varada_match_columns", 1L,
                "external_match_columns", 0L);
        validateQueryStats(query, session, expectedJmxQueryStats);

        query = "select * from functionsTable where cast(int_1 as varchar(1)) = '44' and varchar_col = 'shlomi'";
        QueryRunner.MaterializedResultWithPlan materializedResult = getQueryRunner()
                .executeWithPlan(session, query);
        assertThat(getCustomMetrics(materializedResult.queryId(), getDistributedQueryRunner()).size()).isEqualTo(0); // no pushdown

        query = "select * from functionsTable where cast(int_1 as varchar(1)) = '44' or varchar_col = 'shlomi'";
        expectedJmxQueryStats = Map.of(
                "varada_match_columns", 1L,
                "external_match_columns", 0L);
        validateQueryStats(query, session, expectedJmxQueryStats);

        query = "select * from functionsTable where cast(int_1 as varchar(1)) = '44'";
        materializedResult = getQueryRunner()
                .executeWithPlan(session, query);
        assertThat(getCustomMetrics(materializedResult.queryId(), getDistributedQueryRunner()).size()).isEqualTo(0); // no pushdown

        query = "select* from functionsTable where cast(char_8_col as varchar(2)) = 'aa'";
        expectedJmxQueryStats = Map.of(
                "varada_match_columns", 0L,
                "external_match_columns", 1L);
        validateQueryStats(query, session, expectedJmxQueryStats);
        query = "select * from functionsTable where cast(char_8_col as varchar(2)) = 'aa' or cast(char_8_col as varchar(2)) = 'bb'";
        validateQueryStats(query, session, expectedJmxQueryStats);

        query = "select * from functionsTable where cast(char_8_col as varchar(10)) = 'aaaaaaaa'";
        expectedJmxQueryStats = Map.of(
                "varada_match_columns", 1L,
                "external_match_columns", 0L);
        validateQueryStats(query, session, expectedJmxQueryStats);

        query = "select * from functionsTable where cast(varchar_10_col as varchar(20)) = 'aaa' or cast(varchar_10_col as varchar(20)) = 'bbb'";
        expectedJmxQueryStats = Map.of(
                "varada_match_columns", 1L,
                "external_match_columns", 0L);
        validateQueryStats(query, session, expectedJmxQueryStats); //not pushdown as expression

        query = "select * from functionsTable where cast(varchar_10_col as varchar(5)) = 'aaa' or cast(varchar_10_col as varchar(5)) = 'bbb'";
        expectedJmxQueryStats = Map.of(
                "varada_match_columns", 0L,
                "external_match_columns", 1L);
        validateQueryStats(query, session, expectedJmxQueryStats);

        //cast to nonVarchar
        query = "select * from functionsTable where cast(double1 as real) >= 4.02";
        expectedJmxQueryStats = Map.of(
                "varada_match_columns", 1L,
                "external_match_columns", 0L);
        validateQueryStats(query, session, expectedJmxQueryStats);

        query = "select * from functionsTable where cast(double1 as real) >= 4.02 or cast(double1 as real) = 2.96";
        validateQueryStats(query, session, expectedJmxQueryStats);

        query = "select * from functionsTable where cast(double1 as real) = 10";
        validateQueryStats(query, session, expectedJmxQueryStats);

        query = "select * from functionsTable where cast(double1 as real) = 10 or cast(double1 as real) = 50";
        validateQueryStats(query, session, expectedJmxQueryStats);

        query = "select * from functionsTable where cast(double1 as int) = 5";
        expectedJmxQueryStats = Map.of(
                "varada_match_columns", 0L,
                "external_match_columns", 0L);
        validateQueryStats(query, session, expectedJmxQueryStats);

        //notEqual - not currently to supported to nativeExpression
        query = "select * from functionsTable where double1 <> 5";
        expectedJmxQueryStats = Map.of(
                "varada_match_columns", 0L,
                "external_match_columns", 1L);
        validateQueryStats(query, session, expectedJmxQueryStats);

        query = "select * from functionsTable where double1 != 5";
        validateQueryStats(query, session, expectedJmxQueryStats);

        //ceil
        query = "select * from functionsTable where ceil(not_warm_double) > 5";
        expectedJmxQueryStats = Map.of(
                "varada_match_columns", 0L,
                "external_match_columns", 1L);
        validateQueryStats(query, session, expectedJmxQueryStats);

        query = "select * from functionsTable where ceil(double1) != 5";
        validateQueryStats(query, session, expectedJmxQueryStats);

        query = "select * from functionsTable where ceil(double1) = 5  or ceil(double1) = 9";
        expectedJmxQueryStats = Map.of(
                "varada_match_columns", 1L,
                "external_match_columns", 0L);
        validateQueryStats(query, session, expectedJmxQueryStats);

        query = "select * from functionsTable where ceil(s_real) > 5 ";
        validateQueryStats(query, session, expectedJmxQueryStats);

        expectedJmxQueryStats = Map.of(
                "filtered_by_predicate", 1L,
                "varada_match_columns", 1L,
                "varada_collect_columns", 1L,
                "external_match_columns", 0L);

        query = "select double1 from functionsTable where cast(double1 as varchar) = '01111111111111111111E0'";
        validateQueryStats(query, session, expectedJmxQueryStats); // '01111111111111111111E0' is a non-default format- EmptyPageSource

        query = "select double1 from functionsTable where ceil(double1) = 5  and cast(double1 as varchar)= '5'"; // cast(double1 as varchar)= '5' is not possible cast - EmptyPageSource
        validateQueryStats(query, session, expectedJmxQueryStats);

        query = "select double1 from functionsTable where ceil(double1) > 5  and ceil(double1) < 4"; // EmptyPageSource
        validateQueryStats(query, session, expectedJmxQueryStats);

        expectedJmxQueryStats = Map.of(
                "filtered_by_predicate", 1L,
                "varada_match_columns", 2L,
                "varada_collect_columns", 1L,
                "external_match_columns", 0L);

        query = "select double1 from functionsTable where ceil(double1) > 5  and ceil(double1) < 4 and double2 > 6"; // EmptyPageSource
        validateQueryStats(query, session, expectedJmxQueryStats);

        query = "select double1 from functionsTable where double2 > 5 and ceil(double1) > 5  and ceil(double1) < 4"; // EmptyPageSource
        validateQueryStats(query, session, expectedJmxQueryStats);

        expectedJmxQueryStats = Map.of(
                "filtered_by_predicate", 1L,
                "varada_match_columns", 2L,
                "varada_collect_columns", 2L,
                "external_match_columns", 0L);

        query = "select double1 from functionsTable where ceil(double1) > 5  and ceil(double1) < 4 and  yow(date_col) = 53"; // EmptyPageSource
        validateQueryStats(query, session, expectedJmxQueryStats);

        query = "select double1 from functionsTable where ceil(double1) > 5  and ceil(double1) < 4 and ceil(not_warm_double) > 5"; // EmptyPageSource
        validateQueryStats(query, session, expectedJmxQueryStats);

        query = "select n_long_decimal from functionsTable where ceil(n_long_decimal) > " + Integer.MAX_VALUE + 1;
        validateQueryStats(query, session, Map.of(
                "varada_collect_columns", 1L,
                "varada_match_columns", 0L,
                "external_match_columns", 0L));

        query = "select double1 from functionsTable where double2 > 5 or ceil(double1) > 9  and ceil(double1) < 4";
        //translated to: (double2 > 5 or ceil(double1) > 9) AND (double2 > 5 or ceil(double1) < 4)
        validateQueryStats(query, session, Map.of(
                "varada_collect_columns", 2L,
                "varada_match_columns", 2L,
                "external_match_columns", 0L));

        query = "select * from functionsTable where year(date_col) = 53";
        expectedJmxQueryStats = Map.of(
                "varada_match_columns", 1L,
                "external_match_columns", 0L);
        validateQueryStats(query, session, expectedJmxQueryStats);

        query = "select count(*) from functionsTable where double1 is null and ceil(double1) > 3";
        validateQueryStats(query, session, expectedJmxQueryStats);

        query = "select count(*) from functionsTable where cast(double1 as varchar) = '01111111111111111111E0' or double2 > 2";
        //cast(double1 as varchar) = '01111111111111111111E0' is invalid but left is valid
        expectedJmxQueryStats = Map.of(
                "varada_match_columns", 1L,
                "external_match_columns", 1L); //todo: fix?
        validateQueryStats(query, session, expectedJmxQueryStats);

        query = "select count(*) from functionsTable where cast(double1 as varchar) = '01111111111111111111E0' or cast(double2 as varchar) = '01111111111111111111E0'";
        //both are invalid
        expectedJmxQueryStats = Map.of(
                "filtered_by_predicate", 1L,
                "varada_collect_columns", 2L,
                "varada_match_columns", 2L,
                "external_match_columns", 0L);
        validateQueryStats(query, session, expectedJmxQueryStats);

        //date functions
        query = "select * from functionsTable where day(date_col) > 2012";
        expectedJmxQueryStats = Map.of(
                "varada_match_columns", 1L,
                "external_match_columns", 0L);
        validateQueryStats(query, session, expectedJmxQueryStats);
        query = "select * from functionsTable where day(date_col) = 2012";
        validateQueryStats(query, session, expectedJmxQueryStats);
        query = "select * from functionsTable where day_of_month(date_col) = 2012";
        validateQueryStats(query, session, expectedJmxQueryStats);
        query = "select * from functionsTable where day_of_week(date_col) = 2012";
        validateQueryStats(query, session, expectedJmxQueryStats);
        query = "select * from functionsTable where dow(date_col) < 2012";
        validateQueryStats(query, session, expectedJmxQueryStats);
        query = "select * from functionsTable where dow(date_col) = 2012";
        validateQueryStats(query, session, expectedJmxQueryStats);
        query = "select * from functionsTable where day_of_year(date_col) = 2012";
        validateQueryStats(query, session, expectedJmxQueryStats);
        query = "select * from functionsTable where day_of_year(date_col) >2 and day_of_year(date_col) <60";
        validateQueryStats(query, session, expectedJmxQueryStats);
        query = "select * from functionsTable where doy(date_col) = 2012";
        validateQueryStats(query, session, expectedJmxQueryStats);
        query = "select * from functionsTable where week(date_col) = 53";
        validateQueryStats(query, session, expectedJmxQueryStats);
        query = "select * from functionsTable where week_of_year(date_col) = 53";
        validateQueryStats(query, session, expectedJmxQueryStats);

        query = "select * from functionsTable where year_of_week(date_col) = 53";
        validateQueryStats(query, session, expectedJmxQueryStats);
        query = "select * from functionsTable where yow(date_col) = 53";
        validateQueryStats(query, session, expectedJmxQueryStats);
        query = "select * from functionsTable where yow(date_col) >= 53";
        validateQueryStats(query, session, expectedJmxQueryStats);
        query = "select count(*) from functionsTable where  year(date_col) = 2003 or  year(date_col) = 2001 or   year(date_col) = 1990";
        validateQueryStats(query, session, expectedJmxQueryStats);
        query = "select * from functionsTable where ceil(double1) in (5, 6, 8, 10)";
        validateQueryStats(query, session, expectedJmxQueryStats);
        query = "select * from functionsTable where ceil(double1) in (5, 6, 8, 10) or ceil(double1) > 90";
        validateQueryStats(query, session, expectedJmxQueryStats);

        //functions with operators
        validateQueryStats("select count(*) from functionsTable where ceil(s_real)<=97 and s_real>=12", session, expectedJmxQueryStats);
        validateQueryStats("select * from functionsTable where cast(char_8_col as varchar(10)) = 'aaa' or cast(char_8_col as varchar(10)) = 'bbb'", session, expectedJmxQueryStats);
        validateQueryStats("select * from functionsTable where cast(double1 as varchar) = '5.0E0' or cast(double1 as varchar) = '6.0E0'", session, expectedJmxQueryStats);
        validateQueryStats("select * from functionsTable where cast(double1 as varchar) = '5.0E0' and cast(double1 as varchar) > '6.0E0'", session, expectedJmxQueryStats);
        validateQueryStats("select * from functionsTable where ceil(double1) = 5  or ceil(double1) > 9", session, expectedJmxQueryStats);
        validateQueryStats("select * from functionsTable where ceil(double1) = 5  or ceil(double1) = 7 or ceil(double1) > 9", session, expectedJmxQueryStats);
        validateQueryStats("select count(*) from functionsTable where ceil(double2)<=14455 OR ceil(double2)>=75767", session, expectedJmxQueryStats);

        query = "select * from functionsTable where ceil(double1) = 5  or ceil(double1) = 10 or ceil(double1) = 30 or is_nan(double1)=true or (double1 < 19 and double1 > 17)";
        expectedJmxQueryStats = Map.of(
                "varada_match_columns", 1L,
                "external_match_columns", 0L);
        validateQueryStats(query, session, expectedJmxQueryStats);

        query = "select * from functionsTable where ceil(double1) = 5  or is_nan(double2)=true";
        expectedJmxQueryStats = Map.of(
                "varada_match_columns", 2L,
                "external_match_columns", 0L);
        validateQueryStats(query, session, expectedJmxQueryStats);

        query = "select * from functionsTable where ceil(double1) = 5  and is_nan(double2)=true";
        expectedJmxQueryStats = Map.of(
                "varada_match_columns", 2L,
                "external_match_columns", 0L);
        validateQueryStats(query, session, expectedJmxQueryStats);
        query = "select * from functionsTable where ceil(double1) = 5  or is_nan(double1)=true";
        expectedJmxQueryStats = Map.of(
                "varada_match_columns", 1L,
                "external_match_columns", 0L);
        validateQueryStats(query, session, expectedJmxQueryStats);
        query = "select * from functionsTable where (ceil(double1) = 5  or is_nan(double1)=true) and (mod(double2 , 2) = 0 and mod(double2 , 3) = 0)";

        validateQueryStats(query, session, expectedJmxQueryStats);

        query = "select * from functionsTable where ceil(double1) = 5  or (double1 < 19 and double1 > 17)";
        validateQueryStats(query, session, expectedJmxQueryStats);
        query = "select * from functionsTable where (ceil(double1) = 5  or is_nan(double1)=true) and (ceil(double2) = 5  or ceil(double2) = 9)";
        expectedJmxQueryStats = Map.of(
                "varada_match_columns", 2L,
                "external_match_columns", 0L);
        validateQueryStats(query, session, expectedJmxQueryStats);

        query = "select * from functionsTable where double1 < 19 and double1 > 17";
        // expression translated to: (ceil(double1) = 5 OR double1 < 19) AND (ceil(double1) = 5 OR double1 > 17)
        expectedJmxQueryStats = Map.of(
                "varada_match_columns", 1L,
                "external_match_columns", 0L);
        validateQueryStats(query, session, expectedJmxQueryStats);

        query = "select * from functionsTable where is_nan(double1)";
        expectedJmxQueryStats = Map.of(
                "varada_match_columns", 1L,
                "external_match_columns", 0L);
        validateQueryStats(query, session, expectedJmxQueryStats);
        query = "select * from functionsTable where ST_CONTAINS(ST_Polygon('polygon((-73.9266974303558 40.7398342228254))'),(ST_Point(double1, double2)))";
        expectedJmxQueryStats = Map.of(
                "varada_match_columns", 0L,
                "external_match_columns", 0L);
        validateQueryStats(query, session, expectedJmxQueryStats);
        query = "select * from functionsTable where is_nan(double1) = false";
        expectedJmxQueryStats = Map.of(
                "varada_match_columns", 0L,
                "external_match_columns", 1L);
        validateQueryStats(query, session, expectedJmxQueryStats, List.of("unsupported_functions_native"));

        query = "select * from functionsTable where true = is_nan(double1)";
        expectedJmxQueryStats = Map.of(
                "varada_match_columns", 1L,
                "external_match_columns", 0L);
        validateQueryStats(query, session, expectedJmxQueryStats);

        query = "select * from functionsTable where ST_EQUALS(ST_Point(double1, double2),ST_Point(n_long_decimal, s_real))";
        expectedJmxQueryStats = Map.of(
                "varada_match_columns", 0L,
                "external_match_columns", 0L);
        validateQueryStats(query, session, expectedJmxQueryStats, List.of("unsupported_functions"));

        query = "select count(*) from functionsTable where double1 = double2";
        validateQueryStats(query, session, expectedJmxQueryStats);

        query = "select * from functionsTable where (( double1 = 4  AND  double2 <= 6 )  OR  double1 = 2)";
        //trino convert to a single domain predicate 2 <= double1 <= 4
        expectedJmxQueryStats = Map.of(
                "varada_match_columns", 2L,
                "external_match_columns", 0L);
        validateQueryStats(query, session, expectedJmxQueryStats);
        query = "select * from functionsTable where (( double1 = 4  AND  double2 <= 6 )  OR  n_long_decimal = 2)"; //composite, currently unsupported
        expectedJmxQueryStats = Map.of(
                "varada_match_columns", 0L,
                "external_match_columns", 0L);
        validateQueryStats(query, session, expectedJmxQueryStats);
        session = Session.builder(getSession())
                .setSystemProperty(catalog + "." + UNSUPPORTED_NATIVE_FUNCTIONS, "ceil")
                .setSystemProperty(catalog + "." + UNSUPPORTED_NATIVE_FUNCTIONS, "ceiling")
                .setSystemProperty(catalog + "." + ENABLE_DEFAULT_WARMING, "true")
                .build();
        query = "select * from functionsTable where ceil(s_real) > 5";
        expectedJmxQueryStats = Map.of(
                "varada_match_columns", 0L,
                "external_match_columns", 1L);
        validateQueryStats(query, session, expectedJmxQueryStats);
        computeActual("DROP TABLE IF EXISTS functionsTable");
    }

    @Test
    public void test_dont_warm_default()
            throws IOException
    {
        try {
            computeActual(getSession(), "INSERT INTO t VALUES (1, 'shlomi')");
            createWarmupRules(DEFAULT_SCHEMA,
                    "t",
                    Map.of(C1, Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, DEFAULT_PRIORITY, DEFAULT_TTL))));
            warmAndValidate(format("select %s, %s from t where %s = 'shlomi' and %s = 1", C1, C2, C2, C1), false, 1, 1);
            RowGroupCountResult ret = getRowGroupCount();
            assertThat(ret.nodesWarmupElementsCount().size()).isEqualTo(1);
            assertThat(ret.warmupColumnNames().contains(format("%s.%s.%s.%s", DEFAULT_SCHEMA, "t", C1, WarmUpType.WARM_UP_TYPE_DATA))).isTrue();
            assertThat(ret.warmupColumnNames().contains(format("%s.%s.%s.%s", DEFAULT_SCHEMA, "t", C1, WarmUpType.WARM_UP_TYPE_BASIC))).isFalse();

            buildAndWarmWideTable(100, true, 0, Optional.empty());
            ret = getRowGroupCount();
            assertThat(ret.warmupColumnNames().contains(format("%s.%s.%s.%s", DEFAULT_SCHEMA, WIDE_TABLE_NAME, "c00", WarmUpType.WARM_UP_TYPE_DATA))).isFalse();
            computeActual("DROP TABLE IF EXISTS " + WIDE_TABLE_NAME);
            computeActual("DROP TABLE IF EXISTS t");

            buildAndWarmWideTable(30, true, 90, Optional.empty());
            Failsafe.with(new RetryPolicy<>()
                            .handle(AssertionError.class)
                            .withMaxRetries(5)
                            .withDelay(Duration.ofSeconds(1))
                            .withMaxDuration(Duration.ofSeconds(3)))
                    .run(() -> {
                        RowGroupCountResult rgCount = getRowGroupCount();
                        assertThat(rgCount.warmupColumnNames().contains(format("%s.%s.%s.%s", DEFAULT_SCHEMA, WIDE_TABLE_NAME, "c010", WarmUpType.WARM_UP_TYPE_DATA))).isTrue();
                    });
        }
        finally {
            computeActual("DROP TABLE IF EXISTS " + WIDE_TABLE_NAME);
            computeActual("DROP TABLE IF EXISTS t");
        }
    }

    @Test
    public void test_never_rule()
            throws IOException
    {
        try {
            computeActual(getSession(), "INSERT INTO t VALUES (1, 'shlomi')");
            createWarmupRules(DEFAULT_SCHEMA,
                    "t",
                    Map.of(C2, Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, NEVER_PRIORITY, DEFAULT_TTL),
                            new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BASIC, 5, DEFAULT_TTL))));
            warmAndValidate(format("select %s from t", C2), true, 1, 1);

            RowGroupCountResult ret = getRowGroupCount();

            assertThat(ret.warmupColumnCount().size()).isEqualTo(1);
            assertThat(ret.warmupColumnNames().contains(format("%s.%s.%s.%s", DEFAULT_SCHEMA, "t", C2, WarmUpType.WARM_UP_TYPE_DATA))).isFalse();
            assertThat(ret.warmupColumnNames().contains(format("%s.%s.%s.%s", DEFAULT_SCHEMA, "t", C2, WarmUpType.WARM_UP_TYPE_BASIC))).isTrue();

            cleanWarmupRules();

            createWarmupRules(DEFAULT_SCHEMA,
                    "t",
                    Map.of(C2, Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, NEVER_PRIORITY, DEFAULT_TTL))));
            warmAndValidate(format("select %s from t", C2), true, "all_elements_warmed_or_skipped");
            ret = getRowGroupCount();
            assertThat(ret.warmupColumnCount().size()).isEqualTo(1);
        }
        finally {
            computeActual("DROP TABLE IF EXISTS t");
        }
    }

    @Test
    public void test_warm_default_with_lucene_rule()
            throws IOException
    {
        computeActual(getSession(), "INSERT INTO t VALUES (1, 'shlomi')");
        createWarmupRules(DEFAULT_SCHEMA,
                "t",
                Map.of(C2, Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_LUCENE, DEFAULT_PRIORITY, DEFAULT_TTL))));
        warmAndValidate("select int1, v1 from t where v1 <> 'shlo' and int1 = 1", true, 4, 2);
        RowGroupCountResult ret = getRowGroupCount();
        assertThat(ret.nodesWarmupElementsCount().size()).isEqualTo(1);
        assertThat(ret.warmupColumnNames().contains(format("%s.%s.%s.%s", DEFAULT_SCHEMA, "t", C1, WarmUpType.WARM_UP_TYPE_DATA))).isTrue();
        assertThat(ret.warmupColumnNames().contains(format("%s.%s.%s.%s", DEFAULT_SCHEMA, "t", C1, WarmUpType.WARM_UP_TYPE_BASIC))).isTrue();
        assertThat(ret.warmupColumnNames().contains(format("%s.%s.%s.%s", DEFAULT_SCHEMA, "t", C2, WarmUpType.WARM_UP_TYPE_DATA))).isTrue();
        assertThat(ret.warmupColumnNames().contains(format("%s.%s.%s.%s", DEFAULT_SCHEMA, "t", C2, WarmUpType.WARM_UP_TYPE_BASIC))).isFalse();
        assertThat(ret.warmupColumnNames().contains(format("%s.%s.%s.%s", DEFAULT_SCHEMA, "t", C2, WarmUpType.WARM_UP_TYPE_LUCENE))).isTrue();
        Session session = Session.builder(getSession())
                .setSystemProperty(catalog + "." + ENABLE_DEFAULT_WARMING, "true")
                .build();
        Map<String, Long> expectedJmxQueryStats = Map.of(
                "varada_match_columns", 1L,
                "external_match_columns", 0L);
        validateQueryStats("select count(*) from t where v1 not like '%mishlomi%'", session, expectedJmxQueryStats);
    }

    @Test
    public void testWarmDefaultLuceneRule()
            throws IOException
    {
        computeActual(getSession(), "INSERT INTO t VALUES (1, 'shlomi')");

        warmAndValidate("select int1, v1 from t where v1 <> 'shlo' and int1 = 1", true, 4, 1);
        RowGroupCountResult ret = getRowGroupCount();
        assertThat(ret.nodesWarmupElementsCount().size()).isEqualTo(1);
        assertThat(ret.warmupColumnNames()).contains(format("%s.%s.%s.%s", DEFAULT_SCHEMA, "t", C1, WarmUpType.WARM_UP_TYPE_DATA));
        assertThat(ret.warmupColumnNames()).contains(format("%s.%s.%s.%s", DEFAULT_SCHEMA, "t", C1, WarmUpType.WARM_UP_TYPE_BASIC));
        assertThat(ret.warmupColumnNames()).contains(format("%s.%s.%s.%s", DEFAULT_SCHEMA, "t", C2, WarmUpType.WARM_UP_TYPE_DATA));
        assertThat(ret.warmupColumnNames()).contains(format("%s.%s.%s.%s", DEFAULT_SCHEMA, "t", C2, WarmUpType.WARM_UP_TYPE_BASIC));
        assertThat(ret.warmupColumnNames()).doesNotContain(format("%s.%s.%s.%s", DEFAULT_SCHEMA, "t", C2, WarmUpType.WARM_UP_TYPE_LUCENE));

        //another basic for v1
        warmAndValidate("select int1, v1 from t where v1 = 'shlomi' and int1 = 1", true, 0, 0);
        //another basic for v1
        warmAndValidate("select int1, v1 from t where v1 > 's' and int1 = 1", true, 0, 0);

        //lucene for v1
        warmAndValidate("select int1, v1 from t where v1 like '%shlo%' and int1 = 1", true, 1, 1);
        ret = getRowGroupCount();
        assertThat(ret.nodesWarmupElementsCount().size()).isEqualTo(1);
        assertThat(ret.warmupColumnNames()).contains(format("%s.%s.%s.%s", DEFAULT_SCHEMA, "t", C1, WarmUpType.WARM_UP_TYPE_DATA));
        assertThat(ret.warmupColumnNames()).contains(format("%s.%s.%s.%s", DEFAULT_SCHEMA, "t", C1, WarmUpType.WARM_UP_TYPE_BASIC));
        assertThat(ret.warmupColumnNames()).contains(format("%s.%s.%s.%s", DEFAULT_SCHEMA, "t", C2, WarmUpType.WARM_UP_TYPE_DATA));
        assertThat(ret.warmupColumnNames()).contains(format("%s.%s.%s.%s", DEFAULT_SCHEMA, "t", C2, WarmUpType.WARM_UP_TYPE_BASIC));
        assertThat(ret.warmupColumnNames()).contains(format("%s.%s.%s.%s", DEFAULT_SCHEMA, "t", C2, WarmUpType.WARM_UP_TYPE_LUCENE));

        //lucene already exists so nothing added
        warmAndValidate("select int1, v1 from t where starts_with(v1, 's') and int1 = 1", true, 0, 0);
    }

    @Test
    public void testWarmDefaultBasicAfterData()
            throws IOException
    {
        computeActual(getSession(), "INSERT INTO t VALUES (1, 'shlomi')");
        warmAndValidate("select int1 from t", true, 1, 1);
        RowGroupCountResult ret = getRowGroupCount();
        assertThat(ret.warmupColumnCount().size()).isEqualTo(1);
        assertThat(ret.warmupColumnNames().stream().filter((name) -> name.contains(WarmUpType.WARM_UP_TYPE_DATA.name())).findFirst()).isNotEmpty();
        warmAndValidate("select int1 from t where int1 <> 5", true, 1, 1);
        ret = getRowGroupCount();
        assertThat(ret.warmupColumnCount().size()).isEqualTo(2);
        assertThat(ret.warmupColumnNames().stream().filter((name) -> name.contains(WarmUpType.WARM_UP_TYPE_BASIC.name())).findFirst()).isNotEmpty();
    }

    @Test
    public void test_warm_default()
            throws IOException
    {
        computeActual(getSession(), "INSERT INTO t VALUES (1, 'shlomi')");
        createWarmupRules(DEFAULT_SCHEMA,
                "t",
                Map.of(C1, Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, DEFAULT_PRIORITY, DEFAULT_TTL)),
                        C2, Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, NEVER_PRIORITY, DEFAULT_TTL))));

        warmAndValidate("select int1, v1 from t where v1 ='shlomi' and int1 = 1", true, 3, 2);
        RowGroupCountResult ret = getRowGroupCount();
        assertThat(ret.nodesWarmupElementsCount().size()).isEqualTo(1);
        assertThat(ret.warmupColumnNames().contains(format("%s.%s.%s.%s", DEFAULT_SCHEMA, "t", C1, WarmUpType.WARM_UP_TYPE_DATA))).isTrue();
        assertThat(ret.warmupColumnNames().contains(format("%s.%s.%s.%s", DEFAULT_SCHEMA, "t", C1, WarmUpType.WARM_UP_TYPE_BASIC))).isTrue();
        assertThat(ret.warmupColumnNames().contains(format("%s.%s.%s.%s", DEFAULT_SCHEMA, "t", C2, WarmUpType.WARM_UP_TYPE_DATA))).isFalse();
        assertThat(ret.warmupColumnNames().contains(format("%s.%s.%s.%s", DEFAULT_SCHEMA, "t", C2, WarmUpType.WARM_UP_TYPE_BASIC))).isTrue();

        WarmupRulesUsageData warmupRulesUsageData = objectMapper.readerFor(new TypeReference<WarmupRulesUsageData>() {})
                .readValue(executeRestCommand(WarmupRuleService.WARMUP_PATH, WarmupTask.TASK_NAME_GET_USAGE, null, HttpMethod.GET, HttpURLConnection.HTTP_OK));

        assertThat(warmupRulesUsageData.warmupColRuleUsageDataList().size()).isEqualTo(2);
        assertThat(warmupRulesUsageData.warmupDefaultRuleUsageDataList().size()).isEqualTo(1);

        demoteAll();
        validateDemoteAll();
        cleanWarmupRules();
        warmAndValidate("select v1 from t where int1 = 1", true, 3, 1);
        ret = getRowGroupCount();
        assertThat(ret.nodesWarmupElementsCount().size()).isEqualTo(1);
        assertThat(ret.warmupColumnNames().contains(format("%s.%s.%s.%s", DEFAULT_SCHEMA, "t", C1, WarmUpType.WARM_UP_TYPE_DATA))).isEqualTo(true);
        assertThat(ret.warmupColumnNames().contains(format("%s.%s.%s.%s", DEFAULT_SCHEMA, "t", C1, WarmUpType.WARM_UP_TYPE_BASIC))).isTrue();
        assertThat(ret.warmupColumnNames().contains(format("%s.%s.%s.%s", DEFAULT_SCHEMA, "t", C2, WarmUpType.WARM_UP_TYPE_DATA))).isTrue();

        demoteAll();
        validateDemoteAll();
        cleanWarmupRules();
        Session session = buildSession(true, true);
        warmAndValidate("select * from t", session, 4, 1, 0);
        ret = getRowGroupCount();
        assertThat(ret.nodesWarmupElementsCount().size()).isEqualTo(1);
        assertThat(ret.warmupColumnNames().contains(format("%s.%s.%s.%s", DEFAULT_SCHEMA, "t", C1, WarmUpType.WARM_UP_TYPE_DATA))).isTrue();
        assertThat(ret.warmupColumnNames().contains(format("%s.%s.%s.%s", DEFAULT_SCHEMA, "t", C1, WarmUpType.WARM_UP_TYPE_BASIC))).isTrue();
        assertThat(ret.warmupColumnNames().contains(format("%s.%s.%s.%s", DEFAULT_SCHEMA, "t", C2, WarmUpType.WARM_UP_TYPE_DATA))).isTrue();
        assertThat(ret.warmupColumnNames().contains(format("%s.%s.%s.%s", DEFAULT_SCHEMA, "t", C2, WarmUpType.WARM_UP_TYPE_BASIC))).isTrue();
    }

    private void validateDemoteAll()
            throws IOException
    {
        String result = executeRestCommand(CACHED_ROW_GROUP, "", null, HttpMethod.GET, HttpURLConnection.HTTP_OK);
        List<Object> cachedRowGroupRes = objectMapper.readerFor(new TypeReference<List<Object>>() {})
                .readValue(result);
        assertThat(cachedRowGroupRes).isEmpty();
        RowGroupCountResult rowGroupCountResult = getRowGroupCount();
        assertThat(rowGroupCountResult.warmupColumnCount()).isEmpty();
    }

    @Test
    public void testWarmupAPI()
            throws IOException
    {
        ImmutableSet<WarmupPredicateRule> predicates = ImmutableSet.of(new PartitionValueWarmupPredicateRule(C1, "1"), new PartitionValueWarmupPredicateRule(C2, "shlomi"));
        ImmutableSet<WarmupPredicateRule> predicates2 = ImmutableSet.of(new PartitionValueWarmupPredicateRule(C1, "2"), new PartitionValueWarmupPredicateRule(C2, "shlomi2"));
        createWarmupRules(DEFAULT_SCHEMA,
                "t",
                Map.of(C1, Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, DEFAULT_PRIORITY, DEFAULT_TTL))),
                predicates);
        createWarmupRules(DEFAULT_SCHEMA,
                "t",
                Map.of(C1, Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, DEFAULT_PRIORITY, DEFAULT_TTL))),
                predicates2);
        assertThat(getWarmupRules().size()).isEqualTo(2);
    }

    @Test
    public void testWarmDoubleIndexOnly()
            throws IOException
    {
        createSchemaAndTable("schema_double", "t_double", format("(%s double, %s varchar(20))", C1, C2));

        computeActual(getSession(), "INSERT INTO schema_double.t_double VALUES (1000000000000, 'shlomi')");

        createWarmupRules("schema_double",
                "t_double",
                Map.of(C1, Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BASIC, DEFAULT_PRIORITY, DEFAULT_TTL)),
                        C2, Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, DEFAULT_PRIORITY, DEFAULT_TTL))));
        warmAndValidate("select * from schema_double.t_double", false, 2, 1);

        MaterializedResult materializedRows = computeActual(getSession(), format("select %s from schema_double.t_double where %s = 1000000000000", C2, C1));
        //return 0 because we don't have native, if returns 1 then got it from hive
        assertThat(materializedRows.getRowCount()).describedAs(materializedRows.toString()).isEqualTo(0);
    }

    @Test
    public void testSimple_WarmOnlyOne()
            throws IOException
    {
        computeActual(getSession(), "INSERT INTO t VALUES (1, 'shlomi')");

        createWarmupRules(DEFAULT_SCHEMA, "t", Map.of(C2, Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, DEFAULT_PRIORITY, DEFAULT_TTL))));
        warmAndValidate("select * from t", false, 1, 1);
    }

    @Test
    public void testWarmPredicatePushdownVarchar()
            throws IOException
    {
        computeActual(getSession(), "INSERT INTO t VALUES (1, 'shlomishlomishlomi')");

        createWarmupRules(DEFAULT_SCHEMA, "t", Map.of(C2, Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, DEFAULT_PRIORITY, DEFAULT_TTL),
                new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BASIC, DEFAULT_PRIORITY, DEFAULT_TTL))));

        warmAndValidate("select v1 from t", false, 2, 1);

        MaterializedResult materializedRows = computeActual(getSession(), "select v1 from t where v1 = 'shlomishlomishlomi'");
        assertThat(materializedRows.getRowCount()).isEqualTo(0); //return 0 because we don't have native, just to check the init of reader
    }

    @Test
    public void testWarmDataOnly()
            throws IOException
    {
        computeActual(getSession(), "INSERT INTO t VALUES (1, 'shlomishlomishlomi')");

        createWarmupRules(DEFAULT_SCHEMA, "t", Map.of(C1, Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BASIC, DEFAULT_PRIORITY, DEFAULT_TTL)),
                C2, Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, DEFAULT_PRIORITY, DEFAULT_TTL))));

        warmAndValidate("select * from t", false, 2, 1);

        @Language("SQL") String query = "select v1 from t where int1 = 1";
        Map<String, Long> expectedQueryStats = Map.of(
                "varada_match_columns", 1L,
                "varada_collect_columns", 1L,
                "prefilled_collect_columns", 0L,
                "external_collect_columns", 0L,
                "external_match_columns", 0L);
        validateQueryStats(query, getSession(), expectedQueryStats);

        query = "select int1 from t where v1 = 'shlomishlomishlomi'";
        expectedQueryStats = Map.of(
                "varada_match_columns", 0L,
                "varada_collect_columns", 0L,
                "prefilled_collect_columns", 0L,
                "external_collect_columns", 2L,
                "external_match_columns", 1L);
        validateQueryStats(query, getSession(), expectedQueryStats);
    }

    // since we have no native and the merge of pages fail. kept it here in case as a scenario
    // that can be debugged(maybe in the future we will have a native stub)
    @Test
    @Disabled
    public void testWarmIndexOnly()
            throws IOException
    {
        computeActual(getSession(), "INSERT INTO t VALUES (1, 'shlomishlomishlomi')");

        createWarmupRules(DEFAULT_SCHEMA, "t", Map.of(C1, Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, DEFAULT_PRIORITY, DEFAULT_TTL)),
                C2, Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BASIC, DEFAULT_PRIORITY, DEFAULT_TTL))));

        warmAndValidate("select * from t", false, 2, 1);

        MaterializedResult materializedRows = computeActual(getSession(), "select int1 from t where v1 = 'shlomishlomishlomi'");
        assertThat(materializedRows.getRowCount()).isEqualTo(0); //return 0 because we don't have native, just to check the init of reader

        materializedRows = computeActual(getSession(), "select v1 from t where v1 = 'shlomishlomishlomi'");
        assertThat(materializedRows.getRowCount()).isEqualTo(1);  // mixed, goes to hive for v1
    }

    @Test
    public void testVarcharMaxFail()
            throws IOException
    {
        int varcharMaxLen = storageEngineModule.getStorageEngineConstants().getVarcharMaxLen();

        assertUpdate("CREATE TABLE varchar_max_table (varchar_max varchar) WITH (format='PARQUET', partitioned_by = ARRAY[])");
        computeActual(getSession(), format("INSERT INTO varchar_max_table VALUES ('%s')", "varcharMaxLen"));
        computeActual(getSession(), format("INSERT INTO varchar_max_table VALUES ('%s')", "varcharMaxLen" + StringUtils.randomAlphanumeric(varcharMaxLen)));

        createWarmupRules(DEFAULT_SCHEMA,
                "varchar_max_table",
                Map.of("varchar_max",
                        Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BASIC, DEFAULT_PRIORITY, DEFAULT_TTL),
                                new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, DEFAULT_PRIORITY, DEFAULT_TTL))));

        warmAndValidate("select * from varchar_max_table",
                getSession(),
                3,
                2,
                Optional.of(1));

        @Language("SQL") String query = "select * from varchar_max_table";
        Map<String, Long> expectedQueryStats = Map.of(
                VARADA_COLLECT_COLUMNS_STAT, 1L,
                EXTERNAL_COLLECT_STAT, 1L);
        validateQueryStats(query, getSession(), expectedQueryStats, OptionalInt.empty(), OptionalInt.of(1));
        assertUpdate("DROP TABLE varchar_max_table");
    }

    @Test
    public void testUTF8CharDataFail()
            throws IOException
    {
        assertUpdate("CREATE TABLE char_128_table (cchar_128 char(1)) WITH (format='PARQUET', partitioned_by = ARRAY[])");

        //each insert cmd is a single parquet file
        computeActual(getSession(), "INSERT INTO char_128_table (cchar_128) VALUES ('G'), ('É')");

        createWarmupRules(DEFAULT_SCHEMA,
                "char_128_table",
                Map.of("cchar_128",
                        Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, DEFAULT_PRIORITY, DEFAULT_TTL))));

        warmAndValidate("select cchar_128 from char_128_table",
                Session.builder(getSession())
                        .setSystemProperty(catalog + "." + ENABLE_DEFAULT_WARMING, Boolean.toString(false))
                        .build(),
                0,
                2,
                2);

        MaterializedResult materializedRows = computeActual(getSession(), "select cchar_128 from char_128_table where cchar_128 is not null");
        // fetch from proxy since warmup failed
        assertThat(materializedRows.getRowCount()).isEqualTo(2);

        assertUpdate("DROP TABLE char_128_table");
    }

    @Test
    public void testUTF8CharBasic()
            throws IOException
    {
        assertUpdate("CREATE TABLE char_128_table (cchar_128 char(1)) WITH (format='PARQUET', partitioned_by = ARRAY[])");

        //each insert cmd is a single parquet file
        computeActual(getSession(), "INSERT INTO char_128_table (cchar_128) VALUES ('G'), ('É')");

        createWarmupRules(DEFAULT_SCHEMA,
                "char_128_table",
                Map.of("cchar_128",
                        Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BASIC, DEFAULT_PRIORITY, DEFAULT_TTL))));

        warmAndValidate("select cchar_128 from char_128_table",
                Session.builder(getSession())
                        .setSystemProperty(catalog + "." + ENABLE_DEFAULT_WARMING, Boolean.toString(false))
                        .build(),
                1,
                1,
                0);

        MaterializedResult materializedRows = computeActual(getSession(), "select cchar_128 from char_128_table where cchar_128 is not null");
        // fetch from proxy since warmup failed
        assertThat(materializedRows.getRowCount()).isEqualTo(0);

        assertUpdate("DROP TABLE char_128_table");
    }

    @Test
    public void testDataIndex()
            throws IOException
    {
        computeActual(getSession(), "INSERT INTO t VALUES (1, 'shlomishlomishlomi')");

        createWarmupRules(DEFAULT_SCHEMA, "t", Map.of(C2, Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, DEFAULT_PRIORITY, DEFAULT_TTL))));

        warmAndValidate("select * from t", false, 1, 1);

        computeActual(getSession(), "select count(v1) from t where v1 <> 'shlomishlomishlomi'");
    }

    @Test
    public void testStringRanges()
            throws IOException
    {
        computeActual(getSession(), "INSERT INTO t VALUES (1, 'shlomishlomishlomi')");

        createWarmupRules(DEFAULT_SCHEMA, "t", Map.of(C2, Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BASIC, DEFAULT_PRIORITY, DEFAULT_TTL))));

        warmAndValidate("select * from t", false, 1, 1);

        // in range
        @Language("SQL") String query = "select count(v1) from t where v1 > 'sh' AND v1 < 'zz'";
        Map<String, Long> expectedQueryStats = Map.of(
                VARADA_MATCH_COLUMNS_STAT, 1L);
        validateQueryStats(query, getSession(), expectedQueryStats);

        // smaller then
        query = "select count(v1) from t where v1 < 'T'";
        expectedQueryStats = Map.of(
                VARADA_MATCH_COLUMNS_STAT, 1L);
        validateQueryStats(query, getSession(), expectedQueryStats);

        // greater then
        query = "select count(v1) from t where v1 > 'T'";
        expectedQueryStats = Map.of(
                VARADA_MATCH_COLUMNS_STAT, 1L);
        validateQueryStats(query, getSession(), expectedQueryStats);

        // prefilled
        query = "select count(int1) from t where v1 >= 'T'";
        expectedQueryStats = Map.of(
                VARADA_MATCH_COLUMNS_STAT, 1L,
                PREFILLED_COLUMNS_STAT, 0L);
        validateQueryStats(query, getSession(), expectedQueryStats);
    }

    @Test
    public void testStringRangesBasicBloom()
            throws IOException
    {
        computeActual(getSession(), "INSERT INTO t VALUES (1, 'shlomishlomishlomi')");

        createWarmupRules(DEFAULT_SCHEMA, "t", Map.of(C1, Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BASIC, DEFAULT_PRIORITY, DEFAULT_TTL),
                        new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BLOOM_LOW, DEFAULT_PRIORITY, DEFAULT_TTL)),
                C2, Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BASIC, DEFAULT_PRIORITY, DEFAULT_TTL),
                        new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BLOOM_LOW, DEFAULT_PRIORITY, DEFAULT_TTL))));

        warmAndValidate("select * from t", false, 4, 1);
        @Language("SQL") String query = "select count(*) from t where v1 >= 'T'";
        Map<String, Long> expectedQueryStats = Map.of(
                VARADA_MATCH_COLUMNS_STAT, 1L,
                PREFILLED_COLUMNS_STAT, 0L);
        validateQueryStats(query, getSession(), expectedQueryStats);
    }

    @Test
    public void testDataIndexWithBloom()
            throws IOException
    {
        assertUpdate("CREATE TABLE pt(some_date date, bigint_2 bigint, tinyint_4 tinyint,t_time timestamp(3)) WITH (format='PARQUET', partitioned_by = ARRAY[])");

        computeActual(getSession(), "INSERT INTO pt VALUES (CAST('2002-04-29' as date), 100, 1, CAST('2002-04-29' as TIMESTAMP))");

        createWarmupRules(DEFAULT_SCHEMA, "pt",
                Map.of("tinyint_4",
                        Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, DEFAULT_PRIORITY, DEFAULT_TTL),
                                new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BASIC, DEFAULT_PRIORITY, DEFAULT_TTL)),
                        "bigint_2",
                        Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, DEFAULT_PRIORITY, DEFAULT_TTL),
                                new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BLOOM_HIGH, DEFAULT_PRIORITY, DEFAULT_TTL)),
                        "some_date",
                        Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, DEFAULT_PRIORITY, DEFAULT_TTL),
                                new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BLOOM_MEDIUM, DEFAULT_PRIORITY, DEFAULT_TTL),
                                new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BASIC, DEFAULT_PRIORITY, DEFAULT_TTL)),
                        "t_time",
                        Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, DEFAULT_PRIORITY, DEFAULT_TTL),
                                new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BLOOM_HIGH, DEFAULT_PRIORITY, DEFAULT_TTL))));

        warmAndValidate("select * from pt", false, 9, 1);

        @Language("SQL") String query = "select count(*) from pt where day_of_week(t_time) = 6";
        //bloom is rejected since bloom doesn't support predicate function
        Map<String, Long> expectedQueryStats = Map.of(
                "prefilled_collect_columns", 0L,
                "varada_match_columns", 0L,
                "varada_collect_columns", 1L,
                "external_match_columns", 1L);
        validateQueryStats(query, getSession(), expectedQueryStats);

        query = "select count(*) from pt where tinyint_4<>-16 and bigint_2 = 8148176510382645782 and tinyint_4=-33";
        expectedQueryStats = Map.of(
                "prefilled_collect_columns", 1L,
                "varada_match_columns", 2L,
                "varada_collect_columns", 1L,
                "external_collect_columns", 0L,
                "external_match_columns", 0L);
        validateQueryStats(query, getSession(), expectedQueryStats);

        query = "select count(*) from pt where year(some_date)>2015 and day(some_date)<26";
        expectedQueryStats = Map.of(
                "varada_match_columns", 1L,
                "varada_collect_columns", 1L,
                "external_collect_columns", 0L,
                "external_match_columns", 0L);
        validateQueryStats(query, getSession(), expectedQueryStats);
        assertUpdate("DROP TABLE pt");
    }

    @Test
    public void testWarmWithAlias()
            throws IOException
    {
        computeActual(getSession(), "INSERT INTO t VALUES (1, 'shlomi')");

        createWarmupRules(DEFAULT_SCHEMA, "t", Map.of(C1, Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, DEFAULT_PRIORITY, DEFAULT_TTL)),
                C2, Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, DEFAULT_PRIORITY, DEFAULT_TTL))));

        warmAndValidate(format("select %s as a_int1, %s as a_v1 from t", C1, C2), false, 2, 1);

        MaterializedResult materializedRows = computeActual(getSession(), format("select %s from t where %s = 'shlomi' or %s = 1", C2, C2, C1));
        assertThat(materializedRows.getRowCount()).isEqualTo(0); //return 0 because we don't have native, just to check the init of reader
    }

    @Test
    public void testWarmAllNullsColumns()
            throws IOException
    {
        computeActual(getSession(), "INSERT INTO t VALUES (1, NULL)");

        createWarmupRules(DEFAULT_SCHEMA, "t", Map.of(C1, Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, DEFAULT_PRIORITY, DEFAULT_TTL)),
                C2, Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, DEFAULT_PRIORITY, DEFAULT_TTL))));

        warmAndValidate(format("select %s as a_int1, %s as a_v1 from t", C1, C2), false, 2, 1);

        @Language("SQL") String query = format("select %s from t where %s is NULL", C2, C2);
        Map<String, Long> expectedQueryStats = Map.of(
                "prefilled_collect_columns", 1L,
                "varada_match_columns", 0L,
                "varada_collect_columns", 0L,
                "external_match_columns", 0L);
        validateQueryStats(query, getSession(), expectedQueryStats);
    }

    @Test
    public void testIncrementalWarm()
            throws IOException
    {
        computeActual(getSession(), "INSERT INTO t VALUES (1, 'shlomi')");

        createWarmupRules(DEFAULT_SCHEMA, "t", Map.of(C1, Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, DEFAULT_PRIORITY, DEFAULT_TTL),
                new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BASIC, DEFAULT_PRIORITY, DEFAULT_TTL))));

        warmAndValidate(format("select %s from t", C1), false, 2, 1);

        MaterializedResult materializedRows = computeActual(getSession(), format("select %s from t where %s = 1", C1, C1));
        assertThat(materializedRows.getRowCount()).isEqualTo(0); //return 0 because we don't have native, just to check the init of reader
        //second column
        createWarmupRules(DEFAULT_SCHEMA, "t", Map.of(C2, Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, DEFAULT_PRIORITY, DEFAULT_TTL),
                new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BASIC, DEFAULT_PRIORITY, DEFAULT_TTL))));

        warmAndValidate(format("select %s from t", C2), false, 2, 1);

        materializedRows = computeActual(getSession(), format("select %s,%s from t where %s = 'shlomi' or %s = 1", C1, C2, C2, C1));
        assertThat(materializedRows.getRowCount()).isEqualTo(0); //return 0 because we don't have native, just to check the init of reader
    }

    @Test
    public void testSimple_AnalyzeWithColumns()
    {
        computeActual(getSession(), "INSERT INTO t VALUES (1, 'shlomi')");
        computeActual(getSession(), "ANALYZE t WITH (columns = ARRAY['int1'])");
        assertQuery("SHOW STATS FOR t",
                "SELECT * FROM VALUES " +
                        "('int1',  null,    1,    0, null, 1, 1), " +
                        "('v1',  6,    1,    0, null, null, null), " +
                        "(null,  null,    null,   null,    1, null, null)");
    }

    @Test
    public void testMixedQueryWithPartitionColumn()
    {
        assertUpdate("CREATE TABLE partitionTable(warmedColumn integer, notWarmedColumn varchar, warmedPartition varchar, notWarmedPartition varchar) WITH (format='PARQUET', partitioned_by = ARRAY['warmedPartition', 'notWarmedPartition'])");
        assertUpdate("INSERT INTO partitionTable(warmedColumn, notWarmedColumn, warmedPartition, notWarmedPartition) VALUES(1, 'a1','partition1', 'partition2')", 1);
        warmAndValidate("select warmedColumn, warmedPartition from partitionTable", true, 2, 1);
        Session session = Session.builder(getSession())
                .setSystemProperty(catalog + "." + ENABLE_DEFAULT_WARMING, Boolean.toString(false))
                .build();

        //query on warmed partition column and not warmed regular column
        @Language("SQL") String query = "select warmedPartition, notWarmedColumn from partitionTable where warmedPartition in ('partition1', 'partition2')";
        Map<String, Long> expectedQueryStats = Map.of(
                "prefilled_collect_columns", 1L,
                "varada_match_columns", 0L,
                "varada_collect_columns", 0L,
                "external_collect_columns", 1L,
                "external_match_columns", 0L);
        validateQueryStats(query, session, expectedQueryStats);

        //query on non warmed partition column and a warmed regular column
        query = "select warmedColumn, notWarmedPartition from partitionTable where notWarmedPartition in ('partition1', 'partition2')";
        expectedQueryStats = Map.of(
                "prefilled_collect_columns", 1L,
                "varada_match_columns", 0L,
                "varada_collect_columns", 1L,
                "external_collect_columns", 0L,
                "external_match_columns", 0L);
        validateQueryStats(query, session, expectedQueryStats);

        //only query on notWarmedPartition, and warmedPartition
        query = "select warmedPartition, notWarmedPartition from partitionTable where notWarmedPartition in ('partition1', 'partition2')";
        expectedQueryStats = Map.of(
                "prefilled_collect_columns", 2L,
                "varada_match_columns", 0L,
                "varada_collect_columns", 0L,
                "external_collect_columns", 0L,
                "external_match_columns", 0L);
        validateQueryStats(query, session, expectedQueryStats);

        //only query on notWarmedPartition, and warmedPartition
        query = "select warmedPartition, notWarmedPartition from partitionTable where warmedPartition in ('partition1', 'partition2')";
        expectedQueryStats = Map.of(
                "prefilled_collect_columns", 2L,
                "varada_match_columns", 0L,
                "varada_collect_columns", 0L,
                "external_collect_columns", 0L,
                "external_match_columns", 0L);
        validateQueryStats(query, session, expectedQueryStats);

        //query on notWarmedPartition
        query = "select notWarmedPartition from partitionTable where notWarmedPartition in ('partition1', 'partition2')";
        expectedQueryStats = Map.of(
                "prefilled_collect_columns", 0L,
                "varada_match_columns", 0L,
                "varada_collect_columns", 0L,
                "external_collect_columns", 1L,
                "external_match_columns", 0L);
        validateQueryStats(query, session, expectedQueryStats);
        assertUpdate("DROP TABLE partitionTable");
    }

    @Test
    public void testSimple_AllPartitionQuery()
    {
        assertUpdate("CREATE TABLE pt(id integer, a varchar, b varchar, ds varchar) WITH (format='PARQUET', partitioned_by = ARRAY['ds'])");
        assertUpdate("INSERT INTO pt(id,a,ds) VALUES(1, 'a1','a1')", 1);
        assertUpdate("INSERT INTO pt(id,a,ds) VALUES(2, 'b1','b1')", 1);
        warmAndValidate("select id, ds from pt", true, 4, 2);

        @Language("SQL") String query = "SELECT COUNT(ds) FROM pt where ds = 'b1'";
        List<String> expectedPositiveQueryStats = List.of(createFixedStatKey(PREFILLED, "ds"));
        validateQueryStats(query, getSession(), Collections.emptyMap(), expectedPositiveQueryStats);

        query = "SELECT COUNT(*) FROM pt where ds = 'b1'";
        Map<String, Long> expectedQueryStats = Map.of("empty_collect_columns", 1L);
        validateQueryStats(query, getSession(), expectedQueryStats);
        assertUpdate("DROP TABLE pt");
    }

    @Test
    @Disabled
    public void testPrefillFakeCollect()
            throws IOException
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(catalog + "." + PREDICATE_SIMPLIFY_THRESHOLD, Integer.toString(1))
                .build();

        computeActual(session, "INSERT INTO t VALUES (1, 'shlomi')");
        createWarmupRules(DEFAULT_SCHEMA,
                "t",
                Map.of(C1, Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BASIC, DEFAULT_PRIORITY, DEFAULT_TTL)),
                        C2, Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BASIC, DEFAULT_PRIORITY, DEFAULT_TTL))));
        warmAndValidate("SELECT count(v1) FROM t WHERE int1 = 1", false, 2, 1);

        QueryRunner.MaterializedResultWithPlan materializedResult = getQueryRunner()
                .executeWithPlan(session, "SELECT count(v1) FROM t WHERE int1 > 1");
        Map<String, Long> customMetrics = getCustomMetrics(materializedResult.queryId(), (DistributedQueryRunner) getQueryRunner());
        assertThat(customMetrics.get("dispatcherPageSource:prefilled_collect_columns")).isEqualTo(1);
        assertThat(customMetrics.get("prefilled:int1")).isEqualTo(1);
        assertThat(customMetrics.get("dispatcherPageSource:varada_match_columns")).isEqualTo(1);
        assertThat(customMetrics.get("dispatcherPageSource:varada_match_on_simplified_domain")).isEqualTo(0);
        assertThat(customMetrics.get("dispatcherPageSource:external_collect_columns")).isEqualTo(1);
        assertThat(customMetrics.get("external-collect:v1")).isEqualTo(1);

        // can't prefill because int1's domain should be simplified to 0-2. int1 will be match-collected instead.
        materializedResult = getQueryRunner()
                .executeWithPlan(session, "SELECT count(v1) FROM t WHERE int1 in (0, 2)");
        customMetrics = getCustomMetrics(materializedResult.queryId(), (DistributedQueryRunner) getQueryRunner());
        assertThat(customMetrics.get("dispatcherPageSource:prefilled_collect_columns")).isEqualTo(0);
        assertThat(customMetrics.get("dispatcherPageSource:varada_collect_columns")).isEqualTo(1);
        assertThat(customMetrics.get("varada-collect:int1:WARM_UP_TYPE_BASIC")).isEqualTo(1);
        assertThat(customMetrics.get("dispatcherPageSource:varada_match_columns")).isEqualTo(1);
        assertThat(customMetrics.get("dispatcherPageSource:varada_match_on_simplified_domain")).isEqualTo(1);
        assertThat(customMetrics.get("dispatcherPageSource:external_collect_columns")).isEqualTo(1);
        assertThat(customMetrics.get("external-collect:v1")).isEqualTo(1);
    }

    @Test
    public void testWildcardCountOnDataCol()
            throws IOException
    {
        computeActual("INSERT INTO t VALUES (1, 'shlomi')");
        createWarmupRules(DEFAULT_SCHEMA, "t", Map.of(C1, Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, DEFAULT_PRIORITY, DEFAULT_TTL))));
        warmAndValidate("select int1 from t", false, 1, 1);

        //goto varada
        MaterializedResult result = computeActual("select int1 from t");
        assertThat(result.getRowCount()).isEqualTo(0);
        result = computeActual("select count(int1) from t");
        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat(result.getMaterializedRows().getFirst().getField(0)).isEqualTo(0L);

        //goto hive
        result = computeActual("select v1 from t");
        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat(result.getMaterializedRows().getFirst().getField(0)).isEqualTo("shlomi");
        result = computeActual("select count(v1) from t");
        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat(result.getMaterializedRows().getFirst().getField(0)).isEqualTo(1L);

        //goto varada
        result = computeActual("select count(*) from t");
        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat(result.getMaterializedRows().getFirst().getField(0)).isEqualTo(1L);
    }

    @Test
    public void testWildcardCountOnIndexCol()
            throws IOException
    {
        computeActual("INSERT INTO t VALUES (1, 'shlomi')");
        createWarmupRules(DEFAULT_SCHEMA, "t", Map.of(C1, Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BASIC, DEFAULT_PRIORITY, DEFAULT_TTL))));
        warmAndValidate("select int1 from t", false, 1, 1);

        //goto hive
        MaterializedResult result = computeActual("select int1 from t");
        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat(result.getMaterializedRows().getFirst().getField(0)).isEqualTo(1);
        result = computeActual("select count(int1) from t");
        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat(result.getMaterializedRows().getFirst().getField(0)).isEqualTo(1L);
        result = computeActual("select v1 from t");
        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat(result.getMaterializedRows().getFirst().getField(0)).isEqualTo("shlomi");
        result = computeActual("select count(v1) from t");
        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat(result.getMaterializedRows().getFirst().getField(0)).isEqualTo(1L);
        result = computeActual("select count(*) from t");
        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat(result.getMaterializedRows().getFirst().getField(0)).isEqualTo(1L);
    }

    @Test
    public void testWarmupApi()
            throws IOException
    {
        List<WarmupColRuleData> result = getWarmupRules();
        assertThat(result).isEmpty();

        WarmupColRuleData warmupColRuleDataLucene = new WarmupColRuleData(0,
                DEFAULT_SCHEMA,
                "t",
                new RegularColumnData(C2),
                WarmUpType.WARM_UP_TYPE_LUCENE,
                0,
                Duration.ofSeconds(0),
                ImmutableSet.of(new PartitionValueWarmupPredicateRule(C2, "2"),
                        new DateSlidingWindowWarmupPredicateRule(C2, 30, "DATE_FORMAT", "")));

        WarmupColRuleData warmupColRuleDataData = new WarmupColRuleData(0,
                DEFAULT_SCHEMA,
                "t",
                new RegularColumnData(C2),
                WarmUpType.WARM_UP_TYPE_DATA,
                0,
                Duration.ofSeconds(0),
                ImmutableSet.of(new PartitionValueWarmupPredicateRule(C2, "2"),
                        new DateSlidingWindowWarmupPredicateRule(C2, 30, "DATE_FORMAT", "")));

        executeRestCommand(WarmupRuleService.WARMUP_PATH, WarmupTask.TASK_NAME_SET, List.of(warmupColRuleDataLucene, warmupColRuleDataData), HttpMethod.POST, HttpURLConnection.HTTP_OK);

        result = objectMapper.readerFor(new TypeReference<List<WarmupColRuleData>>() {})
                .readValue(executeRestCommand(WarmupRuleService.WARMUP_PATH, WarmupRuleService.TASK_NAME_GET, null, HttpMethod.GET, HttpURLConnection.HTTP_OK));

        assertThat(result).hasSize(2);

        WarmupColRuleData warmupColRuleDataResult = result.stream().findFirst().orElse(null);
        assertThat(warmupColRuleDataResult.getId()).isNotEqualTo(0);
        assertThat(warmupColRuleDataResult.getColumn()).isEqualTo(warmupColRuleDataLucene.getColumn());
        assertThat(warmupColRuleDataResult.getPredicates()).isEqualTo(warmupColRuleDataLucene.getPredicates());

        executeRestCommand(WarmupRuleService.WARMUP_PATH, WarmupTask.TASK_NAME_DELETE, result.stream().map(WarmupColRuleData::getId).collect(toList()), HttpMethod.DELETE, HttpURLConnection.HTTP_NO_CONTENT);

        result = getWarmupRules();

        assertThat(result).isEmpty();
    }

    @Test
    public void testLikeQuery()
            throws IOException
    {
        computeActual(getSession(), "INSERT INTO t VALUES (1, 'shlomishlomishlomi')");

        createWarmupRules(DEFAULT_SCHEMA, "t", Map.of(C2, Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, DEFAULT_PRIORITY, DEFAULT_TTL),
                new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_LUCENE, DEFAULT_PRIORITY, DEFAULT_TTL))));

        warmAndValidate("select * from t", false, 2, 1);

        Session session = Session.builder(getSession())
                .setSystemProperty(catalog + "." + ENABLE_DEFAULT_WARMING, "true")
                .build();
        Map<String, Long> expectedJmxQueryStats = Map.of(
                "varada_match_columns", 1L,
                "external_match_columns", 0L);
        validateQueryStats("select v1 from t where v1 = 'shlomishlomishlomi' AND v1 like '%mishlomi%'", session, expectedJmxQueryStats);
        session = Session.builder(getSession())
                .setSystemProperty(catalog + "." + ENABLE_DEFAULT_WARMING, "true")
                .setSystemProperty(catalog + "." + UNSUPPORTED_FUNCTIONS, String.format("%s, %s", LIKE_FUNCTION_NAME.getName(), EQUAL_OPERATOR_FUNCTION_NAME.getName()))
                .build();
        expectedJmxQueryStats = Map.of(
                "varada_match_columns", 0L,
                "external_match_columns", 0L);
        validateQueryStats("select v1 from t where v1 like '%mishlomi%'", session, expectedJmxQueryStats);
    }

    @SuppressWarnings("LanguageMismatch")
    @Test
    public void testLuceneQueryPushdown()
    {
        // We're just checking the pushdown - warmup is not required
        String prefixLikePattern = "prefix%";
        String suffixLikePattern = "%suffix";
        String query = format("SELECT %1$s FROM t WHERE %1$s LIKE '%2$s' AND %1$s LIKE '%3$s'", C2, prefixLikePattern, suffixLikePattern);
        DispatcherTableHandle table = executeWithTableHandle(getSession(), query);

        // Validate the translation to VaradaExpression
        WarpExpression warpExpression = table.getWarpExpression().orElseThrow();
        assertThat(warpExpression.varadaExpressionDataLeaves().size()).isEqualTo(2);

        HiveColumnHandle hiveColumnHandle1 = validateLikeExpression(warpExpression.rootExpression().getChildren().get(0), prefixLikePattern);
        HiveColumnHandle hiveColumnHandle2 = validateLikeExpression(warpExpression.rootExpression().getChildren().get(1), suffixLikePattern);
        assertThat(hiveColumnHandle1).isEqualTo(hiveColumnHandle2);

        // Validate the translation to Domain (done by Trino)
        Type expectedColumnType = VarcharType.createVarcharType(20);
        Range range = Range.range(expectedColumnType, Slices.utf8Slice("prefix"), true, Slices.utf8Slice("prefiy"), false);
        SortedRangeSet sortedRangeSet = SortedRangeSet.copyOf(expectedColumnType, List.of(range));
        Domain domain = Domain.create(sortedRangeSet, false);
        assertThat(table.getFullPredicate().getDomains()).isEqualTo(Optional.of(Map.of(hiveColumnHandle1, domain)));

        // Validate that we go to Native
        MaterializedResult result = computeActual(getSession(), query);
        assertThat(result.getRowCount()).isEqualTo(0); //return 0 because we don't have native
    }

    @Test
    public void testPredicatesWithNullExpression()
            throws IOException
    {
        VarcharType type = VarcharType.createVarcharType(20);
        Session session = getSession();
        computeActual(session, "INSERT INTO t VALUES (1, null)");

        createWarmupRules(DEFAULT_SCHEMA,
                "t",
                Map.of(C1, Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, DEFAULT_PRIORITY, DEFAULT_TTL)),
                        C2, Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BASIC, DEFAULT_PRIORITY, DEFAULT_TTL),
                                new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_LUCENE, DEFAULT_PRIORITY, DEFAULT_TTL))));
        warmAndValidate("select * from t", false, 3, 1);

        DispatcherTableHandle table = executeWithTableHandle(session,
                "select count(*) from t where v1 not like 'b'");
        Domain expectedDomain = Domain.create(SortedRangeSet.copyOf(type, List.of(Range.lessThan(type, Slices.utf8Slice("b")),
                Range.greaterThan(type, Slices.utf8Slice("b")))), false);
        assertPredicate(table, null, null, false, expectedDomain);

        table = executeWithTableHandle(session, "select count(*) from t where lower(v1) IS NULL"); //Domain is empty, getting expression of boolean::null
        assertPredicate(table, null, null, true, Domain.all(type));

        table = executeWithTableHandle(session, "select count(*) from t where v1 is null and lower(v1) IS NULL");
        assertPredicate(table, null, null, false, Domain.onlyNull(type));

        table = executeWithTableHandle(session, "select count(*) from t where v1 is null or lower(v1) IS NULL");
        assertPredicate(table, null, null, true, Domain.onlyNull(type));
    }

    private HiveColumnHandle validateLikeExpression(VaradaExpression likeExpression, String likePattern)
    {
        assertThat(likeExpression).isInstanceOf(VaradaCall.class);
        VaradaCall likeCall = (VaradaCall) likeExpression;
        assertThat(likeCall.getFunctionName()).isEqualTo(LIKE_FUNCTION_NAME.getName());
        assertThat(likeCall.getArguments().size()).isEqualTo(2);
        assertThat(likeCall.getArguments().get(0)).isInstanceOf(VaradaVariable.class);
        assertThat(likeCall.getArguments().get(1)).isInstanceOf(VaradaConstant.class);
        VaradaVariable varadaVariable = (VaradaVariable) likeCall.getArguments().get(0);
        VaradaConstant likeConstant = (VaradaConstant) likeCall.getArguments().get(1);
        HiveColumnHandle hiveColumnHandle = (HiveColumnHandle) varadaVariable.getColumnHandle();
        assertThat(hiveColumnHandle.getBaseColumnName()).isEqualTo(C2);
        assertThat(likeConstant).isEqualTo(new VaradaSliceConstant(Slices.utf8Slice(likePattern), VarcharType.createVarcharType(likePattern.length())));
        return hiveColumnHandle;
    }

    @Test
    public void testReplaceLuceneRuleWithBasic()
            throws IOException
    {
        try {
            computeActual(getSession(), "INSERT INTO t VALUES (1, 'shlomi')");

            createWarmupRules(DEFAULT_SCHEMA,
                    "t",
                    Map.of(C2,
                            Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_LUCENE, DEFAULT_PRIORITY, DEFAULT_TTL),
                                    new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, DEFAULT_PRIORITY, DEFAULT_TTL))));
            warmAndValidate("select * from t", false, 2, 1);
            // Query
            @Language("SQL") String query = "SELECT v1 FROM t WHERE v1 = 'singleValue'";
            Map<String, Long> expectedQueryStats = Map.of(VARADA_MATCH_COLUMNS_STAT, 1L);
            validateQueryStats(query, getSession(), expectedQueryStats);

            // Replace lucene rule with a basic rule
            cleanWarmupRules();
            createWarmupRules(DEFAULT_SCHEMA,
                    "t",
                    Map.of(C2,
                            Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, DEFAULT_PRIORITY, DEFAULT_TTL),
                                    new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BASIC, DEFAULT_PRIORITY, DEFAULT_TTL))));

            // warm (no need to validate - data was already warmed so we won't go to hive)
            warmAndValidate("select * from t", false, 1, 1);

            // Query again, expect the same counters
            query = "SELECT v1 FROM t WHERE v1 = 'singleValue'";
            // (Lucene index may or may not be deleted - either way we should match from Varada since we query for a single value)
            expectedQueryStats = Map.of(VARADA_MATCH_COLUMNS_STAT, 1L);
            validateQueryStats(query, getSession(), expectedQueryStats);
        }
        finally {
            computeActual("DROP TABLE IF EXISTS ext");
        }
    }

    @Test
    public void testMultiNot()
            throws IOException
    {
        computeActual(getSession(), "INSERT INTO t VALUES (1, 'shlomishlomishlomi')");

        createWarmupRules(DEFAULT_SCHEMA,
                "t",
                Map.of(C1,
                        Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, DEFAULT_PRIORITY, DEFAULT_TTL),
                                new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BASIC, DEFAULT_PRIORITY, DEFAULT_TTL))));
        warmAndValidate("select int1 from t", false, 2, 1);

        @Language("SQL") String query = "SELECT int1 FROM t WHERE int1 != 3";
        Map<String, Long> expectedQueryStats = Map.of(VARADA_MATCH_COLUMNS_STAT, 1L);
        validateQueryStats(query, getSession(), expectedQueryStats);

        createWarmupRules(DEFAULT_SCHEMA,
                "t",
                Map.of(C2,
                        Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, DEFAULT_PRIORITY, DEFAULT_TTL),
                                new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BASIC, DEFAULT_PRIORITY, DEFAULT_TTL))));
        warmAndValidate(format("select %s from t", C2), false, 2, 1);

        query = format("SELECT %s FROM t WHERE %s NOT IN ('shlomi', 'alfasi', 'aaa')", C2, C2);
        expectedQueryStats = Map.of(VARADA_MATCH_COLUMNS_STAT, 1L);
        validateQueryStats(query, getSession(), expectedQueryStats);

        query = format("SELECT %s FROM t WHERE %s <> 'shlomi'", C2, C2);
        expectedQueryStats = Map.of(VARADA_MATCH_COLUMNS_STAT, 1L);
        validateQueryStats(query, getSession(), expectedQueryStats);
    }

    @Test
    public void testBasicLuceneDataQueryPushdown()
            throws IOException
    {
        computeActual(getSession(), "INSERT INTO t VALUES (1, 'shlomishlomishlomi')");

        createWarmupRules(DEFAULT_SCHEMA,
                "t",
                Map.of(C1,
                        Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, DEFAULT_PRIORITY, DEFAULT_TTL)),
                        C2,
                        Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, DEFAULT_PRIORITY, DEFAULT_TTL),
                                new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BASIC, DEFAULT_PRIORITY, DEFAULT_TTL),
                                new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_LUCENE, DEFAULT_PRIORITY, DEFAULT_TTL))));

        warmAndValidate("select int1,v1 from t", false, 4, 1);

        MaterializedResult materializedRows = computeActual(getSession(), "select v1 from t where v1 = 'shlomishlomishlomi'");
        assertThat(materializedRows.getRowCount()).isEqualTo(0);

        materializedRows = computeActual(getSession(), "select v1 from t where v1 like '%mishlomi%'");
        assertThat(materializedRows.getRowCount()).isEqualTo(0); //return 0 because we don't have native

        materializedRows = computeActual(getSession(), "select v1 from t where v1 = 'shlomishlomishlomi' AND v1 like '%mishlomi%'");
        assertThat(materializedRows.getRowCount()).isEqualTo(0); //return 0 because we don't have native
    }

    @Test
    public void testMixedLuceneQueryPushdown()
            throws IOException
    {
        assertUpdate("CREATE TABLE luceneTable(" +
                "luceneColumn varchar, " +
                "basicColumn varchar, " +
                "luceneAndBasic varchar) WITH (format='PARQUET', partitioned_by = ARRAY[])");

        computeActual(getSession(), "INSERT INTO luceneTable VALUES (" +
                "'shlomi', " +
                "'assaf', " +
                "'tzachi')");
        createWarmupRules(DEFAULT_SCHEMA,
                "luceneTable",
                Map.of("luceneColumn",
                        Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_LUCENE, DEFAULT_PRIORITY, DEFAULT_TTL)),
                        "basicColumn",
                        Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BASIC, DEFAULT_PRIORITY, DEFAULT_TTL)),
                        "luceneAndBasic",
                        Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_LUCENE, DEFAULT_PRIORITY, DEFAULT_TTL),
                                new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BASIC, DEFAULT_PRIORITY, DEFAULT_TTL))));

        warmAndValidate("select * from luceneTable", false, 4, 1);

        List<String> predicates = List.of(
                "where luceneColumn like '%mishlomi%'",
                "where luceneAndBasic like '%mishlomi%'",
                "where luceneColumn > 'a' and luceneColumn like '%mishlomi%'",
                "where basicColumn > 'a' and basicColumn like '%mishlomi%'",
                "where luceneAndBasic > 'a' and luceneAndBasic like '%mishlomi%'");

        Map<String, Long> expectedQueryStats = Map.of(
                "varada_match_columns", 1L,
                "varada_collect_columns", 0L,
                "external_collect_columns", 1L,
                "external_match_columns", 0L);
        for (String predicate : predicates) {
            @Language("SQL") String query = "select count(*) from luceneTable " + predicate;
            validateQueryStats(query, getSession(), expectedQueryStats);
        }
        computeActual("DROP TABLE IF EXISTS luceneTable");
    }

    @Test
    public void testLikeBeforeAndAfterLuceneWarmupIsAdded()
            throws IOException
    {
        // warmup v1 with data+basic and validate that like query is not matched by Varada
        // then update warmups with +lucene, and validate that the query is matched by Varada

        // warmup v1 with data+basic
        computeActual(getSession(), "INSERT INTO t VALUES (1, 'shlomishlomishlomi')");
        createWarmupRules(DEFAULT_SCHEMA,
                "t",
                Map.of(C2,
                        Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, DEFAULT_PRIORITY, DEFAULT_TTL),
                                new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BASIC, DEFAULT_PRIORITY, DEFAULT_TTL))));
        warmAndValidate("select * from t", false, 2, 1);

        // Lucene is not warmed
        @Language("SQL") String query = "select v1 from t where v1 = 'shlomishlomishlomi'";
        Map<String, Long> expectedQueryStats = Map.of(VARADA_MATCH_COLUMNS_STAT, 1L);
        validateQueryStats(query, getSession(), expectedQueryStats);

        query = "select v1 from t where v1 like '%mishlomi%'";
        expectedQueryStats = Map.of(
                VARADA_MATCH_COLUMNS_STAT, 0L,
                VARADA_COLLECT_COLUMNS_STAT, 1L);
        validateQueryStats(query, getSession(), expectedQueryStats);

        // Add a lucene rule
        cleanWarmupRules();
        createWarmupRules(DEFAULT_SCHEMA,
                "t",
                Map.of(C2,
                        Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, DEFAULT_PRIORITY, DEFAULT_TTL),
                                new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BASIC, DEFAULT_PRIORITY, DEFAULT_TTL),
                                new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_LUCENE, DEFAULT_PRIORITY, DEFAULT_TTL))));

        // warm (no need to validate - data was already warmed so we won't go to hive)
        warmAndValidate("select * from t", false, 1, 1);

        // Lucene is warmed
        query = "select v1 from t where v1 = 'shlomishlomishlomi'";
        expectedQueryStats = Map.of(
                VARADA_MATCH_COLUMNS_STAT, 1L);
        validateQueryStats(query, getSession(), expectedQueryStats);

        query = "select v1 from t where v1 like '%mishlomi%'";
        expectedQueryStats = Map.of(
                VARADA_MATCH_COLUMNS_STAT, 1L);
        validateQueryStats(query, getSession(), expectedQueryStats);
    }

    @Test
    @Disabled
    public void testWarmupDemoterStartExe()
            throws IOException
    {
        try {
            WarmupDemoterData warmupDemoterData = WarmupDemoterData.builder()
                    .maxUsageThresholdInPercentage(DEMOTE_CLEAN_UP_USAGE)
                    .cleanupUsageThresholdInPercentage(DEMOTE_CLEAN_UP_USAGE)
                    .executeDemoter(true)
                    .build();
            Map<String, Object> res = demote(warmupDemoterData);
            long totalUsage = ((Number) res.entrySet()
                    .stream()
                    .filter(entry -> entry.getKey().contains(WorkerWarmupDemoterTask.TOTAL_USAGE_THRESHOLD_KEY))
                    .findAny()
                    .orElseThrow()
                    .getValue())
                    .longValue();
            long currentUsage = ((Number) res.entrySet()
                    .stream()
                    .filter(entry -> entry.getKey().contains(WorkerWarmupDemoterTask.CURRENT_USAGE_THRESHOLD_KEY))
                    .findAny()
                    .orElseThrow()
                    .getValue())
                    .longValue();
            double maxUsageThreshold = ((Number) res.entrySet()
                    .stream()
                    .filter(entry -> entry.getKey().contains(WorkerWarmupDemoterTask.MAX_USAGE_THRESHOLD_KEY))
                    .findAny()
                    .orElseThrow()
                    .getValue())
                    .doubleValue();
            double cleanupUsageThreshold = ((Number) res.entrySet()
                    .stream()
                    .filter(entry -> entry.getKey().contains(WorkerWarmupDemoterTask.CLEANUP_USAGE_THRESHOLD_KEY))
                    .findAny()
                    .orElseThrow()
                    .getValue())
                    .doubleValue();
            assertThat(currentUsage).isEqualTo(0);
            assertThat(maxUsageThreshold).isEqualTo(0d);
            assertThat(cleanupUsageThreshold).isEqualTo(0d);
            warmupDemoterData = WarmupDemoterData.builder()
                    .maxUsageThresholdInPercentage(DEMOTE_CLEAN_UP_USAGE)
                    .cleanupUsageThresholdInPercentage(DEMOTE_CLEAN_UP_USAGE)
                    .resetHighestPriority(true)
                    .build();
            demote(warmupDemoterData);

            Session jmxSession = createJmxSession();
            buildAndWarmWideTable(10, false, 30, Optional.empty());

            MaterializedResult jmx0 = computeActual(
                    jmxSession,
                    "select sum(currentUsage), sum(totalUsage) from \"%s*%s\"".formatted(
                            VaradaStatsWarmupDemoter.class.getPackageName(),
                            VaradaStatsWarmupDemoter.class.getSimpleName().toLowerCase(Locale.ROOT)));
            long jmxUsageStart = (long) jmx0.getMaterializedRows().getFirst().getField(0);
            long jmxTotalUsage = (long) jmx0.getMaterializedRows().getFirst().getField(1);
            assertThat(jmxTotalUsage).isEqualTo(totalUsage);
            warmupDemoterData = WarmupDemoterData.builder()
                    .batchSize(2)
                    .executeDemoter(true)
                    .warmupDemoterThreshold(new WarmupDemoterThreshold(0.95, 0.7))
                    .build();

            res = demote(warmupDemoterData);

            MaterializedResult jmx1 = computeActual(
                    jmxSession,
                    "select sum(currentUsage) from \"%s*%s\"".formatted(
                            VaradaStatsWarmupDemoter.class.getPackageName(),
                            VaradaStatsWarmupDemoter.class.getSimpleName().toLowerCase(Locale.ROOT)));
            long jmxUsageEnd = (long) jmx1.getMaterializedRows().getFirst().getField(0);
            assertThat(jmxUsageEnd).isLessThan(jmxUsageStart);
            Integer deadObjectsDeletedCount = (Integer) res.entrySet()
                    .stream()
                    .filter(entry -> entry.getKey().endsWith("dead_objects_deleted"))
                    .findAny()
                    .orElseThrow()
                    .getValue();
            Integer deletedByLowPriorityCount = (Integer) res.entrySet()
                    .stream()
                    .filter(entry -> entry.getKey().endsWith("deleted_by_low_priority"))
                    .findAny()
                    .orElseThrow()
                    .getValue();
            Double highestPriority = (Double) res.entrySet()
                    .stream()
                    .filter(entry -> entry.getKey().endsWith(WorkerWarmupDemoterTask.HIGHEST_PRIORITY_KEY))
                    .findAny()
                    .orElseThrow()
                    .getValue();
            currentUsage = ((Number) res.entrySet()
                    .stream()
                    .filter(entry -> entry.getKey().contains(WorkerWarmupDemoterTask.CURRENT_USAGE_THRESHOLD_KEY))
                    .findAny()
                    .orElseThrow()
                    .getValue())
                    .longValue();
            long totalUsageEnd = ((Number) res.entrySet()
                    .stream()
                    .filter(entry -> entry.getKey().contains(WorkerWarmupDemoterTask.TOTAL_USAGE_THRESHOLD_KEY))
                    .findAny()
                    .orElseThrow()
                    .getValue())
                    .longValue();
            assertThat(totalUsage).isEqualTo(totalUsageEnd);
            assertThat(currentUsage).isEqualTo(jmxUsageEnd);
            assertThat(deletedByLowPriorityCount).isGreaterThan(1);
            assertThat(deadObjectsDeletedCount).isGreaterThan(1);
            assertThat(highestPriority).isLessThan(7.5);
        }
        finally {
            computeActual("DROP TABLE IF EXISTS " + WIDE_TABLE_NAME);
        }
    }

    @Test
    public void testWarmupDemoteAutomatic()
            throws IOException
    {
        try {
            Session jmxSession = createJmxSession();
            MaterializedResult jmx0 = computeActual(
                    jmxSession,
                    "select sum(number_of_runs), sum(deleted_by_low_priority) from \"%s*%s\"".formatted(
                            VaradaStatsWarmupDemoter.class.getPackageName(),
                            VaradaStatsWarmupDemoter.class.getSimpleName().toLowerCase(Locale.ROOT)));
            long numOfRuns0 = (long) jmx0.getMaterializedRows().getFirst().getField(0);
            long numberOfDeletedByLowPrio0 = (long) jmx0.getMaterializedRows().getFirst().getField(1);
            buildAndWarmWideTable(10, true, 30, Optional.empty());
            WarmupDemoterData warmupDemoterData = WarmupDemoterData.builder()
                    .batchSize(2)
                    .executeDemoter(false)
                    .modifyConfiguration(true)
                    .warmupDemoterThreshold(new WarmupDemoterThreshold(0.95, 0.6))
                    .build();
            demote(warmupDemoterData);

            MaterializedResult jmx1 = computeActual(
                    jmxSession,
                    "select sum(number_of_runs), sum(deleted_by_low_priority), sum(number_of_calls), sum(number_of_runs_fail) from \"%s*%s\"".formatted(
                            VaradaStatsWarmupDemoter.class.getPackageName(),
                            VaradaStatsWarmupDemoter.class.getSimpleName().toLowerCase(Locale.ROOT)));
            long numOfRuns1 = (long) jmx1.getMaterializedRows().getFirst().getField(0);
            long numberOfDeletedByLowPrio1 = (long) jmx1.getMaterializedRows().getFirst().getField(1);
            long numberOfCalls = (long) jmx1.getMaterializedRows().getFirst().getField(2);
            long numberOfFails = (long) jmx1.getMaterializedRows().getFirst().getField(3);
            assertThat(numOfRuns0).isEqualTo(numOfRuns1);
            assertThat(numberOfDeletedByLowPrio0).isEqualTo(numberOfDeletedByLowPrio1);
            computeActual(getSession(), "INSERT INTO t VALUES (1, 'shlomishlomishlomi')");
            warmAndValidateLazyDemote("select * from t", true);
            runWithRetries(() -> {
                MaterializedResult jmx2 = computeActual(
                        jmxSession,
                        "select sum(number_of_runs), sum(deleted_by_low_priority), sum(number_of_calls), sum(number_of_runs_fail) from \"%s*%s\"".formatted(
                                VaradaStatsWarmupDemoter.class.getPackageName(),
                                VaradaStatsWarmupDemoter.class.getSimpleName().toLowerCase(Locale.ROOT)));
                long numOfRuns2 = (long) jmx2.getMaterializedRows().getFirst().getField(0);
                long numberOfCalls2 = (long) jmx2.getMaterializedRows().getFirst().getField(2);
                long numberOfFails2 = (long) jmx2.getMaterializedRows().getFirst().getField(3);
                assertThat(numberOfFails2).isEqualTo(numberOfFails);
                assertThat(numberOfCalls2).isGreaterThan(numberOfCalls);
                assertThat(numOfRuns2 - 1).isEqualTo(numOfRuns1);
            });
        }
        finally {
            computeActual("DROP TABLE IF EXISTS " + WIDE_TABLE_NAME);
        }
    }

    @Test
    public void test_export()
            throws IOException
    {
        try {
            final String schemaName = "varada";
            final String tableName = "ext";
            assertUpdate("CREATE SCHEMA " + schemaName);
            computeActual(format("CREATE TABLE %s.%s(c1 varchar, c2 varchar, c3 varchar) WITH (format='PARQUET', partitioned_by = ARRAY[])", schemaName, tableName));
            computeActual(getSession(), format("INSERT INTO %s.%s VALUES ('import', 'export', 'test')", schemaName, tableName));
            createWarmupRules(schemaName,
                    tableName,
                    Map.of("c1",
                            Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, DEFAULT_PRIORITY, DEFAULT_TTL),
                                    new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BASIC, DEFAULT_PRIORITY, DEFAULT_TTL),
                                    new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_LUCENE, DEFAULT_PRIORITY, DEFAULT_TTL)),
                            "c2",
                            Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, DEFAULT_PRIORITY, DEFAULT_TTL),
                                    new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_LUCENE, DEFAULT_PRIORITY, DEFAULT_TTL))));
            Session session = Session.builder(getSession())
                    .setSystemProperty(catalog + ".enable_import_export", "true")
                    .build();
            int expectedWarmupElements = 5;
            warmAndValidateWithExport(format("select count(c1), count(c2) from %s.%s", schemaName, tableName),
                    session,
                    expectedWarmupElements,
                    1,
                    1);
        }
        finally {
            computeActual("DROP TABLE IF EXISTS varada.ext");
        }
    }

    @Test
    public void testWarmupDemoterWithFilterShouldDemoteOnlyByTable()
            throws IOException
    {
        try {
            int numberOfColumns = 3;
            int expectedElementCount = 9;
            buildAndWarmWideTable(numberOfColumns, false, expectedElementCount, Optional.empty());

            List<WarmupDemoterWarmupElementData> warmupDemoterWarmupElementDataList = new ArrayList<>(expectedElementCount);
            IntStream.range(0, numberOfColumns).forEach(columnId -> {
                warmupDemoterWarmupElementDataList.add(new WarmupDemoterWarmupElementData("c0" + columnId, Collections.emptyList()));
                warmupDemoterWarmupElementDataList.add(new WarmupDemoterWarmupElementData("c1" + columnId, Collections.emptyList()));
                warmupDemoterWarmupElementDataList.add(new WarmupDemoterWarmupElementData("c2" + columnId, Collections.emptyList()));
            });
            WarmupDemoterData warmupDemoterData = WarmupDemoterData.builder()
                    .maxUsageThresholdInPercentage(31d)
                    .cleanupUsageThresholdInPercentage(21d)
                    .batchSize(10)
                    .executeDemoter(true)
                    .forceExecuteDeadObjects(true)
                    .schemaTableName(new SchemaTableName(DEFAULT_SCHEMA, WIDE_TABLE_NAME))
                    .warmupElementsData(warmupDemoterWarmupElementDataList)
                    .resetHighestPriority(true)
                    .build();
            Map<String, Object> res = demote(warmupDemoterData);
            Integer deadObjectsDeletedCount = (Integer) res.entrySet()
                    .stream()
                    .filter(entry -> entry.getKey().endsWith("dead_objects_deleted"))
                    .findAny()
                    .orElseThrow()
                    .getValue();
            Integer deletedByLowPriorityCount = (Integer) res.entrySet()
                    .stream()
                    .filter(entry -> entry.getKey().endsWith("deleted_by_low_priority"))
                    .findAny()
                    .orElseThrow()
                    .getValue();
            Double highestPriority = (Double) res.entrySet()
                    .stream()
                    .filter(entry -> entry.getKey().endsWith(WorkerWarmupDemoterTask.HIGHEST_PRIORITY_KEY))
                    .findAny()
                    .orElseThrow()
                    .getValue();
            assertThat(highestPriority).isEqualTo(0);
            assertThat(deadObjectsDeletedCount).isEqualTo(9);
            assertThat(deletedByLowPriorityCount).isEqualTo(0);
        }
        finally {
            computeActual("DROP TABLE IF EXISTS " + WIDE_TABLE_NAME);
        }
    }

    @Test
    public void testSimpleWarmupSyncDemoter()
            throws IOException
    {
        try {
            buildAndWarmWideTable(3, false, 9, Optional.of(Duration.ofMinutes(0)));
            WarmupDemoterData warmupDemoterData = WarmupDemoterData.builder()
                    .maxUsageThresholdInPercentage(31d)
                    .cleanupUsageThresholdInPercentage(21d)
                    .batchSize(10)
                    .executeDemoter(true)
                    .forceExecuteDeadObjects(true)
                    .forceDeleteFailedObjects(true)
                    .modifyConfiguration(true)
                    .build();
            Map<String, Object> res = demote(warmupDemoterData);

            Integer deadObjectsDeletedCount = (Integer) res.entrySet()
                    .stream()
                    .filter(entry -> entry.getKey().endsWith("dead_objects_deleted"))
                    .findAny()
                    .orElseThrow()
                    .getValue();
            Integer deletedByLowPriorityCount = (Integer) res.entrySet()
                    .stream()
                    .filter(entry -> entry.getKey().endsWith("deleted_by_low_priority"))
                    .findAny()
                    .orElseThrow()
                    .getValue();
            assertThat(deadObjectsDeletedCount).isEqualTo(9);
            assertThat(deletedByLowPriorityCount).isEqualTo(0);
        }
        finally {
            computeActual("DROP TABLE IF EXISTS " + WIDE_TABLE_NAME);
        }
    }

    @Test
    public void testWarmUnsupportedColTypes()
    {
        assertUpdate("CREATE TABLE schema.test_table(" +
                "intCol integer, " +
                "rowCol ROW(latitudedeg varchar, longitudedeg double), " +
                "mapCol MAP(varchar(3), integer), " +
                "arrayCol ARRAY(integer))");
        computeActual("INSERT INTO schema.test_table select " +
                "7, " +
                "CAST(ROW('x', 4.5) AS ROW(latitudedeg varchar, longitudedeg double)), " +
                "MAP(ARRAY['foo', 'bar'], ARRAY[1, 2]), " +
                "ARRAY[1]");

        Session session = Session.builder(getSession())
                .setSystemProperty(catalog + "." + ENABLE_DEFAULT_WARMING, "true")
                .build();
        warmAndValidate("select intCol, rowCol, mapCol, arrayCol from schema.test_table where rowCol.latitudedeg = 'x' and rowCol.longitudedeg > 2", session, 6, 1, 0);
        computeActual("select count(rowCol.latitudedeg), count(rowCol.longitudedeg) from schema.test_table where rowCol.latitudedeg = 'x'");
        assertUpdate("DROP TABLE test_table");
    }

    @Test
    public void testWarmInternalRowFields()
            throws IOException
    {
        assertUpdate("CREATE TABLE schema.test_table(" +
                "rowCol ROW(latitudedeg varchar, longitudedeg double))");
        computeActual("INSERT INTO schema.test_table select " +
                "CAST(ROW('x', 4.5) AS ROW(latitudedeg varchar, longitudedeg double))");

        createWarmupRules(DEFAULT_SCHEMA,
                "test_table",
                Map.of("rowCol#latitudedeg", Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, DEFAULT_PRIORITY, DEFAULT_TTL),
                                new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BASIC, DEFAULT_PRIORITY, DEFAULT_TTL)),
                        "rowCol#longitudedeg", Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, DEFAULT_PRIORITY, DEFAULT_TTL))));

        Session session = buildSession(false, false);
        warmAndValidate("select rowCol.latitudedeg, rowCol.longitudedeg from schema.test_table", session, 3, 1, 0);

        RowGroupCountResult ret = getRowGroupCount();
        assertThat(ret.warmupColumnCount().size()).isEqualTo(3);
        assertThat(ret.warmupColumnNames().contains(format("%s.%s.%s.%s", DEFAULT_SCHEMA, "test_table", "rowcol#latitudedeg", WarmUpType.WARM_UP_TYPE_DATA))).isTrue();
        assertThat(ret.warmupColumnNames().contains(format("%s.%s.%s.%s", DEFAULT_SCHEMA, "test_table", "rowcol#latitudedeg", WarmUpType.WARM_UP_TYPE_BASIC))).isTrue();
        assertThat(ret.warmupColumnNames().contains(format("%s.%s.%s.%s", DEFAULT_SCHEMA, "test_table", "rowcol#longitudedeg", WarmUpType.WARM_UP_TYPE_DATA))).isTrue();
        assertThat(ret.warmupColumnNames().contains(format("%s.%s.%s.%s", DEFAULT_SCHEMA, "test_table", "rowcol#longitudedeg", WarmUpType.WARM_UP_TYPE_BASIC))).isFalse();

        MaterializedResult materializedRows = computeActual(session, "select rowCol.latitudedeg, rowCol.longitudedeg from schema.test_table where rowCol.latitudedeg = 'x'");
        assertThat(materializedRows.getRowCount()).isEqualTo(0); //return 0 because we don't have native, just to check the init of reader

        assertUpdate("DROP TABLE test_table");
    }

    @Test
    public void testNeverRuleShouldNotWarmUnsupportColumnType()
            throws IOException
    {
        computeActual("CREATE TABLE bbb (a ARRAY(ROW(b integer, c varchar)), d varchar) WITH (format='PARQUET', partitioned_by = ARRAY['d'])");
        computeActual("INSERT INTO bbb values (array[row(1, 'abc')], 'a')");
        Map<String, Set<WarmupPropertiesData>> rules = new HashMap<>();
        rules.put("a", Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, NEVER_PRIORITY, DEFAULT_TTL),
                new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BASIC, NEVER_PRIORITY, DEFAULT_TTL)));

        createWarmupRules(DEFAULT_SCHEMA, "bbb", rules);

        Session session1 = buildSession(true, false);
        warmAndValidate("select * from bbb", session1, 1, 1, 0);

        RowGroupCountResult result = getRowGroupCount();
        assertThat(result.nodesWarmupElementsCount().size()).isEqualTo(1);

        String str = executeRestCommand(RowGroupTask.ROW_GROUP_PATH, RowGroupTask.ROW_GROUP_COUNT_WITH_FILES_TASK_NAME, null, HttpMethod.GET, HttpURLConnection.HTTP_OK);
        result = objectMapper.readerFor(RowGroupCountResult.class).readValue(str);
        assertThat(result.warmupColumnNames()).containsExactly("schema.bbb.d.WARM_UP_TYPE_DATA");
        computeActual("DROP TABLE bbb");
    }

    /**
     * 2 splits with same values, will create and export dictionary only once
     */
    @Test
    public void testExportedDictionaries()
    {
        String tableName = "dictionary_exporter_test";
        computeActual(format("CREATE TABLE  %s (int1 bigint)", tableName));
        computeActual(getSession(), "INSERT INTO dictionary_exporter_test VALUES (1)"); //split1
        computeActual(getSession(), "INSERT INTO dictionary_exporter_test VALUES (1)"); //split2

        Session session = Session.builder(getSession())
                .setSystemProperty(catalog + "." + ENABLE_DEFAULT_WARMING, Boolean.toString(true))
                .setSystemProperty(catalog + ".enable_import_export", "true")
                .build();
        int createdDictionaries = 1; //same value for all splits
        int expectedExportRowGroupsAccomplished = 2;
        int expectedWarmupElements = 2; // 2 rowGroups, each hold single column
        int expectedWarmFinished = 2;
        Session jmxSession = createJmxSession();
        MaterializedResult dictionaryStats = computeActual(jmxSession, "select sum(dictionary_max_exception_count), sum(write_dictionaries_count), sum(dictionary_read_elements_count) from \"*dictionary*\"");
        long beforeDictionaryMaxExceptionCount = (long) dictionaryStats.getMaterializedRows().getFirst().getField(0);
        long beforeWriteDictionaryCount = (long) dictionaryStats.getMaterializedRows().getFirst().getField(1);
        long expectedReadDictionaryCount = (long) dictionaryStats.getMaterializedRows().getFirst().getField(2);
        warmAndValidateWithExport("select * from dictionary_exporter_test",
                session,
                expectedWarmupElements,
                expectedWarmFinished,
                expectedExportRowGroupsAccomplished);
        validateDictionaryStats(jmxSession, beforeDictionaryMaxExceptionCount, beforeWriteDictionaryCount + createdDictionaries, expectedReadDictionaryCount);
        computeActual("DROP TABLE dictionary_exporter_test");
    }

    @Test
    public void testDictionary()
            throws IOException
    {
        String tableName = "dictionary_test_1";
        computeActual(format("CREATE TABLE  %s (bingint1 bigint, var1 varchar, char1 char(5), int1 integer, shortdecimal decimal(2,1))", tableName));

        computeActual(getSession(), "INSERT INTO dictionary_test_1 VALUES (1, 'tzachi', 'bla', 2, 3)");

        Session session = Session.builder(getSession())
                .setSystemProperty(catalog + "." + ENABLE_DEFAULT_WARMING, Boolean.toString(true))
                .setSystemProperty(catalog + "." + "enable_import_export", "true")
                .build();
        int createdDictionaries = 5;
        Session jmxSession = createJmxSession();
        MaterializedResult dictionaryStats = computeActual(jmxSession, "select sum(dictionary_max_exception_count), sum(write_dictionaries_count), sum(dictionary_read_elements_count) from \"*dictionary*\"");
        long beforeDictionaryMaxExceptionCount = (long) dictionaryStats.getMaterializedRows().getFirst().getField(0);
        long beforeWriteDictionaryCount = (long) dictionaryStats.getMaterializedRows().getFirst().getField(1);
        long expectedReadDictionaryCount = (long) dictionaryStats.getMaterializedRows().getFirst().getField(2);
//        int beforeWarmDictionaryUsage = getDictionariesUsage();
        warmAndValidateWithExport("select * from dictionary_test_1",
                session,
                createdDictionaries,
                1,
                1);
        validateDictionaryStats(jmxSession, beforeDictionaryMaxExceptionCount, beforeWriteDictionaryCount + createdDictionaries, expectedReadDictionaryCount);
//        int dictionariesUsedPages = getDictionariesUsage();
//        int expectedUsedPages = 5; //each dictionary entry is a page (mock)
//        assertThat(dictionariesUsedPages - beforeWarmDictionaryUsage).isEqualTo(expectedUsedPages);
        String executeRestCommand = executeRestCommand(DictionaryTask.DICTIONARY_PATH, DictionaryTask.DICTIONARY_COUNT_AGGREGATED_TASK_NAME, null, HttpMethod.POST, HttpURLConnection.HTTP_OK);
        DictionaryCountResult dictionaryCountAggregatedResult = objectMapper.readerFor(new TypeReference<DictionaryCountResult>() {})
                .readValue(executeRestCommand);
        List<DebugDictionaryMetadata> dictionaryResult = dictionaryCountAggregatedResult
                .getWorkerDictionaryResultsList()
                .getFirst()
                .getDictionaryMetadataList()
                .stream()
                .filter(x -> x.dictionaryKey().schemaTableName().getTableName().equalsIgnoreCase(tableName))
                .collect(toList());

        assertThat(dictionaryCountAggregatedResult.getWorkerDictionaryResultsList().size()).isEqualTo(1);
        assertThat(dictionaryResult.size()).isEqualTo(createdDictionaries);
        assertThat(dictionaryResult.stream().allMatch(x -> x.dictionarySize() == 1)).isTrue();
        assertThat(dictionaryResult.stream().allMatch(x -> x.failedWriteCount() == 0)).isTrue();
        assertThat(dictionaryCountAggregatedResult.getWorkerDictionaryResultsList().getFirst().getNodeIdentifier()).isNull();
        executeRestCommand = executeRestCommand(DictionaryTask.DICTIONARY_PATH, DictionaryTask.DICTIONARY_COUNT_TASK_NAME, null, HttpMethod.POST, HttpURLConnection.HTTP_OK);
        DictionaryCountResult dictionaryCountResult = objectMapper.readerFor(new TypeReference<DictionaryCountResult>() {})
                .readValue(executeRestCommand);
        dictionaryResult = dictionaryCountResult
                .getWorkerDictionaryResultsList()
                .getFirst()
                .getDictionaryMetadataList()
                .stream()
                .filter(x -> x.dictionaryKey().schemaTableName().getTableName().equalsIgnoreCase(tableName))
                .toList();

        assertThat(dictionaryResult.size()).isEqualTo(createdDictionaries);
        assertThat(dictionaryResult.stream().allMatch(x -> x.dictionarySize() == 1)).isTrue();
        assertThat(dictionaryResult.stream().allMatch(x -> x.failedWriteCount() == 0)).isTrue();
        assertThat(dictionaryCountResult.getWorkerDictionaryResultsList().getFirst().getNodeIdentifier()).isNotNull();

        // expect one cached row group with 5 elements and zero failed warm up elements
        validateWarmupElementsDictionaryId(createdDictionaries, 1, 0, tableName);

        jmxSession = createJmxSession();
        dictionaryStats = computeActual(jmxSession, "select sum(dictionary_max_exception_count), sum(write_dictionaries_count), sum(dictionary_read_elements_count) from \"*dictionary*\"");
        beforeDictionaryMaxExceptionCount = (long) dictionaryStats.getMaterializedRows().getFirst().getField(0);
        beforeWriteDictionaryCount = (long) dictionaryStats.getMaterializedRows().getFirst().getField(1);
        long beforeReadDictionaryCount = (long) dictionaryStats.getMaterializedRows().getFirst().getField(2);
        MaterializedResult materializedRows = computeActual(getSession(), "select * from dictionary_test_1");
        validateDictionaryStats(jmxSession, beforeDictionaryMaxExceptionCount, beforeWriteDictionaryCount, beforeReadDictionaryCount + createdDictionaries);
        assertThat(materializedRows.getRowCount()).isEqualTo(0);
        computeActual("DROP TABLE dictionary_test_1");
    }

    @Disabled
    @Test
    public void testDictionaryFailed()
            throws IOException
    {
        int maxSize = 3;
        int numberOfExpectedWriteDictionaries = 2;
        String tableName = "dictionary_test_2";
        computeActual(format("CREATE TABLE %s (int1 bigint, var varchar)", tableName));
        // one row group per successfully dictionary key + one to fail
        computeActual(getSession(), "INSERT INTO dictionary_test_2 VALUES (1, 'tzachi1')");
        computeActual(getSession(), "INSERT INTO dictionary_test_2 VALUES (2, 'tzachi2')");
        computeActual(getSession(), "INSERT INTO dictionary_test_2 VALUES (3, 'tzachi3')");
        computeActual(getSession(), "INSERT INTO dictionary_test_2 VALUES (4, 'tzachi4')");

        Session session = Session.builder(getSession())
                .setSystemProperty(catalog + "." + ENABLE_DEFAULT_WARMING, Boolean.toString(true))
                .build();
        Session jmxSession = createJmxSession();
        MaterializedResult dictionaryStats = computeActual(jmxSession, "select sum(dictionary_max_exception_count), sum(write_dictionaries_count), sum(dictionary_read_elements_count) from \"*dictionary*\"");
        long beforeDictionaryMaxExceptionCount = (long) dictionaryStats.getMaterializedRows().getFirst().getField(0);
        long beforeWriteDictionaryCount = (long) dictionaryStats.getMaterializedRows().getFirst().getField(1);
        long expectedReadDictionaryCount = (long) dictionaryStats.getMaterializedRows().getFirst().getField(2);
        int expectedFailures = 2;
        warmAndValidate("select * from dictionary_test_2", session, 8, 5, expectedFailures);
        long expectedDictionaryWriteCount = beforeWriteDictionaryCount + expectedFailures;
        validateDictionaryStats(jmxSession, beforeDictionaryMaxExceptionCount + expectedFailures, expectedDictionaryWriteCount, expectedReadDictionaryCount);
        String executeRestCommand = executeRestCommand(DictionaryTask.DICTIONARY_PATH, DictionaryTask.DICTIONARY_COUNT_AGGREGATED_TASK_NAME, null, HttpMethod.POST, HttpURLConnection.HTTP_OK);
        DictionaryCountResult dictionaryCountAggregatedResult = objectMapper.readerFor(new TypeReference<DictionaryCountResult>() {})
                .readValue(executeRestCommand);
        assertThat(dictionaryCountAggregatedResult.getWorkerDictionaryResultsList().size()).isEqualTo(1);
        List<DebugDictionaryMetadata> dictionaryResult = dictionaryCountAggregatedResult
                .getWorkerDictionaryResultsList()
                .getFirst()
                .getDictionaryMetadataList()
                .stream()
                .filter(x -> x.dictionaryKey().schemaTableName().getTableName().equalsIgnoreCase(tableName))
                .toList();
        assertThat(dictionaryResult.size()).isEqualTo(2);
        assertThat(dictionaryResult.stream().allMatch(x -> x.dictionarySize() == maxSize)).isTrue(); //dictionary is empty

        // expect 4 cached row groups with 2 elements each and 2 failed dictionaries
        validateWarmupElementsDictionaryId(2, maxSize + 1, 2, tableName);

        jmxSession = createJmxSession();
        dictionaryStats = computeActual(jmxSession, "select sum(dictionary_max_exception_count), sum(write_dictionaries_count), sum(dictionary_read_elements_count) from \"*dictionary*\"");
        beforeDictionaryMaxExceptionCount = (long) dictionaryStats.getMaterializedRows().getFirst().getField(0);
        beforeWriteDictionaryCount = (long) dictionaryStats.getMaterializedRows().getFirst().getField(1);
        long beforeReadDictionaryCount = (long) dictionaryStats.getMaterializedRows().getFirst().getField(2);
        MaterializedResult materializedRows = computeActual(getSession(), "select * from dictionary_test_2");
        int expectedValidDictionaries = 3 * 2; //3 rowGroups each one use 2 dictionaries
        validateDictionaryStats(jmxSession, beforeDictionaryMaxExceptionCount, beforeWriteDictionaryCount, beforeReadDictionaryCount + expectedValidDictionaries);
        assertThat(materializedRows.getRowCount()).isEqualTo(0);

        //reset dictionaries
        String dictionariesReset = executeRestCommand(DictionaryTask.DICTIONARY_PATH, DictionaryTask.DICTIONARY_RESET_MEMORY_TASK_NAME, null, HttpMethod.POST, HttpURLConnection.HTTP_OK);
        assertThat(Integer.valueOf(dictionariesReset.trim())).isEqualTo(numberOfExpectedWriteDictionaries);

        computeActual(format("DROP TABLE %s", tableName));
    }

    @Test
    @Disabled
    public void testDictionaryLRU()
            throws IOException
    {
        String tableName = "dictionary_test_3";
        String var = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        computeActual(format("CREATE TABLE %s (int1 bigint, var1 varchar, var2 varchar, var3 varchar)", tableName));
        computeActual(getSession(), format("INSERT INTO %s VALUES (1, '%s', '%s', '%s')", tableName, var + "0", var + "00", var + "00"));

        Session session = Session.builder(getSession())
                .setSystemProperty(catalog + "." + ENABLE_DEFAULT_WARMING, Boolean.toString(true))
                .build();
        Session jmxSession = createJmxSession();
        warmAndValidate("select * from dictionary_test_3", session, 4, 1, 0);

        computeActual(getSession(), format("INSERT INTO %s VALUES (2, '%s', '%s', '%s')", tableName, var + "1", var + "10", var + "10"));
        computeActual(getSession(), format("INSERT INTO %s VALUES (3, '%s', '%s', '%s')", tableName, var + "2", var + "20", var + "20"));

        //reset dictionaries
        Map<String, DictionaryConfigurationResult> configurationResults = objectMapper.readerFor(new TypeReference<Map<String, DictionaryConfigurationResult>>() {})
                .readValue(executeRestCommand(DictionaryTask.DICTIONARY_PATH, DictionaryTask.DICTIONARY_GET_CONFIGURATION, null, HttpMethod.GET, HttpURLConnection.HTTP_OK));
        DictionaryConfigurationResult configuration = new DictionaryConfigurationResult(300, 1, -1);
        assertThat(configurationResults.values().stream().findAny().orElseThrow().getMaxDictionaryTotalCacheWeight()).isGreaterThan(300);
        executeRestCommand(DictionaryTask.DICTIONARY_PATH, DictionaryTask.DICTIONARY_SET_CONFIGURATION_AND_RESET_CACHE, configuration, HttpMethod.POST, HttpURLConnection.HTTP_OK);
        configurationResults = objectMapper.readerFor(new TypeReference<Map<String, DictionaryConfigurationResult>>() {})
                .readValue(executeRestCommand(DictionaryTask.DICTIONARY_PATH, DictionaryTask.DICTIONARY_GET_CONFIGURATION, null, HttpMethod.GET, HttpURLConnection.HTTP_OK));
        assertThat(configurationResults.values().stream().findAny().orElseThrow().getMaxDictionaryTotalCacheWeight()).isEqualTo(300);
        assertThat(configurationResults.values().stream().findAny().orElseThrow().getConcurrency()).isEqualTo(1);
        warmAndValidate("select * from dictionary_test_3", session, 8, 2, 0);
        MaterializedResult dictionaryStats = computeActual(jmxSession, "select sum(dictionary_evicted_entries), sum(dictionary_active_size), sum(dictionaries_weight), sum(dictionary_entries), sum(write_dictionaries_count) from \"*dictionary*\"");
        MaterializedRow stats = dictionaryStats.getMaterializedRows().getFirst();
        assertThat((long) stats.getField(0)).isEqualTo(1); //dictionary_evicted_entries
        assertThat((long) stats.getField(1)).isEqualTo(0); // dictionary_active_size
        assertThat((long) stats.getField(2)).isLessThanOrEqualTo(300); // dictionaries_weight
        assertThat((long) stats.getField(3)).isEqualTo(3); // dictionary_entries

        //we run query on int 1 - so var will be evicted
        computeActual(getSession(), "select int1 from dictionary_test_3");
        dictionaryStats = computeActual(jmxSession, "select sum(dictionary_evicted_entries), sum(dictionary_active_size), sum(dictionaries_weight), sum(dictionaries_varlen_str_weight), sum(dictionary_entries), sum(write_dictionaries_count) from \"*dictionary*\"");
        MaterializedRow statsAfterWarmInt = dictionaryStats.getMaterializedRows().getFirst();
        assertThat((long) statsAfterWarmInt.getField(1)).isEqualTo(0); // dictionary_active_size
        assertThat((long) statsAfterWarmInt.getField(2)).isLessThanOrEqualTo(300); //dictionaries_weight
        long cacheTotalSize = (long) statsAfterWarmInt.getField(2);
        long cacheTotalStrSize = (long) statsAfterWarmInt.getField(3);
        assertThat(cacheTotalSize).isGreaterThan(cacheTotalStrSize); // dictionary_entries
        Map<String, Map<String, Integer>> cachedValues = objectMapper.readerFor(new TypeReference<Map<String, Map<String, Long>>>() {})
                .readValue(executeRestCommand(DictionaryTask.DICTIONARY_PATH, DictionaryTask.DICTIONARY_GET_CACHE_KEYS, null, HttpMethod.GET, HttpURLConnection.HTTP_OK));
        assertThat(cachedValues.values().stream().findAny().orElseThrow()).containsKey("schema.dictionary_test_3.int1");
        //reset dictionaries
        executeRestCommand(DictionaryTask.DICTIONARY_PATH, DictionaryTask.DICTIONARY_RESET_MEMORY_TASK_NAME, null, HttpMethod.POST, HttpURLConnection.HTTP_OK);

        computeActual(format("DROP TABLE %s", tableName));
    }

    /**
     * the api define on workers only so run on single.
     */
    @Test
    public void testPredicateCache()
            throws IOException
    {
        String str = executeRestCommand(PredicateCacheTask.PREDICATES_DUMP, "", null, HttpMethod.GET, HttpURLConnection.HTTP_OK);
        PredicateCacheTask.PredicateCacheDump result = objectMapper.readerFor(PredicateCacheTask.PredicateCacheDump.class).readValue(str);
        assertThat(result.bufferPoolDumpMap().size()).isEqualTo(PredicateBufferPoolType.values().length);
    }

    @Test
    public void testSharedRowGroups()
    {
        computeActual(getSession(), "INSERT INTO t VALUES (1, 'shlomishlomishlomi')");
        warmAndValidate("SELECT * FROM T", true, "warm_finished");

        Failsafe.with(new RetryPolicy<>()
                        .handle(AssertionError.class)
                        .withMaxRetries(5)
                        .withDelay(Duration.ofMillis(100))
                        .withMaxDuration(Duration.ofMillis(1000)))
                .run(() -> {
                    String cachedSharedRowGroupsStr = executeRestCommand(CACHED_SHARED_ROW_GROUP, "", null, HttpMethod.GET, HttpURLConnection.HTTP_OK);
                    List<Object> cachedSharedRowGroupRes = objectMapper.readerFor(new TypeReference<List<Object>>() {})
                            .readValue(cachedSharedRowGroupsStr);
                    assertThat(cachedSharedRowGroupRes.size()).isEqualTo(1);
                    assertThat(((LinkedHashMap<?, ?>) ((LinkedHashMap<?, ?>) cachedSharedRowGroupRes.getFirst()).get("key")).get("schema_name")).isEqualTo(DEFAULT_SCHEMA);
                });
    }

    @Test
    public void testFailuresGenerator()
            throws IOException
    {
        computeActual(getSession(), "INSERT INTO t VALUES (1, 'shlomishlomishlomi')");

        List<FailureGeneratorResource.FailureGeneratorData> failureGeneratorDataList =
                List.of(new FailureGeneratorResource.FailureGeneratorData(
                        PageSink.class.getName(),
                        "appendPage",
                        FailureRepetitionMode.REP_MODE_ALWAYS,
                        FailureGeneratorInvocationHandler.FailureType.JAVA_EXCEPTION,
                        1));
//        FailureGeneratorResource.FailureGeneratorData data = new FailureGeneratorResource.FailureGeneratorData(StorageEngine.class.getName(), "txInsertCreate", FailureRepetitionMode.REP_MODE_ONCE, FailureGeneratorInvocationHandler.FailureType.JAVA_EXCEPTION, 1);
//        failureGeneratorDataList.add(data);
        executeRestCommand(FailureGeneratorResource.TASK_NAME, "", failureGeneratorDataList, HttpMethod.POST, HttpURLConnection.HTTP_NO_CONTENT);
        warmAndValidate("SELECT * FROM T", true, "warm_accomplished");

        Failsafe.with(new RetryPolicy<>()
                        .handle(AssertionError.class)
                        .withMaxRetries(5)
                        .withDelay(Duration.ofSeconds(1))
                        .withMaxDuration(Duration.ofSeconds(10)))
                .run(() -> {
                    String cachedRowGroupsStr = executeRestCommand(CACHED_ROW_GROUP, "", null, HttpMethod.GET, HttpURLConnection.HTTP_OK);
                    List<RowGroupData> cachedRowGroupRes = objectMapper.readerFor(new TypeReference<List<RowGroupData>>() {}).readValue(cachedRowGroupsStr);
                    assertThat(cachedRowGroupRes.size()).isEqualTo(1);
                    RowGroupData rowGroup = cachedRowGroupRes.getFirst();
                    Collection<WarmUpElement> warmupElements = rowGroup.getWarmUpElements();
                    WarmUpElementState state = warmupElements.iterator().next().getState();
                    assertThat((Integer) state.temporaryFailureCount()).isGreaterThan(0);
                });
        executeRestCommand(FailureGeneratorResource.TASK_NAME, "", Collections.emptyList(), HttpMethod.POST, HttpURLConnection.HTTP_NO_CONTENT);
    }

    @Test
    public void testPrefill()
            throws IOException
    {
        computeActual("CREATE TABLE table_with_nulls (c_char varchar, c_null varchar, c_partition varchar) WITH (format='PARQUET', partitioned_by = ARRAY['c_partition'])");
        computeActual("INSERT INTO table_with_nulls values ('a', NULL, 'a_p')");
//        computeActual("INSERT INTO table_with_nulls values ('b', NULL, 'b_p')");

        warmAndValidate("SELECT * FROM table_with_nulls",
                true,
                "warm_finished");

        //prefill data source
        MaterializedResult materializedRows = computeActual("SELECT c_null FROM table_with_nulls");
        assertThat(materializedRows.getRowCount()).isEqualTo(1);

        //prefill data source
        materializedRows = computeActual("SELECT c_partition FROM table_with_nulls");
        assertThat(materializedRows.getRowCount()).isEqualTo(1);

        //native data source
        materializedRows = computeActual("SELECT c_char FROM table_with_nulls");
        assertThat(materializedRows.getRowCount()).isEqualTo(0);

        //prefill data source
        materializedRows = computeActual("SELECT c_null, c_partition FROM table_with_nulls");
        assertThat(materializedRows.getRowCount()).isEqualTo(1);

        //mix data source
        materializedRows = computeActual("SELECT c_null, c_char, c_partition FROM table_with_nulls");
        assertThat(materializedRows.getRowCount()).isEqualTo(0);

        //prefill data source - count returns 0 since all values are null
        materializedRows = computeActual("SELECT count(c_null) FROM table_with_nulls");
        assertThat(materializedRows.getRowCount()).isEqualTo(1);
        assertThat((long) materializedRows.getMaterializedRows().getFirst().getField(0)).isEqualTo(0);

        //prefill data source
        materializedRows = computeActual("SELECT count(c_partition) FROM table_with_nulls");
        assertThat(materializedRows.getRowCount()).isEqualTo(1);
        assertThat((long) materializedRows.getMaterializedRows().getFirst().getField(0)).isEqualTo(1);

        //test prefill data source with specific warmup rule and lucene syntax
        createWarmupRules(DEFAULT_SCHEMA,
                "table_with_nulls",
                Map.of("c_partition",
                        Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_LUCENE, DEFAULT_PRIORITY, DEFAULT_TTL))));

        //warmup
        warmAndValidate("SELECT * FROM table_with_nulls",
                getSession(),
                1,
                1,
                0);
        @Language("SQL") String query = "SELECT c_partition FROM table_with_nulls WHERE c_partition LIKE 'a_%'";
        Map<String, Long> expectedQueryStats = Map.of(
                "prefilled_collect_columns", 1L,
                "varada_match_columns", 0L,
                "external_match_columns", 0L);
        validateQueryStats(query, getSession(), expectedQueryStats);
        computeActual("DROP TABLE table_with_nulls");
    }

    @Test
    public void testTableLevelWarmupRule()
            throws IOException
    {
        computeActual(getSession(), "INSERT INTO t VALUES (1, 'shlomishlomishlomi')");

        WarmupColRuleData tableLevelRule = new WarmupColRuleData(0,
                DEFAULT_SCHEMA,
                "t",
                new WildcardColumnData(),
                WarmUpType.WARM_UP_TYPE_DATA,
                DEFAULT_PRIORITY,
                Duration.ofSeconds(DEFAULT_TTL.getSeconds()),
                ImmutableSet.of());

        WarmupColRuleData columnRule = new WarmupColRuleData(0,
                DEFAULT_SCHEMA,
                "t",
                new RegularColumnData(C2),
                WarmUpType.WARM_UP_TYPE_BASIC,
                DEFAULT_PRIORITY,
                Duration.ofSeconds(DEFAULT_TTL.getSeconds()),
                ImmutableSet.of());

        executeRestCommand(WarmupRuleService.WARMUP_PATH,
                WarmupTask.TASK_NAME_SET,
                List.of(tableLevelRule, columnRule),
                HttpMethod.POST,
                HttpURLConnection.HTTP_OK);

        List<WarmupColRuleData> result = objectMapper.readerFor(new TypeReference<List<WarmupColRuleData>>() {})
                .readValue(executeRestCommand(WarmupRuleService.WARMUP_PATH,
                        WarmupRuleService.TASK_NAME_GET,
                        null,
                        HttpMethod.GET,
                        HttpURLConnection.HTTP_OK));

        assertThat(result).hasSize(2);

        warmAndValidate("select * from t", false, 3, 1);
        WarmupRulesUsageData warmupRulesUsageData = objectMapper.readerFor(new TypeReference<WarmupRulesUsageData>() {})
                .readValue(executeRestCommand(WarmupRuleService.WARMUP_PATH,
                        WarmupTask.TASK_NAME_GET_USAGE,
                        null,
                        HttpMethod.GET,
                        HttpURLConnection.HTTP_OK));

        assertThat(warmupRulesUsageData.warmupColRuleUsageDataList()).hasSize(2);

        WarmupColRuleUsageData tableLevelWarmupColRuleUsageData = warmupRulesUsageData.warmupColRuleUsageDataList()
                .stream()
                .filter(warmupColRuleUsageData -> warmupColRuleUsageData.getColumn() instanceof WildcardColumnData)
                .findFirst()
                .orElseThrow();

        assertThat(tableLevelWarmupColRuleUsageData.getSchema()).isEqualTo(tableLevelRule.getSchema());
        assertThat(tableLevelWarmupColRuleUsageData.getTable()).isEqualTo(tableLevelRule.getTable());
        assertThat(tableLevelWarmupColRuleUsageData.getColumn()).isEqualTo(new WildcardColumnData());
        assertThat(tableLevelWarmupColRuleUsageData.getWarmUpType()).isEqualTo(tableLevelRule.getWarmUpType());
        assertThat(tableLevelWarmupColRuleUsageData.getTtl()).isEqualTo(tableLevelRule.getTtl());
        assertThat(tableLevelWarmupColRuleUsageData.getPriority()).isEqualTo(tableLevelRule.getPriority());
        assertThat(tableLevelWarmupColRuleUsageData.getPredicates()).isEqualTo(tableLevelRule.getPredicates());

        WarmupColRuleUsageData columnLevelWarmupColRuleUsageData = warmupRulesUsageData.warmupColRuleUsageDataList()
                .stream()
                .filter(warmupColRuleUsageData -> warmupColRuleUsageData.getColumn() instanceof RegularColumnData)
                .findFirst()
                .orElseThrow();

        assertThat(columnLevelWarmupColRuleUsageData.getSchema()).isEqualTo(columnRule.getSchema());
        assertThat(columnLevelWarmupColRuleUsageData.getTable()).isEqualTo(columnRule.getTable());
        assertThat(columnLevelWarmupColRuleUsageData.getColumn()).isEqualTo(new RegularColumnData(C2));
        assertThat(columnLevelWarmupColRuleUsageData.getWarmUpType()).isEqualTo(columnRule.getWarmUpType());
        assertThat(columnLevelWarmupColRuleUsageData.getTtl()).isEqualTo(columnRule.getTtl());
        assertThat(columnLevelWarmupColRuleUsageData.getPriority()).isEqualTo(columnRule.getPriority());
        assertThat(columnLevelWarmupColRuleUsageData.getPredicates()).isEqualTo(columnRule.getPredicates());

        String cachedSharedRowGroupsStr = executeRestCommand(CACHED_SHARED_ROW_GROUP, "", null, HttpMethod.GET, HttpURLConnection.HTTP_OK);

        List<RowGroupData> cachedSharedRowGroupRes = objectMapper.readerFor(new TypeReference<List<RowGroupData>>() {})
                .readValue(cachedSharedRowGroupsStr);
        assertThat(cachedSharedRowGroupRes.size()).isEqualTo(1);
        Collection<WarmUpElement> warmUpElements = cachedSharedRowGroupRes.getFirst().getWarmUpElements();
        Stream.of(Pair.of(C1, WarmUpType.WARM_UP_TYPE_DATA),
                        Pair.of(C2, WarmUpType.WARM_UP_TYPE_DATA),
                        Pair.of(C2, WarmUpType.WARM_UP_TYPE_BASIC))
                .forEach(warmUpTypePair -> {
                    WarmUpElement warmupElement = warmUpElements.stream()
                            .filter(warmUpElement -> {
                                VaradaColumn varadaColumn = warmUpElement.getVaradaColumn();
                                return varadaColumn instanceof RegularColumn &&
                                        varadaColumn.getName().equals(warmUpTypePair.getKey()) &&
                                        warmUpElement.getWarmUpType().name().equals(warmUpTypePair.getValue().name());
                            })
                            .findFirst()
                            .orElse(null);
                    assertThat(warmupElement).isNotNull();
                });
        demoteAll();
        validateDemoteAll();
    }

    @Test
    public void testTableLevelWarmupRuleOverriddenByColumnLevelWarmupRule()
            throws IOException
    {
        computeActual(getSession(), "INSERT INTO t VALUES (1, 'shlomishlomishlomi')");

        WarmupColRuleData tableLevelRule = new WarmupColRuleData(0,
                DEFAULT_SCHEMA,
                "t",
                new WildcardColumnData(),
                WarmUpType.WARM_UP_TYPE_DATA,
                DEFAULT_PRIORITY,
                Duration.ofSeconds(DEFAULT_TTL.getSeconds()),
                ImmutableSet.of());

        WarmupColRuleData columnRule = new WarmupColRuleData(0,
                DEFAULT_SCHEMA,
                "t",
                new RegularColumnData(C2),
                WarmUpType.WARM_UP_TYPE_DATA,
                tableLevelRule.getPriority() - 1,
                Duration.ofSeconds(DEFAULT_TTL.getSeconds()),
                ImmutableSet.of());

        executeRestCommand(WarmupRuleService.WARMUP_PATH,
                WarmupTask.TASK_NAME_SET,
                List.of(tableLevelRule, columnRule),
                HttpMethod.POST,
                HttpURLConnection.HTTP_OK);

        List<WarmupColRuleData> result = objectMapper.readerFor(new TypeReference<List<WarmupColRuleData>>() {})
                .readValue(executeRestCommand(WarmupRuleService.WARMUP_PATH,
                        WarmupRuleService.TASK_NAME_GET,
                        null,
                        HttpMethod.GET,
                        HttpURLConnection.HTTP_OK));

        assertThat(result).hasSize(2);

        warmAndValidate("select * from t", false, 2, 1);
        WarmupRulesUsageData warmupRulesUsageData = objectMapper.readerFor(new TypeReference<WarmupRulesUsageData>() {})
                .readValue(executeRestCommand(WarmupRuleService.WARMUP_PATH,
                        WarmupTask.TASK_NAME_GET_USAGE,
                        null,
                        HttpMethod.GET,
                        HttpURLConnection.HTTP_OK));

        assertThat(warmupRulesUsageData.warmupColRuleUsageDataList()).hasSize(2);

        WarmupColRuleUsageData tableLevelWarmupColRuleUsageData = warmupRulesUsageData.warmupColRuleUsageDataList()
                .stream()
                .filter(warmupColRuleUsageData -> warmupColRuleUsageData.getColumn() instanceof WildcardColumnData)
                .findFirst()
                .orElseThrow();

        assertThat(tableLevelWarmupColRuleUsageData.getSchema()).isEqualTo(tableLevelRule.getSchema());
        assertThat(tableLevelWarmupColRuleUsageData.getTable()).isEqualTo(tableLevelRule.getTable());
        assertThat(tableLevelWarmupColRuleUsageData.getColumn()).isEqualTo(new WildcardColumnData());
        assertThat(tableLevelWarmupColRuleUsageData.getWarmUpType()).isEqualTo(tableLevelRule.getWarmUpType());
        assertThat(tableLevelWarmupColRuleUsageData.getTtl()).isEqualTo(tableLevelRule.getTtl());
        assertThat(tableLevelWarmupColRuleUsageData.getPriority()).isEqualTo(tableLevelRule.getPriority());
        assertThat(tableLevelWarmupColRuleUsageData.getPredicates()).isEqualTo(tableLevelRule.getPredicates());

        WarmupColRuleUsageData columnLevelWarmupColRuleUsageData = warmupRulesUsageData.warmupColRuleUsageDataList()
                .stream()
                .filter(warmupColRuleUsageData -> warmupColRuleUsageData.getColumn() instanceof RegularColumnData)
                .findFirst()
                .orElseThrow();

        assertThat(columnLevelWarmupColRuleUsageData.getSchema()).isEqualTo(columnRule.getSchema());
        assertThat(columnLevelWarmupColRuleUsageData.getTable()).isEqualTo(columnRule.getTable());
        assertThat(columnLevelWarmupColRuleUsageData.getColumn()).isEqualTo(new RegularColumnData(C2));
        assertThat(columnLevelWarmupColRuleUsageData.getWarmUpType()).isEqualTo(columnRule.getWarmUpType());
        assertThat(columnLevelWarmupColRuleUsageData.getTtl()).isEqualTo(columnRule.getTtl());
        assertThat(columnLevelWarmupColRuleUsageData.getPriority()).isEqualTo(columnRule.getPriority());
        assertThat(columnLevelWarmupColRuleUsageData.getPredicates()).isEqualTo(columnRule.getPredicates());

        String cachedSharedRowGroupsStr = executeRestCommand(CACHED_SHARED_ROW_GROUP, "", null, HttpMethod.GET, HttpURLConnection.HTTP_OK);

        List<RowGroupData> cachedSharedRowGroupRes = objectMapper.readerFor(new TypeReference<List<RowGroupData>>() {})
                .readValue(cachedSharedRowGroupsStr);
        assertThat(cachedSharedRowGroupRes.size()).isEqualTo(1);
        List<WarmUpElement> warmUpElements = (List<WarmUpElement>) cachedSharedRowGroupRes.getFirst().getWarmUpElements();
        Stream.of(Pair.of(C1, WarmUpType.WARM_UP_TYPE_DATA),
                        Pair.of(C2, WarmUpType.WARM_UP_TYPE_DATA))
                .forEach(warmUpTypePair -> {
                    WarmUpElement warmupElement = warmUpElements.stream()
                            .filter(warmUpElement -> {
                                VaradaColumn varadaColumn = warmUpElement.getVaradaColumn();
                                return varadaColumn instanceof RegularColumn &&
                                        varadaColumn.getName().equals(warmUpTypePair.getKey()) &&
                                        warmUpElement.getWarmUpType().name().equals(warmUpTypePair.getValue().name());
                            })
                            .findFirst()
                            .orElse(null);
                    assertThat(warmupElement).isNotNull();
                });
        demoteAll();
        validateDemoteAll();
    }

    @Test
    public void testExternalCollectMetrics()
    {
        computeActual(getSession(), "INSERT INTO t VALUES (1, 'shlomi')");

        @Language("SQL") String query = format("SELECT %s, %s FROM t WHERE %s=1", C1, C2, C1);
        Map<String, Long> expectedQueryStats = Map.of(
                EXTERNAL_COLLECT_STAT, 2L,
                EXTERNAL_MATCH_STAT, 1L);
        validateQueryStats(query, getSession(), expectedQueryStats);
    }

    @Test
    public void testAlternatives()
    {
        try {
            computeActual("CREATE TABLE table1(id1 integer)");
            computeActual("INSERT INTO table1 VALUES(2)");

            warmAndValidate("SELECT * from table1 WHERE id1 > 0", true, 2, 1);

            @Language("SQL") String query = "SELECT count(*) FROM table1 WHERE id1 > 5";

            String plan = computeActual("EXPLAIN ANALYZE " + query).getMaterializedRows().getFirst().getFields().getFirst().toString();
            assertThat(plan).containsOnlyOnce("ChooseAlternativeNode[alternativesCount = 2]"); // there are 2 alternatives
            assertThat(plan).containsOnlyOnce("TableScan"); // only 1 alternative was used (analyze shows only the alternatives that were used)
            assertThat(plan).containsOnlyOnce("subsumedPredicates=true"); // assert usage of the alternative in which the filter is subsumed by WarpSpeed

            Map<String, Long> expectedQueryStats = Map.of(
                    CACHED_TOTAL_ROWS, 1L, // table1's row
                    VARADA_MATCH_COLUMNS_STAT, 1L, // id1 > 5
                    VARADA_COLLECT_COLUMNS_STAT, 0L, // predicate is fully pushed down - no need to collect
                    PREFILLED_COLUMNS_STAT, 0L,
                    EXTERNAL_MATCH_STAT, 0L,
                    EXTERNAL_COLLECT_STAT, 0L);
            validateQueryStats(query, getSession(), expectedQueryStats);
        }
        finally {
            computeActual("DROP TABLE table1");
        }
    }

    @Test
    public void testMappedMatchCollect()
            throws IOException
    {
        try {
            computeActual("CREATE TABLE mapped_match_collect_test (double_1 double, varchar_1 varchar, short_decimal decimal(2,1))");
            computeActual("INSERT INTO mapped_match_collect_test (double_1, varchar_1, short_decimal) values (1, '1', 1.0), (2, '2', 2.0), (3, '3', 3.0)");
            String warmQuery = "select double_1, short_decimal, varchar_1 from mapped_match_collect_test";

            Session session = Session.builder(getSession())
                    .setSystemProperty(catalog + "." + ENABLE_MAPPED_MATCH_COLLECT, "false")
                    .build();
            createWarmupRules(DEFAULT_SCHEMA,
                    "mapped_match_collect_test",
                    Map.of("double_1", Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BASIC, DEFAULT_PRIORITY, DEFAULT_TTL)),
                            "short_decimal", Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BASIC, DEFAULT_PRIORITY, DEFAULT_TTL)),
                            "varchar_1", Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_LUCENE, DEFAULT_PRIORITY, DEFAULT_TTL))));

            warmAndValidate(warmQuery, session, 3, 1, 0);

            // ================== Mapped match collect supported ================== //
            List<String> mapSupportedQueries = List.of(
                    "select double_1 from mapped_match_collect_test where double_1=1 or double_1=3",
                    "select double_1 from mapped_match_collect_test where double_1 in(1, 3)",
                    "select short_decimal from mapped_match_collect_test where short_decimal=1 or short_decimal=3");

            Map<String, Long> expectedStatsMapSupported = Map.of(
                    "varada_match_collect_columns", 1L,
                    "varada_mapped_match_collect_columns", 1L);

            // ================== Mapped match collect unsupported ================== //

            List<String> mapUnsupportedQueries = List.of(
                    "select double_1 from mapped_match_collect_test where double_1<3",
                    "select double_1 from mapped_match_collect_test where ceil(double_1)=1 or ceil(double_1)=3");

            Map<String, Long> expectedStatsMapUnsupported = Map.of(
                    "varada_match_collect_columns", 1L,
                    "varada_mapped_match_collect_columns", 0L);

            // ================== Match collect unsupported ================== //

            List<String> matchCollectUnsupportedQueries = List.of(
                    "select varchar_1 from mapped_match_collect_test where varchar_1 in('1', '3')");

            Map<String, Long> expectedStatsMatchCollectUnsupported = Map.of(
                    "varada_match_collect_columns", 0L,
                    "varada_mapped_match_collect_columns", 0L);

            for (@Language("SQL") String query : mapSupportedQueries) {
                validateQueryStats(query, session, expectedStatsMapUnsupported);
            }

            session = Session.builder(getSession())
                    .setSystemProperty(catalog + "." + VaradaSessionProperties.ENABLE_MAPPED_MATCH_COLLECT, "true")
                    .build();

            for (@Language("SQL") String query : mapSupportedQueries) {
                validateQueryStats(query, session, expectedStatsMapSupported);
            }

            for (@Language("SQL") String query : mapUnsupportedQueries) {
                validateQueryStats(query, session, expectedStatsMapUnsupported);
            }

            for (@Language("SQL") String query : matchCollectUnsupportedQueries) {
                validateQueryStats(query, session, expectedStatsMatchCollectUnsupported);
            }
        }
        finally {
            computeActual("DROP TABLE IF EXISTS mapped_match_collect_test");
        }
    }

    private void buildAndWarmWideTable(int numberOfColumns, boolean defaultWarm, int expectedElementsToWarm, Optional<Duration> duration)
            throws IOException
    {
        StringJoiner columnDefinition = new StringJoiner(",", "(", ")");
        StringJoiner values = new StringJoiner(",");
        Map<String, Set<WarmupPropertiesData>> rules = new HashMap<>();
        IntStream.range(0, numberOfColumns).forEach(columnId -> {
            columnDefinition.add("C0" + columnId + " varchar(20)");
            values.add("'value0" + columnId + "'");
            if (!defaultWarm) {
                rules.put("C0" + columnId, Collections.singleton(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, 7.5, duration.orElse(Duration.ofMinutes(columnId * 10L)))));
            }

            columnDefinition.add("C1" + columnId + " integer");
            values.add(String.valueOf(columnId));
            if (!defaultWarm) {
                rules.put("C1" + columnId, Collections.singleton(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, 7.1, duration.orElse(Duration.ofMinutes(columnId)))));
            }

            columnDefinition.add("C2" + columnId + " tinyint");
            values.add("1");
            if (!defaultWarm) {
                rules.put("C2" + columnId, Collections.singleton(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, 6.3, duration.orElse(Duration.ofSeconds(columnId * 10L)))));
            }
        });

        computeActual(getSession(), format("create table %s %s", WIDE_TABLE_NAME, columnDefinition));
        if (!defaultWarm) {
            createWarmupRules(DEFAULT_SCHEMA, WIDE_TABLE_NAME, rules);
        }
        computeActual(getSession(), format("INSERT INTO %s VALUES (%s)", WIDE_TABLE_NAME, values));
        Session session = buildSession(defaultWarm, false);
        int expectedWarmAccomplished = expectedElementsToWarm == 0 ? 0 : 1;
        warmAndValidate(format("select * from %s", WIDE_TABLE_NAME), session, expectedElementsToWarm, expectedWarmAccomplished, 0);
    }

    private DispatcherTableHandle executeWithTableHandle(Session session, String sql)
    {
        PlanNode plan = executeWithPlan(session, sql).getRoot();
        TableScanNode tableScanNode = PlanNodeSearcher.searchFrom(plan)
                .where(node -> node instanceof TableScanNode || node instanceof ChooseAlternativeNode)
                .findAll().stream()
                .map(node -> (TableScanNode) (node instanceof TableScanNode ? node : ((ChooseAlternativeNode) node).getOriginalTableScan().tableScanNode()))
                .findFirst()
                .orElseThrow();
        return (DispatcherTableHandle) tableScanNode.getTable().getConnectorHandle();
    }

    @SuppressWarnings("LanguageMismatch")
    private Plan executeWithPlan(Session session, String sql)
    {
        return getQueryRunner().executeWithPlan(session, sql).queryPlan().orElseThrow();
    }

    private void validateWarmupElementsDictionaryId(int numWarmupElements, int numCachedRowGroups, int expectedNumFailedWarmupElements, String tableName)
            throws IOException
    {
        HealthResult healthResult = objectMapper.readerFor(HealthResult.class)
                .readValue(executeRestCommand(HealthTask.HEALTH_PATH, ClusterHealthTask.TASK_NAME, null, HttpMethod.GET, 200));
        assertThat(healthResult.getHealthNodes().size()).isEqualTo(1);
        String nodeIdentifier = healthResult.getHealthNodes().stream().findFirst().orElseThrow().nodeIdentifier();
        String cachedRowGroupsStr = executeRestCommand(CACHED_ROW_GROUP, "", null, HttpMethod.GET, HttpURLConnection.HTTP_OK);
        List<RowGroupData> cachedRowGroupRes = objectMapper.readerFor(new TypeReference<List<RowGroupData>>() {})
                .readValue(cachedRowGroupsStr);
        List<RowGroupData> tableCachedRowGroup = new ArrayList<>();
        for (RowGroupData cachedRowGroup : cachedRowGroupRes) {
            RowGroupKey rowGroupKey = cachedRowGroup.getRowGroupKey();
            assertThat(cachedRowGroup.getNodeIdentifier()).isEqualTo(nodeIdentifier);
            if (rowGroupKey.table().equalsIgnoreCase(tableName)) {
                tableCachedRowGroup.add(cachedRowGroup);
            }
        }
        assertThat(tableCachedRowGroup.size()).isEqualTo(numCachedRowGroups);

        int numFailed = 0;
        for (int i = 0; i < numCachedRowGroups; i++) {
            Collection<WarmUpElement> warmUpElements = tableCachedRowGroup.get(i).getWarmUpElements();
            assertThat(warmUpElements.size()).isEqualTo(numWarmupElements);
            for (WarmUpElement warmUpElement : warmUpElements) {
                assertThat(warmUpElement.getState().state().name()).isEqualTo("VALID");
                DictionaryInfo dictionaryInfo = warmUpElement.getDictionaryInfo();
                if (dictionaryInfo.toString().contains(DICTIONARY_MAX_EXCEPTION.name())) {
                    numFailed++;
                }
                else {
                    assertThat(dictionaryInfo.toString()).contains(DictionaryState.DICTIONARY_VALID.name());
                }
            }
        }
        assertThat(numFailed).isEqualTo(expectedNumFailedWarmupElements);
    }

//    private int getDictionariesUsage()
//            throws IOException
//    {
//        return Integer.parseInt(executeRestCommand(DictionaryTask.DICTIONARY_PATH, DictionaryTask.DICTIONARY_USAGE_TASK_NAME, null, HttpMethod.GET, HttpURLConnection.HTTP_OK).trim());
//    }

    private void assertPredicate(DispatcherTableHandle table,
            String columnName,
            String functionName,
            boolean collectNulls,
            Domain domain)
    {
        if (domain != null) {
            assertThat(((HiveTableHandle) table.getProxyConnectorTableHandle())
                    .getCompactEffectivePredicate()
                    .getDomains()
                    .stream())
                    .allMatch(x -> x.values().stream().allMatch(y -> y.equals(domain)));
        }
        else {
            assertThat(((HiveTableHandle) table.getProxyConnectorTableHandle())
                    .getCompactEffectivePredicate()
                    .getDomains()
                    .orElseThrow())
                    .isEmpty();
        }
        if (columnName != null) {
            Map<RegularColumn, VaradaExpressionData> varadaExpressions = table.getWarpExpression().orElseThrow().varadaExpressionDataLeaves().stream().collect(Collectors.toMap(VaradaExpressionData::getVaradaColumn, Function.identity()));
            VaradaExpressionData varadaExpressionData = varadaExpressions.get(new RegularColumn(columnName));
            assertThat(varadaExpressionData.isCollectNulls()).isEqualTo(collectNulls);
            if (functionName != null) {
                validateFunctionNameExistInExpression(varadaExpressionData.getExpression(), functionName);
            }
            else {
                assertThat(varadaExpressionData.getExpression() instanceof VaradaConstant).isTrue();
            }
        }
        else {
            assertThat(table.getWarpExpression()).isEmpty();
        }
    }

    private boolean validateFunctionNameExistInExpression(VaradaExpression varadaExpression, String functionName)
    {
        boolean result = false;
        if (varadaExpression instanceof VaradaCall callExpression) {
            if (callExpression.getFunctionName().equals(functionName)) {
                result = true;
            }
            else {
                for (VaradaExpression expression : callExpression.getArguments()) {
                    result |= validateFunctionNameExistInExpression(expression, functionName);
                }
            }
        }
        return result;
    }

    private List<io.trino.plugin.warp.gen.constants.WarmUpType> getWarmedElements()
            throws IOException
    {
        String cachedRowGroupsStr = executeRestCommand(CACHED_ROW_GROUP, "", null, HttpMethod.GET, HttpURLConnection.HTTP_OK);
        List<RowGroupData> cachedRowGroupRes = objectMapper.readerFor(new TypeReference<List<RowGroupData>>() {}).readValue(cachedRowGroupsStr);
        if (cachedRowGroupRes.isEmpty()) {
            return Collections.emptyList();
        }
        assertThat(cachedRowGroupRes.size()).isEqualTo(1);
        RowGroupData rowGroup = cachedRowGroupRes.getFirst();
        return rowGroup.getValidWarmUpElements().stream().map(WarmUpElement::getWarmUpType).toList();
    }
}
