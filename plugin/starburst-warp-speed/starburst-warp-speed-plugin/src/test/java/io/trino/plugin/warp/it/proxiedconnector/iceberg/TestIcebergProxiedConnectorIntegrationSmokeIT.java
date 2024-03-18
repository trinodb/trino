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
package io.trino.plugin.warp.it.proxiedconnector.iceberg;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.metadata.InternalFunctionBundle;
import io.trino.plugin.iceberg.IcebergPlugin;
import io.trino.plugin.varada.api.warmup.PartitionValueWarmupPredicateRule;
import io.trino.plugin.varada.api.warmup.WarmUpType;
import io.trino.plugin.varada.api.warmup.WarmupColRuleData;
import io.trino.plugin.varada.api.warmup.WarmupPropertiesData;
import io.trino.plugin.varada.api.warmup.column.RegularColumnData;
import io.trino.plugin.varada.dispatcher.DispatcherConnectorFactory;
import io.trino.plugin.varada.warmup.WarmupRuleService;
import io.trino.plugin.warp.WarpPlugin;
import io.trino.plugin.warp.di.VaradaStubsStorageEngineModule;
import io.trino.plugin.warp.extension.execution.warmup.WarmupTask;
import io.trino.plugin.warp.it.DispatcherQueryRunner;
import io.trino.plugin.warp.it.DispatcherStubsIntegrationSmokeIT;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import jakarta.ws.rs.HttpMethod;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.IntStream;

import static io.trino.plugin.varada.VaradaSessionProperties.ENABLE_DEFAULT_WARMING;
import static io.trino.plugin.varada.configuration.ProxiedConnectorConfiguration.ICEBERG_CONNECTOR_NAME;
import static io.trino.plugin.varada.configuration.ProxiedConnectorConfiguration.PROXIED_CONNECTOR;
import static io.trino.plugin.warp.extension.configuration.WarpExtensionConfiguration.USE_HTTP_SERVER_PORT;
import static io.varada.tools.configuration.MultiPrefixConfigurationWrapper.WARP_SPEED_PREFIX;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestIcebergProxiedConnectorIntegrationSmokeIT
        extends DispatcherStubsIntegrationSmokeIT
{
    public TestIcebergProxiedConnectorIntegrationSmokeIT()
    {
        super(1, "iceberg");
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = DispatcherQueryRunner.createQueryRunner(new VaradaStubsStorageEngineModule(),
                Optional.empty(), numNodes,
                Collections.emptyMap(),
                Map.of("http-server.log.enabled", "false",
                        WARP_SPEED_PREFIX + USE_HTTP_SERVER_PORT, "false",
                        "node.environment", "varada",
                        "iceberg.catalog.type", "TESTING_FILE_METASTORE",
                        WARP_SPEED_PREFIX + PROXIED_CONNECTOR, ICEBERG_CONNECTOR_NAME),
                hiveDir,
                DispatcherConnectorFactory.DISPATCHER_CONNECTOR_NAME,
                catalog,
                new WarpPlugin(),
                Collections.emptyMap());
        InternalFunctionBundle.InternalFunctionBundleBuilder functions = InternalFunctionBundle.builder();
        new IcebergPlugin().getFunctions().forEach(functions::functions);
        queryRunner.addFunctions(functions.build());
        return queryRunner;
    }

    @Test
    public void testSimple_withoutWarm_ReturnIceberg()
    {
        computeActual("INSERT INTO t VALUES (1, 'shlomi')");
        MaterializedResult materializedRows = computeActual(String.format("SELECT * FROM t WHERE %s = 1", C1));
        assertThat(materializedRows.getRowCount()).isEqualTo(1);
    }

    @Test
    public void testPartitionOnTimestampColumn()
    {
        try {
            computeActual("CREATE TABLE pt (\n" +
                    "    id INTEGER,\n" +
                    "    a VARCHAR,\n" +
                    "    timestamp_col TIMESTAMP\n" +
                    ")\n" +
                    "WITH (\n" +
                    "    format = 'PARQUET',\n" +
                    "    partitioning = ARRAY['timestamp_col']\n" +
                    ")");
            assertUpdate("INSERT INTO pt(id, a, timestamp_col) VALUES(1, 'bla', CAST('2024-02-13 10:15:30' AS TIMESTAMP))", 1);

            Session warmSession = Session.builder(getSession())
                    .setSystemProperty(catalog + "." + ENABLE_DEFAULT_WARMING, "true")
                    .build();
            @Language("SQL") String query = "SELECT * FROM pt WHERE timestamp_col=CAST('2024-02-13 10:15:30' AS TIMESTAMP)";
            warmAndValidate(query, warmSession, 3, 1, 0);
            Map<String, Long> expectedQueryStats = Map.of(
                    VARADA_MATCH_COLUMNS_STAT, 0L,
                    "prefilled_collect_columns", 1L,
                    VARADA_COLLECT_COLUMNS_STAT, 2L);
            validateQueryStats(query, getSession(), expectedQueryStats);
        }
        finally {
            computeActual("DROP TABLE IF EXISTS pt");
        }
    }

    @Test
    public void testRenameWithPredicate()
            throws IOException
    {
        try {
            assertUpdate("CREATE TABLE schema.my_table3 (c1 integer,c2 integer) WITH (format = 'PARQUET', partitioning = ARRAY[])");
            computeActual(getSession(), "INSERT INTO schema.my_table3 VALUES (1, 2)");
            createWarmupRules(DEFAULT_SCHEMA,
                    "my_table3",
                    Map.of("c1", Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, DEFAULT_PRIORITY, Duration.ofSeconds(0)),
                                    new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BASIC, DEFAULT_PRIORITY, Duration.ofSeconds(0))),
                            "c2", Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, DEFAULT_PRIORITY, Duration.ofSeconds(0)),
                                    new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_BASIC, DEFAULT_PRIORITY, Duration.ofSeconds(0)))));
            Session warmSession = Session.builder(getSession())
                    .setSystemProperty(catalog + "." + ENABLE_DEFAULT_WARMING, "false")
                    .build();
            warmAndValidate("select * from schema.my_table3", warmSession, 4, 1, 0);

            @Language("SQL") String query = "select * from schema.my_table3 where c1 > 0 and c2 > 0";
            Map<String, Long> expectedQueryStats = Map.of(
                    VARADA_MATCH_COLUMNS_STAT, 2L,
                    VARADA_COLLECT_COLUMNS_STAT, 2L);
            validateQueryStats(query, getSession(), expectedQueryStats);

            computeActual("ALTER TABLE schema.my_table3 RENAME COLUMN c1 TO tmpColumn");
            query = "select * from schema.my_table3 where tmpColumn > 0 and c2 > 0";
            expectedQueryStats = Map.of(
                    VARADA_MATCH_COLUMNS_STAT, 2L,
                    VARADA_COLLECT_COLUMNS_STAT, 2L);
            validateQueryStats(query, getSession(), expectedQueryStats);
            int expectedDeadObjects = 4;
            validateDemoter(expectedDeadObjects);
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS schema.my_table3");
        }
    }

    @Test
    public void testRename()
            throws IOException
    {
        try {
            assertUpdate("CREATE TABLE schema.my_table1 (c1 integer,c2 integer) WITH (format = 'PARQUET', partitioning = ARRAY[])");
            computeActual(getSession(), "INSERT INTO schema.my_table1 VALUES (1, 2)");
            createWarmupRules(DEFAULT_SCHEMA,
                    "my_table1",
                    Map.of("c1", Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, DEFAULT_PRIORITY, Duration.ofSeconds(0))),
                            "c2", Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, DEFAULT_PRIORITY, Duration.ofSeconds(0)))));
            Session warmSession = Session.builder(getSession())
                    .setSystemProperty(catalog + "." + ENABLE_DEFAULT_WARMING, "false")
                    .build();
            warmAndValidate("select * from schema.my_table1", warmSession, 2, 1, 0);

            @Language("SQL") String query = "select * from schema.my_table1";
            Map<String, Long> expectedQueryStats = Map.of(VARADA_COLLECT_COLUMNS_STAT, 2L);
            validateQueryStats(query, getSession(), expectedQueryStats);

            computeActual("ALTER TABLE schema.my_table1 RENAME COLUMN c1 TO tmpColumn");

            query = "select tmpColumn from schema.my_table1";
            expectedQueryStats = Map.of(VARADA_COLLECT_COLUMNS_STAT, 1L);
            validateQueryStats(query, getSession(), expectedQueryStats);

            //now create new split
            computeActual(getSession(), "INSERT INTO schema.my_table1 VALUES (9, 9)");
            // after altering the table a new snapshot is created so we are warming all elements
            warmAndValidate("select * from schema.my_table1", warmSession, 2, 2, 0);

            query = "select * from schema.my_table1";
            expectedQueryStats = Map.of(VARADA_COLLECT_COLUMNS_STAT, 2L);
            validateQueryStats(query, getSession(), expectedQueryStats);
            warmSession = Session.builder(getSession())
                    .setSystemProperty(catalog + "." + ENABLE_DEFAULT_WARMING, "true")
                    .build();
            warmAndValidate("select tmpColumn from schema.my_table1 where tmpColumn > 5", warmSession, 2, 1, 0);

            int expectedDeadObjects = 4; // 1 from previous snapshot and 3 objects with ttl 0 (tmpColumn ttl -1)
            validateDemoter(expectedDeadObjects);
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS schema.my_table1");
        }
    }

    @Test
    public void testRenameSwapColumn()
            throws IOException
    {
        try {
            assertUpdate("CREATE TABLE schema.my_table (int1 integer,v1 varchar) WITH (format = 'PARQUET', partitioning = ARRAY[])");
            computeActual(getSession(), "INSERT INTO schema.my_table VALUES (1, 'string')");
            createWarmupRules(DEFAULT_SCHEMA,
                    "my_table",
                    Map.of("int1", Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, DEFAULT_PRIORITY, Duration.ofSeconds(0))),
                            "v1", Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, DEFAULT_PRIORITY, Duration.ofSeconds(0)))));
            Session warmSession = Session.builder(getSession())
                    .setSystemProperty(catalog + "." + ENABLE_DEFAULT_WARMING, "false")
                    .build();
            warmAndValidate("select * from schema.my_table", warmSession, 2, 1, 0);
            @Language("SQL") String query = "select * from schema.my_table";
            Map<String, Long> expectedQueryStats = Map.of(VARADA_COLLECT_COLUMNS_STAT, 2L);
            validateQueryStats(query, getSession(), expectedQueryStats);

            computeActual("ALTER TABLE schema.my_table RENAME COLUMN int1 TO tmpColumn");
            computeActual("ALTER TABLE schema.my_table RENAME COLUMN v1 TO int1");
            computeActual("ALTER TABLE schema.my_table RENAME COLUMN tmpColumn TO v1");

            query = "select * from schema.my_table";
            expectedQueryStats = Map.of(VARADA_COLLECT_COLUMNS_STAT, 2L);
            validateQueryStats(query, getSession(), expectedQueryStats);

            //now create new split
            computeActual(getSession(), "INSERT INTO schema.my_table VALUES (9, 'another string')");
            //each alter table has a different snapshot id which get warm
            warmAndValidate("select * from schema.my_table", warmSession, 4, 2, 0);

            query = "select * from schema.my_table";
            expectedQueryStats = Map.of(VARADA_COLLECT_COLUMNS_STAT, 4L);
            validateQueryStats(query, getSession(), expectedQueryStats);

            validateDemoter(6);
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS schema.my_table");
        }
    }

    @Test
    public void testCount()
            throws IOException
    {
        computeActual(getSession(), "INSERT INTO t VALUES (1, 'shlomi')");

        MaterializedResult result = computeActual(getSession(), "select count(*) from t");
        assertThat(result.getRowCount()).isEqualTo(1); // collect from hive
        assertThat(result.getMaterializedRows().getFirst().getField(0)).isEqualTo(1L);

        String jmxTable = "io.trino.plugin.warp.gen.stats:*,name=dispatcherpagesource." + catalog + ",type=varadastatsdispatcherpagesource";
        computeActual(createJmxSession(), "show tables");
        MaterializedRow statsMaterializedRow = getServiceStats(createJmxSession(),
                jmxTable,
                ImmutableList.of("empty_collect_columns"));
        assertThat((long) statsMaterializedRow.getField(0))
                .describedAs("empty_collect_columns is none zero")
                .isZero();

        createWarmupRules(DEFAULT_SCHEMA,
                "t",
                Map.of(C1, Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, DEFAULT_PRIORITY, DEFAULT_TTL)),
                        C2, Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, DEFAULT_PRIORITY, DEFAULT_TTL))));

        warmAndValidate("select * from t",
                false,
                2,
                1);

        result = computeActual(getSession(), "select count(*) from t");
        assertThat(result.getRowCount()).isEqualTo(1); // collect from row group
        assertThat(result.getMaterializedRows().getFirst().getField(0)).isEqualTo(1L);

        statsMaterializedRow = getServiceStats(createJmxSession(),
                jmxTable,
                ImmutableList.of("empty_collect_columns"));
        assertThat((long) statsMaterializedRow.getField(0))
                .describedAs("empty_collect_columns is none zero")
                .isEqualTo(1);
    }

    @Test
    public void testLongTimestampWithTimeZoneType()
    {
        computeActual("CREATE TABLE longTimezoneTable (int1 integer, \n" +
                "longTimestampWithTimeZoneTypeColumn TIMESTAMP(6) WITH TIME ZONE)");
        computeActual("INSERT INTO longTimezoneTable (int1, longTimestampWithTimeZoneTypeColumn)\n" +
                "VALUES (1, TIMESTAMP '2023-06-18 10:30:00.000000 America/New_York')");
        warmAndValidate("select * from longTimezoneTable", true, 1, 1);
        @Language("SQL") String query = "SELECT longTimestampWithTimeZoneTypeColumn FROM longTimezoneTable WHERE longTimestampWithTimeZoneTypeColumn >= TIMESTAMP '2023-06-18 10:30:00.000000 America/New_York'";
        //predicate in domain
        Map<String, Long> expectedQueryStats = Map.of(
                "varada_collect_columns", 0L,
                "varada_match_columns", 0L,
                "external_collect_columns", 1L,
                "external_match_columns", 0L);
        validateQueryStats(query, getSession(), expectedQueryStats);

        query = "SELECT longTimestampWithTimeZoneTypeColumn FROM longTimezoneTable WHERE day(longTimestampWithTimeZoneTypeColumn) > 3";
        expectedQueryStats = Map.of(
                "varada_collect_columns", 0L,
                "varada_match_columns", 0L,
                "external_collect_columns", 1L,
                "external_match_columns", 0L);
        validateQueryStats(query, getSession(), expectedQueryStats);
        computeActual("drop table longTimezoneTable");
    }

    @Test
    public void testMerge()
    {
        assertUpdate(getSession(), "INSERT INTO t VALUES (1, 'shlomi1')", 1);
        assertUpdate(getSession(), "INSERT INTO t VALUES (2, 'shlomi2')", 1);

        MaterializedResult result = computeActual(getSession(), "select count(*) from t");
        assertThat(result.getMaterializedRows().getFirst().getField(0)).isEqualTo(2L); // collect from row group

        assertUpdate("CREATE TABLE t2 (int2 int, var2 varchar)");
        assertUpdate(getSession(), "INSERT INTO t2 VALUES (2, 'shlomi2-1')", 1);

        MaterializedResult materializedRows = computeActual("select * from t2");
        assertThat(materializedRows.getRowCount()).isEqualTo(1);

        assertUpdate(getSession(), "update t2 set int2 =1 where int2=2", 1);
        assertUpdate(getSession(), "merge into t using t2 on t.int1=t2.int2 when matched then delete", 1);

        materializedRows = computeActual("select * from t2");
        assertThat(materializedRows.getRowCount()).isEqualTo(1);

        materializedRows = computeActual("select * from t");
        assertThat(materializedRows.getRowCount()).isEqualTo(1);
    }

    @Test
    public void testDuplicateSourceIdPartition()
    {
        String table = "pt1";
        String aCol = "a";
        String dateIntCol = "date_int";
        String dateDateCol = "date_date";
        computeActual(format("CREATE TABLE %s(%s varchar, %s integer, %s date) " +
                        "WITH (format='PARQUET', partitioning = ARRAY['bucket(%s, 1)', 'truncate(%s, 1)'])",
                table, aCol, dateIntCol, dateDateCol, aCol, aCol));
        int partitionValue = 20190315;
        @Language("SQL") String sql = format("INSERT INTO %s(%s, %s, %s) VALUES('a-%d', %d, CAST('2020-04-%d%d' AS date))",
                table, aCol, dateIntCol, dateDateCol, 1, partitionValue, 1, 2);
        assertUpdate(sql, 1);
        computeActual(format("SELECT * FROM %s", table));
    }

    @Test
    public void testRenamePartition()
            throws IOException
    {
        String table = "pt1";
        String aCol = "a";
        String dateIntCol = "date_int";
        String dateDateCol = "date_date";
        computeActual(format("CREATE TABLE %s(%s varchar, %s integer, %s date) " +
                        "WITH (format='PARQUET', partitioning = ARRAY['%s'])",
                table, aCol, dateIntCol, dateDateCol, dateIntCol));
        int partitionValue = 20190315;
        int notPartitionValue = 4;
        @Language("SQL") String sql = format("INSERT INTO %s(%s, %s, %s) VALUES('a-%d', %d, CAST('2020-04-%d%d' AS date))",
                table, aCol, dateIntCol, dateDateCol, 1, partitionValue, 1, 2);
        assertUpdate(sql, 1);

        WarmupColRuleData ruleNotMatchPartition = new WarmupColRuleData(0,
                DEFAULT_SCHEMA,
                table,
                new RegularColumnData(dateDateCol),
                WarmUpType.WARM_UP_TYPE_DATA,
                5,
                Duration.ofMillis(10),
                ImmutableSet.of(new PartitionValueWarmupPredicateRule(dateIntCol, String.valueOf(notPartitionValue))));
        executeRestCommand(WarmupRuleService.WARMUP_PATH, WarmupTask.TASK_NAME_SET, List.of(ruleNotMatchPartition), HttpMethod.POST, HttpURLConnection.HTTP_OK);
        createWarmupRules(DEFAULT_SCHEMA,
                table,
                Map.of(aCol, Set.of(new WarmupPropertiesData(WarmUpType.WARM_UP_TYPE_DATA, DEFAULT_PRIORITY, DEFAULT_TTL))));

        Session warmSession = Session.builder(getSession())
                .setSystemProperty(catalog + "." + ENABLE_DEFAULT_WARMING, "false")
                .build();
        @Language("SQL") String query = format("select %s,%s,%s from pt1", aCol, dateIntCol, dateDateCol);
        warmAndValidate(query, warmSession, 1, 1, 0);

        Map<String, Long> expectedQueryStats = Map.of(
                VARADA_COLLECT_COLUMNS_STAT, 1L,
                PREFILLED_COLUMNS_STAT, 1L);
        validateQueryStats(query, getSession(), expectedQueryStats);

        WarmupColRuleData ruleMatchPartition = new WarmupColRuleData(0,
                DEFAULT_SCHEMA,
                table,
                new RegularColumnData(dateDateCol),
                WarmUpType.WARM_UP_TYPE_DATA,
                5,
                Duration.ofMillis(10),
                ImmutableSet.of(new PartitionValueWarmupPredicateRule(dateIntCol, String.valueOf(partitionValue))));
        executeRestCommand(WarmupRuleService.WARMUP_PATH, WarmupTask.TASK_NAME_SET, List.of(ruleMatchPartition), HttpMethod.POST, HttpURLConnection.HTTP_OK);
        warmAndValidate(query, warmSession, 1, 1, 0);

        expectedQueryStats = Map.of(
                VARADA_COLLECT_COLUMNS_STAT, 2L,
                PREFILLED_COLUMNS_STAT, 1L);
        validateQueryStats(query, getSession(), expectedQueryStats);

        String newColumnName = "renameColumn";
        computeActual(format("ALTER TABLE %s.%s RENAME COLUMN %s TO %s", DEFAULT_SCHEMA, table, dateIntCol, newColumnName));
        query = query.replace(dateIntCol, newColumnName);
        expectedQueryStats = Map.of(
                VARADA_COLLECT_COLUMNS_STAT, 2L,
                PREFILLED_COLUMNS_STAT, 1L);
        validateQueryStats(query, getSession(), expectedQueryStats);
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
                            "WITH (format='PARQUET', partitioning = ARRAY['%s', '%s'])",
                    table, aCol, dateIntCol, dateDateCol, dateIntCol, dateDateCol));

            IntStream.range(0, 2).forEach(indexDateInt -> IntStream.range(1, 3).forEach(indexDateDate -> {
                @Language("SQL") String sql = format("INSERT INTO %s(%s, %s, %s) VALUES('a-%d', 2019031%d, CAST('2020-04-%d%d' AS date))",
                        table, aCol, dateIntCol, dateDateCol, indexDateDate, indexDateInt, indexDateInt, indexDateDate);
                assertUpdate(sql,
                        1);
            }));

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
                    CACHED_TOTAL_ROWS, 1L,
                    VARADA_MATCH_COLUMNS_STAT, 1L,
                    VARADA_COLLECT_COLUMNS_STAT, 0L,
                    PREFILLED_COLUMNS_STAT, 2L,
                    EXTERNAL_MATCH_STAT, 0L,
                    EXTERNAL_COLLECT_STAT, 0L);
            validateQueryStats(query, getSession(), expectedQueryStats, OptionalInt.of(1));

            query = format("SELECT %s, %s FROM %s WHERE %s=20190311 AND %s=CAST('2020-04-12' AS date) AND %s='a-2'",
                    aCol, dateIntCol, table, dateIntCol, dateDateCol, aCol);
            expectedQueryStats = Map.of(
                    CACHED_TOTAL_ROWS, 1L,
                    VARADA_MATCH_COLUMNS_STAT, 1L,
                    VARADA_COLLECT_COLUMNS_STAT, 0L,
                    PREFILLED_COLUMNS_STAT, 2L,
                    EXTERNAL_MATCH_STAT, 0L,
                    EXTERNAL_COLLECT_STAT, 0L);
            validateQueryStats(query, getSession(), expectedQueryStats, OptionalInt.of(1));
        }
        finally {
            computeActual("DROP TABLE IF EXISTS pt");
        }
    }

    @Test
    public void testTimestamp6WithTimezone()
    {
        assertUpdate("CREATE TABLE timestamp_6(timestamp_col TIMESTAMP(6) with time zone, another_column TIMESTAMP(6) with time zone) WITH (format='PARQUET')");
        computeActual("INSERT INTO timestamp_6 VALUES (CAST('1969-12-31 15:03:00.123456 +01:00' as TIMESTAMP), CAST('1969-12-31 15:03:00.123456 +02:00' as TIMESTAMP))");
        computeActual("SELECT * FROM timestamp_6 WHERE timestamp_col = TIMESTAMP '1969-12-31 15:03:00.123456 +01:00' OR another_column = TIMESTAMP '1969-12-31 15:03:00.123456 +01:00'");
    }
}
