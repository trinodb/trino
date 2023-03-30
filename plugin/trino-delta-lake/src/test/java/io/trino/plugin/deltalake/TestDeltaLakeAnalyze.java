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
package io.trino.plugin.deltalake;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DataProviders;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.testng.annotations.Test;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Optional;

import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.DELTA_CATALOG;
import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.TPCH_SCHEMA;
import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.createDeltaLakeQueryRunner;
import static io.trino.plugin.deltalake.DeltaLakeSessionProperties.EXTENDED_STATISTICS_COLLECT_ON_WRITE;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.INSERT_TABLE;
import static io.trino.testing.TestingAccessControlManager.privilege;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static java.time.ZoneOffset.UTC;

// smoke test which covers ANALYZE compatibility with different filesystems is part of BaseDeltaLakeConnectorSmokeTest
public class TestDeltaLakeAnalyze
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createDeltaLakeQueryRunner(
                DELTA_CATALOG,
                ImmutableMap.of(),
                ImmutableMap.of("delta.enable-non-concurrent-writes", "true"));
    }

    @Test
    public void testAnalyze()
    {
        testAnalyze(Optional.empty());
    }

    @Test
    public void testAnalyzeWithCheckpoints()
    {
        testAnalyze(Optional.of(1));
    }

    private void testAnalyze(Optional<Integer> checkpointInterval)
    {
        String tableName = "test_analyze_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName
                + (checkpointInterval.isPresent() ? format(" WITH (checkpoint_interval = %s)", checkpointInterval.get()) : "")
                + " AS SELECT * FROM tpch.sf1.nation", 25);

        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', null, 25.0, 0.0, null, 0, 24)," +
                        "('regionkey', null, 5.0, 0.0, null, 0, 4)," +
                        "('comment', 1857.0, 25.0, 0.0, null, null, null)," +
                        "('name', 177.0, 25.0, 0.0, null, null, null)," +
                        "(null, null, null, null, 25.0, null, null)");

        // check that analyze does not change already calculated statistics
        assertUpdate("ANALYZE " + tableName);

        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', null, 25.0, 0.0, null, 0, 24)," +
                        "('regionkey', null, 5.0, 0.0, null, 0, 4)," +
                        "('comment', 1857.0, 25.0, 0.0, null, null, null)," +
                        "('name', 177.0, 25.0, 0.0, null, null, null)," +
                        "(null, null, null, null, 25.0, null, null)");

        // insert one more copy
        assertUpdate("INSERT INTO " + tableName + " SELECT * FROM tpch.sf1.nation", 25);

        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', null, 25.0, 0.0, null, 0, 24)," +
                        "('regionkey', null, 5.0, 0.0, null, 0, 4)," +
                        "('comment', 3714.0, 25.0, 0.0, null, null, null)," +
                        "('name', 354.0, 25.0, 0.0, null, null, null)," +
                        "(null, null, null, null, 50.0, null, null)");

        // check that analyze does not change already calculated statistics
        assertUpdate("ANALYZE " + tableName);

        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', null, 25.0, 0.0, null, 0, 24)," +
                        "('regionkey', null, 5.0, 0.0, null, 0, 4)," +
                        "('comment', 3714.0, 25.0, 0.0, null, null, null)," +
                        "('name', 354.0, 25.0, 0.0, null, null, null)," +
                        "(null, null, null, null, 50.0, null, null)");

        // insert modified rows
        assertUpdate("INSERT INTO " + tableName + " SELECT nationkey + 25, reverse(name), regionkey + 5, reverse(comment) FROM tpch.sf1.nation", 25);

        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', null, 50.0, 0.0, null, 0, 49)," +
                        "('regionkey', null, 10.0, 0.0, null, 0, 9)," +
                        "('comment', 5571.0, 50.0, 0.0, null, null, null)," +
                        "('name', 531.0, 50.0, 0.0, null, null, null)," +
                        "(null, null, null, null, 75.0, null, null)");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testAnalyzePartitioned()
    {
        String tableName = "test_analyze_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName
                + " WITH ("
                + "   partitioned_by = ARRAY['regionkey']"
                + ")"
                + "AS SELECT * FROM tpch.sf1.nation", 25);

        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', null, 25.0, 0.0, null, 0, 24)," +
                        "('regionkey', null, 5.0, 0.0, null, null, null)," +
                        "('comment', 1857.0, 25.0, 0.0, null, null, null)," +
                        "('name', 177.0, 25.0, 0.0, null, null, null)," +
                        "(null, null, null, null, 25.0, null, null)");

        // check that analyze does not change already calculated statistics
        assertUpdate("ANALYZE " + tableName);

        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', null, 25.0, 0.0, null, 0, 24)," +
                        "('regionkey', null, 5.0, 0.0, null, null, null)," +
                        "('comment', 1857.0, 25.0, 0.0, null, null, null)," +
                        "('name', 177.0, 25.0, 0.0, null, null, null)," +
                        "(null, null, null, null, 25.0, null, null)");

        // insert one more copy
        assertUpdate("INSERT INTO " + tableName + " SELECT * FROM tpch.sf1.nation", 25);

        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', null, 25.0, 0.0, null, 0, 24)," +
                        "('regionkey', null, 5.0, 0.0, null, null, null)," +
                        "('comment', 3714.0, 25.0, 0.0, null, null, null)," +
                        "('name', 354.0, 25.0, 0.0, null, null, null)," +
                        "(null, null, null, null, 50.0, null, null)");

        // check that analyze does not change already calculated statistics
        assertUpdate("ANALYZE " + tableName);

        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', null, 25.0, 0.0, null, 0, 24)," +
                        "('regionkey', null, 5.0, 0.0, null, null, null)," +
                        "('comment', 3714.0, 25.0, 0.0, null, null, null)," +
                        "('name', 354.0, 25.0, 0.0, null, null, null)," +
                        "(null, null, null, null, 50.0, null, null)");

        // insert modified rows
        assertUpdate("INSERT INTO " + tableName + " SELECT nationkey + 25, reverse(name), regionkey + 5, reverse(comment) FROM tpch.sf1.nation", 25);

        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', null, 50.0, 0.0, null, 0, 49)," +
                        "('regionkey', null, 10.0, 0.0, null, null, null)," +
                        "('comment', 5571.0, 50.0, 0.0, null, null, null)," +
                        "('name', 531.0, 50.0, 0.0, null, null, null)," +
                        "(null, null, null, null, 75.0, null, null)");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testAnalyzeEmpty()
    {
        String tableName = "test_analyze_empty_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM tpch.sf1.nation WHERE false", 0);

        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', 0.0, 0.0, 1.0, null, null, null)," +
                        "('regionkey', 0.0, 0.0, 1.0, null, null, null)," +
                        "('comment', 0.0, 0.0, 1.0, null, null, null)," +
                        "('name', 0.0, 0.0, 1.0, null, null, null)," +
                        "(null, null, null, null, 0, null, null)");

        assertUpdate("ANALYZE " + tableName);

        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', 0.0, 0.0, 1.0, null, null, null)," +
                        "('regionkey', 0.0, 0.0, 1.0, null, null, null)," +
                        "('comment', 0.0, 0.0, 1.0, null, null, null)," +
                        "('name', 0.0, 0.0, 1.0, null, null, null)," +
                        "(null, null, null, null, 0, null, null)");

        // add some data and reanalyze
        assertUpdate("INSERT INTO " + tableName + " SELECT * FROM tpch.sf1.nation", 25);

        assertUpdate("ANALYZE " + tableName);

        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', null, 25.0, 0.0, null, 0, 24)," +
                        "('regionkey', null, 5.0, 0.0, null, 0, 4)," +
                        "('comment', 1857.0, 25.0, 0.0, null, null, null)," +
                        "('name', 177.0, 25.0, 0.0, null, null, null)," +
                        "(null, null, null, null, 25.0, null, null)");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testAnalyzeExtendedStatisticsDisabled()
    {
        String tableName = "test_analyze_extended_stats_disabled" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM tpch.sf1.nation", 25);

        Session extendedStatisticsDisabled = Session.builder(getSession())
                .setCatalogSessionProperty(getSession().getCatalog().orElseThrow(), "extended_statistics_enabled", "false")
                .build();

        assertQueryFails(
                extendedStatisticsDisabled,
                "ANALYZE " + tableName,
                "ANALYZE not supported if extended statistics are disabled. Enable via delta.extended-statistics.enabled config property or extended_statistics_enabled session property.");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testAnalyzeWithFilesModifiedAfter()
            throws InterruptedException
    {
        String tableName = "test_analyze_" + randomNameSuffix();

        assertUpdate(
                withStatsOnWrite(false),
                "CREATE TABLE " + tableName + " AS SELECT * FROM tpch.sf1.nation",
                25);

        Thread.sleep(100);
        Instant afterInitialDataIngestion = Instant.now();
        Thread.sleep(100);

        // add some more data
        // insert one more copy; should not influence stats other than rowcount
        assertUpdate("INSERT INTO " + tableName + " SELECT * FROM tpch.sf1.nation WHERE nationkey < 5", 5);

        // ANALYZE only new data
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("'TIMESTAMP '''yyyy-MM-dd HH:mm:ss.SSS VV''").withZone(UTC);
        getDistributedQueryRunner().executeWithQueryId(
                getSession(),
                format("ANALYZE %s WITH(files_modified_after = %s)", tableName, formatter.format(afterInitialDataIngestion)));

        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', null, 5.0, 0.0, null, 0, 24)," +
                        "('regionkey', null, 3.0, 0.0, null, 0, 4)," +
                        "('comment', 434.0, 5.0, 0.0, null, null, null)," +
                        "('name', 33.0, 5.0, 0.0, null, null, null)," +
                        "(null, null, null, null, 30.0, null, null)");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testAnalyzeSomeColumns()
    {
        String tableName = "test_analyze_some_columns" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM tpch.sf1.nation", 25);

        // analyze empty list of columns
        assertQueryFails(format("ANALYZE %s WITH(columns = ARRAY[])", tableName), "Cannot specify empty list of columns for analysis");

        // specify inexistent column
        assertQueryFails(format("ANALYZE %s WITH(columns = ARRAY['nationkey', 'blah'])", tableName), "\\QInvalid columns specified for analysis: [blah]\\E");

        // analyze nationkey and regionkey
        assertUpdate(format("ANALYZE %s WITH(columns = ARRAY['nationkey', 'regionkey'])", tableName));
        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', null, 25.0, 0.0, null, 0, 24)," +
                        "('regionkey', null, 5.0, 0.0, null, 0, 4)," +
                        "('comment', null, null, 0.0, null, null, null)," +
                        "('name', null, null, 0.0, null, null, null)," +
                        "(null, null, null, null, 25.0, null, null)");

        // we should not be able to analyze for more columns
        assertQueryFails(format("ANALYZE %s WITH(columns = ARRAY['nationkey', 'regionkey', 'name'])", tableName),
                "List of columns to be analyzed must be a subset of previously used: \\[nationkey, regionkey\\]. To extend list of analyzed columns drop table statistics");

        // we should not be able to analyze for all columns
        assertQueryFails("ANALYZE " + tableName,
                "List of columns to be analyzed must be a subset of previously used: \\[nationkey, regionkey\\]. To extend list of analyzed columns drop table statistics");

        // insert modified rows should update stats only for already used columns
        assertUpdate("INSERT INTO " + tableName + " SELECT nationkey + 25, concat(name, '1'), regionkey + 5, concat(comment, '21') FROM tpch.sf1.nation", 25);

        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', null, 50.0, 0.0, null, 0, 49)," +
                        "('regionkey', null, 10.0, 0.0, null, 0, 9)," +
                        "('comment', null, null, 0.0, null, null, null)," +
                        "('name', null, null, 0.0, null, null, null)," +
                        "(null, null, null, null, 50.0, null, null)");

        // insert should not extend list of analyzed columns
        assertQueryFails(
                format("ANALYZE %s WITH(columns = ARRAY['nationkey', 'regionkey', 'name'])", tableName),
                "List of columns to be analyzed must be a subset of previously used: \\[nationkey, regionkey\\]. To extend list of analyzed columns drop table statistics");

        // perform one more analyze for nationkey and regionkey
        assertUpdate(format("ANALYZE %s WITH(columns = ARRAY['nationkey', 'regionkey'])", tableName));
        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', null, 50.0, 0.0, null, 0, 49)," +
                        "('regionkey', null, 10.0, 0.0, null, 0, 9)," +
                        "('comment', null, null, 0.0, null, null, null)," +
                        "('name', null, null, 0.0, null, null, null)," +
                        "(null, null, null, null, 50.0, null, null)");

        // drop stats
        assertUpdate(format("CALL %s.system.drop_extended_stats('%s', '%s')", DELTA_CATALOG, TPCH_SCHEMA, tableName));

        // now we should be able to analyze all columns
        assertUpdate(format("ANALYZE %s", tableName));
        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', null, 50.0, 0.0, null, 0, 49)," +
                        "('regionkey', null, 10.0, 0.0, null, 0, 9)," +
                        "('comment', 3764.0, 50.0, 0.0, null, null, null)," +
                        "('name', 379.0, 50.0, 0.0, null, null, null)," +
                        "(null, null, null, null, 50.0, null, null)");

        // we and we should be able to reanalyze with a subset of columns
        assertUpdate(format("ANALYZE %s WITH(columns = ARRAY['nationkey', 'regionkey'])", tableName));
        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', null, 50.0, 0.0, null, 0, 49)," +
                        "('regionkey', null, 10.0, 0.0, null, 0, 9)," +
                        "('comment', null, null, 0.0, null, null, null)," +
                        "('name', null, null, 0.0, null, null, null)," +
                        "(null, null, null, null, 50.0, null, null)");

        // and even smaller subset
        assertUpdate(format("ANALYZE %s WITH(columns = ARRAY['nationkey'])", tableName));
        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', null, 50.0, 0.0, null, 0, 49)," +
                        "('regionkey', null, null, 0.0, null, 0, 9)," +
                        "('comment', null, null, 0.0, null, null, null)," +
                        "('name', null, null, 0.0, null, null, null)," +
                        "(null, null, null, null, 50.0, null, null)");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testDropExtendedStats()
    {
        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_drop_extended_stats",
                "AS SELECT * FROM tpch.sf1.nation")) {
            String query = "SHOW STATS FOR " + table.getName();
            String baseStats = "VALUES"
                    //  column       size  NDist nullF rows   low  high
                    + "('nationkey', null, null,  0.0, null,    0,   24),"
                    + "('regionkey', null, null,  0.0, null,    0,    4),"
                    + "('comment',   null, null,  0.0, null, null, null),"
                    + "('name',      null, null,  0.0, null, null, null),"
                    + "(null,        null, null, null, 25.0, null, null)";
            String extendedStats = "VALUES"
                    + "('nationkey', null, 25.0,  0.0, null,    0,   24),"
                    + "('regionkey', null,  5.0,  0.0, null,    0,    4),"
                    + "('comment', 1857.0, 25.0,  0.0, null, null, null),"
                    + "('name',     177.0, 25.0,  0.0, null, null, null),"
                    + "(null,        null, null, null, 25.0, null, null)";

            assertQuery(query, extendedStats);

            // Dropping extended stats clears distinct count and leaves other stats alone
            assertUpdate(format("CALL %s.system.drop_extended_stats('%s', '%s')", DELTA_CATALOG, TPCH_SCHEMA, table.getName()));
            assertQuery(query, baseStats);

            // Re-analyzing should work
            assertUpdate("ANALYZE " + table.getName());
            assertQuery(query, extendedStats);
        }
    }

    @Test
    public void testDropMissingStats()
    {
        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_drop_missing_stats",
                "AS SELECT * FROM tpch.sf1.nation")) {
            // When there are no extended stats, the procedure should have no effect
            assertUpdate(format("CALL %s.system.drop_extended_stats('%s', '%s')", DELTA_CATALOG, TPCH_SCHEMA, table.getName()));
            assertQuery(
                    "SHOW STATS FOR " + table.getName(),
                    "VALUES"
                            //  column       size  NDist nullF rows   low  high
                            + "('nationkey', null, null,  0.0, null,    0,   24),"
                            + "('regionkey', null, null,  0.0, null,    0,    4),"
                            + "('comment',   null, null,  0.0, null, null, null),"
                            + "('name',      null, null,  0.0, null, null, null),"
                            + "(null,        null, null, null, 25.0, null, null)");
        }
    }

    @Test
    public void testDropStatsAccessControl()
    {
        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_deny_drop_stats",
                "AS SELECT * FROM tpch.sf1.nation")) {
            assertAccessDenied(
                    format("CALL %s.system.drop_extended_stats('%s', '%s')", DELTA_CATALOG, TPCH_SCHEMA, table.getName()),
                    "Cannot insert into table .*",
                    privilege(table.getName(), INSERT_TABLE));
        }
    }

    /**
     * Verify Delta has good stats for TPC-DS data sets. Note that TPC-DS date_dim contains
     * dates as old as 1900-01-02, which may be problematic.
     */
    @Test
    public void testStatsOnTpcDsData()
    {
        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_old_date_stats",
                "AS SELECT d_date FROM tpcds.tiny.date_dim")) {
            assertUpdate("ANALYZE " + table.getName());
            // Accurate column stats on d_date are important for producing efficient query plans, e.g. on q72
            assertQuery(
                    "SHOW STATS FOR " + table.getName(),
                    "VALUES"
                            + "('d_date', null, 72713.0, 0.0,  null,    '1900-01-02', '2100-01-01'),"
                            + "(null,     null, null,    null, 73049.0, null,         null)");
        }
    }

    @Test
    public void testCreateTableStatisticsWhenCollectionOnWriteDisabled()
    {
        String tableName = "test_statistics_" + randomNameSuffix();
        assertUpdate(
                withStatsOnWrite(false),
                "CREATE TABLE " + tableName + " AS SELECT * FROM tpch.sf1.nation",
                25);

        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', null, null, 0.0, null, 0, 24)," +
                        "('regionkey', null, null, 0.0, null, 0, 4)," +
                        "('comment', null, null, 0.0, null, null, null)," +
                        "('name', null, null, 0.0, null, null, null)," +
                        "(null, null, null, null, 25.0, null, null)");

        assertUpdate("ANALYZE " + tableName);

        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', null, 25.0, 0.0, null, 0, 24)," +
                        "('regionkey', null, 5.0, 0.0, null, 0, 4)," +
                        "('comment', 1857.0, 25.0, 0.0, null, null, null)," +
                        "('name', 177.0, 25.0, 0.0, null, null, null)," +
                        "(null, null, null, null, 25.0, null, null)");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testCreatePartitionedTableStatisticsWhenCollectionOnWriteDisabled()
    {
        String tableName = "test_statistics_" + randomNameSuffix();
        assertUpdate(
                withStatsOnWrite(false),
                "CREATE TABLE " + tableName
                        + " WITH ("
                        + "   partitioned_by = ARRAY['regionkey']"
                        + ")"
                        + "AS SELECT * FROM tpch.sf1.nation",
                25);

        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', null, null, 0.0, null, 0, 24)," +
                        "('regionkey', null, 5.0, 0.0, null, null, null)," +
                        "('comment', null, null, 0.0, null, null, null)," +
                        "('name', null, null, 0.0, null, null, null)," +
                        "(null, null, null, null, 25.0, null, null)");

        assertUpdate("ANALYZE " + tableName);

        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', null, 25.0, 0.0, null, 0, 24)," +
                        "('regionkey', null, 5.0, 0.0, null, null, null)," +
                        "('comment', 1857.0, 25.0, 0.0, null, null, null)," +
                        "('name', 177.0, 25.0, 0.0, null, null, null)," +
                        "(null, null, null, null, 25.0, null, null)");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testStatisticsOnInsertWhenStatsNotCollectedBefore()
    {
        String tableName = "test_statistics_on_insert_when_stats_not_collected_before_" + randomNameSuffix();
        assertUpdate(
                withStatsOnWrite(false),
                "CREATE TABLE " + tableName + " AS SELECT * FROM tpch.sf1.nation", 25);

        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', null, null, 0.0, null, 0, 24)," +
                        "('regionkey', null, null, 0.0, null, 0, 4)," +
                        "('comment', null, null, 0.0, null, null, null)," +
                        "('name', null, null, 0.0, null, null, null)," +
                        "(null, null, null, null, 25.0, null, null)");

        assertUpdate("INSERT INTO " + tableName + " VALUES (111, 'a', 333, 'b')", 1);

        // size and NVD statistics should be based only on data added after statistics are enabled
        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', null, 1.0, 0.0, null, 0, 111)," +
                        "('regionkey', null, 1.0, 0.0, null, 0, 333)," +
                        "('comment', 1.0, 1.0, 0.0, null, null, null)," +
                        "('name', 1.0, 1.0, 0.0, null, null, null)," +
                        "(null, null, null, null, 26.0, null, null)");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testStatisticsOnInsertWhenCollectionOnWriteDisabled()
    {
        String tableName = "test_statistics_on_insert_when_collection_on_write_disabled_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM tpch.sf1.nation", 25);

        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', null, 25.0, 0.0, null, 0, 24)," +
                        "('regionkey', null, 5.0, 0.0, null, 0, 4)," +
                        "('comment', 1857.0, 25.0, 0.0, null, null, null)," +
                        "('name', 177.0, 25.0, 0.0, null, null, null)," +
                        "(null, null, null, null, 25.0, null, null)");

        // insert modified rows
        assertUpdate(
                withStatsOnWrite(false),
                "INSERT INTO " + tableName + " SELECT nationkey + 25, reverse(name), regionkey + 5, reverse(comment) FROM tpch.sf1.nation", 25);

        // without ANALYZE all stats but size and NDV should be updated
        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', null, 25.0, 0.0, null, 0, 49)," +
                        "('regionkey', null, 5.0, 0.0, null, 0, 9)," +
                        "('comment', 1857.0, 25.0, 0.0, null, null, null)," +
                        "('name', 177.0, 25.0, 0.0, null, null, null)," +
                        "(null, null, null, null, 50.0, null, null)");

        // with analyze we should get new size and NDV
        assertUpdate("ANALYZE " + tableName);

        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', null, 50.0, 0.0, null, 0, 49)," +
                        "('regionkey', null, 10.0, 0.0, null, 0, 9)," +
                        "('comment', 3714.0, 50.0, 0.0, null, null, null)," +
                        "('name', 354.0, 50.0, 0.0, null, null, null)," +
                        "(null, null, null, null, 50.0, null, null)");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testPartitionedStatisticsOnInsertWhenCollectionOnWriteDisabled()
    {
        String tableName = "test_partitioned_statistics_on_insert_when_collection_on_write_disabled_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName
                        + " WITH ("
                        + "   partitioned_by = ARRAY['regionkey']"
                        + ")"
                        + "AS SELECT * FROM tpch.sf1.nation",
                25);

        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', null, 25.0, 0.0, null, 0, 24)," +
                        "('regionkey', null, 5.0, 0.0, null,  null, null)," +
                        "('comment', 1857.0, 25.0, 0.0, null, null, null)," +
                        "('name', 177.0, 25.0, 0.0, null, null, null)," +
                        "(null, null, null, null, 25.0, null, null)");

        // insert modified rows
        assertUpdate(
                withStatsOnWrite(false),
                "INSERT INTO " + tableName + " SELECT nationkey + 25, reverse(name), regionkey + 5, reverse(comment) FROM tpch.sf1.nation", 25);

        // without ANALYZE all stats but size and NDV should be updated
        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', null, 25.0, 0.0, null, 0, 49)," +
                        "('regionkey', null, 10.0, 0.0, null,  null, null)," +
                        "('comment', 1857.0, 25.0, 0.0, null, null, null)," +
                        "('name', 177.0, 25.0, 0.0, null, null, null)," +
                        "(null, null, null, null, 50.0, null, null)");

        // with analyze we should get new size and NDV
        assertUpdate("ANALYZE " + tableName);

        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', null, 50.0, 0.0, null, 0, 49)," +
                        "('regionkey', null, 10.0, 0.0, null,  null, null)," +
                        "('comment', 3714.0, 50.0, 0.0, null, null, null)," +
                        "('name', 354.0, 50.0, 0.0, null, null, null)," +
                        "(null, null, null, null, 50.0, null, null)");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testIncrementalStatisticsUpdateOnInsert()
    {
        String tableName = "test_incremental_statistics_update_on_insert_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM tpch.sf1.nation", 25);

        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', null, 25.0, 0.0, null, 0, 24)," +
                        "('regionkey', null, 5.0, 0.0, null, 0, 4)," +
                        "('comment', 1857.0, 25.0, 0.0, null, null, null)," +
                        "('name', 177.0, 25.0, 0.0, null, null, null)," +
                        "(null, null, null, null, 25.0, null, null)");

        assertUpdate("INSERT INTO " + tableName + " SELECT nationkey + 25, reverse(name), regionkey + 5, reverse(comment) FROM tpch.sf1.nation", 25);

        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', null, 50.0, 0.0, null, 0, 49)," +
                        "('regionkey', null, 10.0, 0.0, null, 0, 9)," +
                        "('comment', 3714.0, 50.0, 0.0, null, null, null)," +
                        "('name', 354.0, 50.0, 0.0, null, null, null)," +
                        "(null, null, null, null, 50.0, null, null)");

        assertUpdate("INSERT INTO " + tableName + " SELECT nationkey + 50, reverse(name), regionkey + 5, reverse(comment) FROM tpch.sf1.nation", 25);

        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', null, 75.0, 0.0, null, 0, 74)," +
                        "('regionkey', null, 10.0, 0.0, null, 0, 9)," +
                        "('comment', 5571.0, 50.0, 0.0, null, null, null)," +
                        "('name', 531.0, 50.0, 0.0, null, null, null)," +
                        "(null, null, null, null, 75.0, null, null)");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test(dataProviderClass = DataProviders.class, dataProvider = "trueFalse")
    public void testCollectStatsAfterColumnAdded(boolean collectOnWrite)
    {
        String tableName = "test_collect_stats_after_column_added_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (col_int_1 bigint, col_varchar_1 varchar)");
        assertUpdate("INSERT INTO " + tableName + " VALUES (11, 'aa')", 1);

        assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN col_int_2 bigint");
        assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN col_varchar_2 varchar");

        assertUpdate(
                withStatsOnWrite(collectOnWrite),
                "INSERT INTO " + tableName + " VALUES (12, 'ab', 21, 'ba'), (13, 'ac', 22, 'bb')",
                2);

        if (!collectOnWrite) {
            assertQuery(
                    "SHOW STATS FOR " + tableName,
                    """
                            VALUES
                            ('col_int_1', null, 1.0, 0.0, null, 11, 13),
                            ('col_varchar_1', 2.0, 1.0, 0.0, null, null, null),
                            ('col_int_2', null, null, null, null, 21, 22),
                            ('col_varchar_2', null, null, null, null, null, null),
                            (null, null, null, null, 3.0, null, null)
                            """);

            assertUpdate("ANALYZE " + tableName);
        }

        assertQuery(
                "SHOW STATS FOR " + tableName,
                """
                        VALUES
                        ('col_int_1', null, 3.0, 0.0, null, 11, 13),
                        ('col_varchar_1', 6.0, 3.0, 0.0, null, null, null),
                        ('col_int_2', null, 2.0, 0.1, null, 21, 22),
                        ('col_varchar_2', 4.0, 2.0, 0.1, null, null, null),
                        (null, null, null, null, 3.0, null, null)
                        """);

        assertUpdate("DROP TABLE " + tableName);
    }

    private Session withStatsOnWrite(boolean value)
    {
        Session session = getSession();
        return Session.builder(session)
                .setCatalogSessionProperty(session.getCatalog().orElseThrow(), EXTENDED_STATISTICS_COLLECT_ON_WRITE, Boolean.toString(value))
                .build();
    }
}
