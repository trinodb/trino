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
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.hdfs.HdfsFileSystemFactory;
import io.trino.plugin.deltalake.transactionlog.AddFileEntry;
import io.trino.plugin.deltalake.transactionlog.DeltaLakeTransactionLogEntry;
import io.trino.plugin.deltalake.transactionlog.statistics.DeltaLakeFileStatistics;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DataProviders;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Verify.verify;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.DELTA_CATALOG;
import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.TPCH_SCHEMA;
import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.createDeltaLakeQueryRunner;
import static io.trino.plugin.deltalake.DeltaLakeSessionProperties.EXTENDED_STATISTICS_COLLECT_ON_WRITE;
import static io.trino.plugin.deltalake.DeltaTestingConnectorSession.SESSION;
import static io.trino.plugin.deltalake.TestingDeltaLakeUtils.copyDirectoryContents;
import static io.trino.plugin.deltalake.transactionlog.checkpoint.TransactionLogTail.getEntriesFromJson;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_FILE_SYSTEM_STATS;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.INSERT_TABLE;
import static io.trino.testing.TestingAccessControlManager.privilege;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.Assertions.assertThat;

// smoke test which covers ANALYZE compatibility with different filesystems is part of BaseDeltaLakeConnectorSmokeTest
public class TestDeltaLakeAnalyze
        extends AbstractTestQueryFramework
{
    private static final TrinoFileSystem FILE_SYSTEM = new HdfsFileSystemFactory(HDFS_ENVIRONMENT, HDFS_FILE_SYSTEM_STATS).create(SESSION);

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createDeltaLakeQueryRunner(
                DELTA_CATALOG,
                ImmutableMap.of(),
                ImmutableMap.of(
                        "delta.enable-non-concurrent-writes", "true",
                        "delta.register-table-procedure.enabled", "true"));
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

        String expectedStats = "VALUES " +
                "('nationkey', null, 25.0, 0.0, null, 0, 24)," +
                "('regionkey', null, 5.0, 0.0, null, 0, 4)," +
                "('comment', 1857.0, 25.0, 0.0, null, null, null)," +
                "('name', 177.0, 25.0, 0.0, null, null, null)," +
                "(null, null, null, null, 25.0, null, null)";
        assertQuery(
                "SHOW STATS FOR " + tableName,
                expectedStats);

        // check that analyze with mode = incremental returns the same result as analyze without mode
        assertUpdate("ANALYZE " + tableName + " WITH(mode = 'incremental')");
        assertQuery(
                "SHOW STATS FOR " + tableName,
                expectedStats);

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

        // show that using full_refresh allows us to analyze any subset of columns
        assertUpdate(format("ANALYZE %s WITH(mode = 'full_refresh', columns = ARRAY['nationkey', 'regionkey', 'name'])", tableName), 50);
        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', null, 50.0, 0.0, null, 0, 49)," +
                        "('regionkey', null, 10.0, 0.0, null, 0, 9)," +
                        "('comment', null, null, 0.0, null, null, null)," +
                        "('name', 379.0, 50.0, 0.0, null, null, null)," +
                        "(null, null, null, null, 50.0, null, null)");

        String expectedFullStats = "VALUES " +
                "('nationkey', null, 50.0, 0.0, null, 0, 49)," +
                "('regionkey', null, 10.0, 0.0, null, 0, 9)," +
                "('comment', 3764.0, 50.0, 0.0, null, null, null)," +
                "('name', 379.0, 50.0, 0.0, null, null, null)," +
                "(null, null, null, null, 50.0, null, null)";
        assertUpdate(format("ANALYZE %s WITH(mode = 'full_refresh')", tableName), 50);
        assertQuery("SHOW STATS FOR " + tableName, expectedFullStats);

        // drop stats
        assertUpdate(format("CALL %s.system.drop_extended_stats('%s', '%s')", DELTA_CATALOG, TPCH_SCHEMA, tableName));
        // now we should be able to analyze all columns
        assertUpdate(format("ANALYZE %s", tableName), 50);
        assertQuery("SHOW STATS FOR " + tableName, expectedFullStats);

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
            assertUpdate("ANALYZE " + table.getName(), 25);
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

        assertUpdate("ANALYZE " + tableName, 25);

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

        assertUpdate("ANALYZE " + tableName, 25);

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

    @Test
    public void testForceRecalculateStatsWithDeleteAndUpdate()
    {
        String tableName = "test_recalculate_all_stats_with_delete_and_update_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName
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

        assertUpdate("DELETE FROM " + tableName + " WHERE nationkey = 1", 1);
        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', null, 24.0, 0.0, null, 0, 24)," +
                        "('regionkey', null, 5.0, 0.0, null, 0, 4)," +
                        "('comment', 1857.0, 24.0, 0.0, null, null, null)," +
                        "('name', 177.0, 24.0, 0.0, null, null, null)," +
                        "(null, null, null, null, 24.0, null, null)");
        assertUpdate("UPDATE " + tableName + " SET name = null WHERE nationkey = 2", 1);
        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', null, 24.0, 0.0, null, 0, 24)," +
                        "('regionkey', null, 5.0, 0.0, null, 0, 4)," +
                        "('comment', 1857.0, 24.0, 0.0, null, null, null)," +
                        "('name', 180.84782608695653, 23.5, 0.02083333333333337, null, null, null)," +
                        "(null, null, null, null, 24.0, null, null)");

        assertUpdate(format("ANALYZE %s", tableName));
        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', null, 24.0, 0.0, null, 0, 24)," +
                        "('regionkey', null, 5.0, 0.0, null, 0, 4)," +
                        "('comment', 3638.0, 24.0, 0.0, null, null, null)," +
                        "('name', 346.3695652173913, 23.5, 0.02083333333333337, null, null, null)," +
                        "(null, null, null, null, 24.0, null, null)");

        assertUpdate(format("ANALYZE %s WITH(mode = 'full_refresh')", tableName), 24);
        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', null, 24.0, 0.0, null, 0, 24)," +
                        "('regionkey', null, 5.0, 0.0, null, 0, 4)," +
                        "('comment', 1781.0, 24.0, 0.0, null, null, null)," +
                        "('name', 162.0, 23.0, 0.041666666666666664, null, null, null)," +
                        "(null, null, null, null, 24.0, null, null)");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testForceRecalculateAllStats()
    {
        String tableName = "test_recalculate_all_stats_" + randomNameSuffix();
        assertUpdate(
                withStatsOnWrite(false),
                "CREATE TABLE " + tableName + " AS SELECT nationkey, regionkey, name  FROM tpch.sf1.nation",
                25);

        assertUpdate(
                withStatsOnWrite(true),
                "INSERT INTO " + tableName + " VALUES(27, 1, 'name1')",
                1);

        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', null, 1.0, 0.0, null, 0, 27)," +
                        "('regionkey', null, 1.0, 0.0, null, 0, 4)," +
                        "('name', 5.0, 1.0, 0.0, null, null, null)," +
                        "(null, null, null, null, 26.0, null, null)");

        // check that analyze does not change already calculated statistics
        assertUpdate("ANALYZE " + tableName);

        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', null, 1.0, 0.0, null, 0, 27)," +
                        "('regionkey', null, 1.0, 0.0, null, 0, 4)," +
                        "('name', 5.0, 1.0, 0.0, null, null, null)," +
                        "(null, null, null, null, 26.0, null, null)");

        assertUpdate(format("ANALYZE %s WITH(mode = 'full_refresh')", tableName), 26);
        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', null, 26.0, 0.0, null, 0, 27)," +
                        "('regionkey', null, 5.0, 0.0, null, 0, 4)," +
                        "('name', 182.0, 26.0, 0.0, null, null, null)," +
                        "(null, null, null, null, 26.0, null, null)");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testNoStats()
            throws Exception
    {
        String tableName = copyResourcesAndRegisterTable("no_stats", "trino410/no_stats");
        String expectedData = "VALUES (42, 'foo'), (12, 'ab'), (null, null), (15, 'cd'), (15, 'bar')";

        assertQuery("SELECT * FROM " + tableName, expectedData);
        assertQuery(
                "SHOW STATS FOR " + tableName,
                """
                        VALUES
                        ('c_int', null, null, null, null, null, null),
                        ('c_str', null, null, null, null, null, null),
                        (null, null, null, null, null, null, null)
                        """);

        assertUpdate("ANALYZE " + tableName, 5);
        assertQuery(
                "SHOW STATS FOR " + tableName,
                """
                        VALUES
                        ('c_int', null, 3.0, 0.2, null, 12, 42),
                        ('c_str', 10.0, 4.0, 0.2, null, null, null),
                        (null, null, null, null, 5.0, null, null)
                        """);

        // Ensure that ANALYZE does not change data
        assertQuery("SELECT * FROM " + tableName, expectedData);

        cleanExternalTable(tableName);
    }

    @Test
    public void testNoColumnStats()
            throws Exception
    {
        String tableName = copyResourcesAndRegisterTable("no_column_stats", "databricks73/no_column_stats");
        assertQuery("SELECT * FROM " + tableName, "VALUES (42, 'foo')");

        assertUpdate("ANALYZE " + tableName, 1);
        assertQuery(
                "SHOW STATS FOR " + tableName,
                """
                        VALUES
                        ('c_int', null, 1.0, 0.0, null, 42, 42),
                        ('c_str', 3.0, 1.0, 0.0, null, null, null),
                        (null, null, null, null, 1.0, null, null)
                        """);

        cleanExternalTable(tableName);
    }

    @Test
    public void testNoColumnStatsMixedCase()
            throws Exception
    {
        String tableName = copyResourcesAndRegisterTable("no_column_stats_mixed_case", "databricks104/no_column_stats_mixed_case");
        String tableLocation = getTableLocation(tableName);
        assertQuery("SELECT * FROM " + tableName, "VALUES (11, 'a'), (2, 'b'), (null, null)");

        assertUpdate("ANALYZE " + tableName, 3);
        assertQuery(
                "SHOW STATS FOR " + tableName,
                """
                        VALUES
                        ('c_int', null, 2.0, 0.33333333, null, 2, 11),
                        ('c_str', 2.0, 2.0, 0.33333333, null, null, null),
                        (null, null, null, null, 3.0, null, null)
                        """);

        // Version 3 should be created with recalculated statistics.
        List<DeltaLakeTransactionLogEntry> transactionLogAfterUpdate = getEntriesFromJson(3, tableLocation + "/_delta_log", FILE_SYSTEM).orElseThrow();
        assertThat(transactionLogAfterUpdate).hasSize(2);
        AddFileEntry updateAddFileEntry = transactionLogAfterUpdate.get(1).getAdd();
        DeltaLakeFileStatistics updateStats = updateAddFileEntry.getStats().orElseThrow();
        assertThat(updateStats.getMinValues().orElseThrow().get("c_Int")).isEqualTo(2);
        assertThat(updateStats.getMaxValues().orElseThrow().get("c_Int")).isEqualTo(11);
        assertThat(updateStats.getNullCount("c_Int").orElseThrow()).isEqualTo(1);
        assertThat(updateStats.getNullCount("c_Str").orElseThrow()).isEqualTo(1);

        cleanExternalTable(tableName);
    }

    @Test
    public void testPartiallyNoStats()
            throws Exception
    {
        String tableName = copyResourcesAndRegisterTable("no_stats", "trino410/no_stats");
        // Add additional transaction log entry with statistics
        assertUpdate("INSERT INTO " + tableName + " VALUES (1,'a'), (12,'b')", 2);
        assertQuery("SELECT * FROM " + tableName, " VALUES (42, 'foo'), (12, 'ab'), (null, null), (15, 'cd'), (15, 'bar'), (1, 'a'), (12, 'b')");

        // Simulate initial analysis
        assertUpdate(format("CALL system.drop_extended_stats('%s', '%s')", TPCH_SCHEMA, tableName));

        assertUpdate("ANALYZE " + tableName, 7);
        assertQuery(
                "SHOW STATS FOR " + tableName,
                """
                        VALUES
                        ('c_int', null, 4.0, 0.14285714285714285, null, 1, 42),
                        ('c_str', 12.0, 6.0, 0.14285714285714285, null, null, null),
                        (null, null, null, null, 7.0, null, null)
                        """);

        cleanExternalTable(tableName);
    }

    @Test
    public void testNoStatsPartitionedTable()
            throws Exception
    {
        String tableName = copyResourcesAndRegisterTable("no_stats_partitions", "trino410/no_stats_partitions");
        assertQuery("SELECT * FROM " + tableName,
                """
                        VALUES
                        ('p?p', 42, 'foo'),
                        ('p?p', 12, 'ab'),
                        (null, null, null),
                        ('ppp', 15, 'cd'),
                        ('ppp', 15, 'bar')
                        """);

        assertUpdate("ANALYZE " + tableName, 5);
        assertQuery(
                "SHOW STATS FOR " + tableName,
                """
                        VALUES
                        ('p_str', null, 2.0, 0.2, null, null, null),
                        ('c_int', null, 3.0, 0.2, null, 12, 42),
                        ('c_str', 10.0, 4.0, 0.2, null, null, null),
                        (null, null, null, null, 5.0, null, null)
                        """);

        cleanExternalTable(tableName);
    }

    @Test
    public void testNoStatsVariousTypes()
            throws Exception
    {
        String tableName = copyResourcesAndRegisterTable("no_stats_various_types", "trino410/no_stats_various_types");
        assertQuery("SELECT c_boolean, c_tinyint, c_smallint, c_integer, c_bigint, c_real, c_double, c_decimal1, c_decimal2, c_date1, CAST(c_timestamp AS TIMESTAMP), c_varchar1, c_varchar2, c_varbinary FROM " + tableName,
                """
                        VALUES
                        (false, 37, 32123, 1274942432, 312739231274942432, 567.123, 1234567890123.123, 12.345, 123456789012.345, '1999-01-01', '2020-02-12 14:03:00', 'ab', 'de',  X'12ab3f'),
                        (true, 127, 32767, 2147483647, 9223372036854775807, 999999.999, 9999999999999.999, 99.999, 999999999999.99, '2028-10-04', '2199-12-31 22:59:59.999', 'zzz', 'zzz',  X'ffffffffffffffffffff'),
                        (null,null,null,null,null,null,null,null,null,null,null,null,null,null),
                        (null,null,null,null,null,null,null,null,null,null,null,null,null,null)
                        """);

        assertUpdate("ANALYZE " + tableName, 4);
        assertQuery(
                "SHOW STATS FOR " + tableName,
                """
                        VALUES
                        ('c_boolean', null, 2.0, 0.5, null, null, null),
                        ('c_tinyint', null, 2.0, 0.5, null, '37', '127'),
                        ('c_smallint', null, 2.0, 0.5, null, '32123', '32767'),
                        ('c_integer', null, 2.0, 0.5, null, '1274942432', '2147483647'),
                        ('c_bigint', null, 2.0, 0.5, null, '312739231274942464', '9223372036854775807'),
                        ('c_real', null, 2.0, 0.5, null, '567.123', '1000000.0'),
                        ('c_double', null, 2.0, 0.5, null, '1.234567890123123E12', '9.999999999999998E12'),
                        ('c_decimal1', null, 2.0, 0.5, null, '12.345', '99.999'),
                        ('c_decimal2', null, 2.0, 0.5, null, '1.23456789012345E11', '9.9999999999999E11'),
                        ('c_date1', null, 2.0, 0.5, null, '1999-01-01', '2028-10-04'),
                        ('c_timestamp', null, 2.0, 0.5, null, '2020-02-12 14:03:00.000 UTC', '2199-12-31 22:59:59.999 UTC'),
                        ('c_varchar1', 5.0, 2.0, 0.5, null, null, null),
                        ('c_varchar2', 5.0, 2.0, 0.5, null, null, null),
                        ('c_varbinary', 13.0, 2.0, 0.5, null, null, null),
                        (null, null, null, null, 4.0, null, null)
                        """);

        cleanExternalTable(tableName);
    }

    @Test
    public void testNoStatsWithColumnMappingModeId()
            throws Exception
    {
        String tableName = copyResourcesAndRegisterTable("no_stats_column_mapping_id", "databricks104/no_stats_column_mapping_id");

        assertQuery("SELECT * FROM " + tableName, " VALUES (42, 'foo'), (1, 'a'), (2, 'b'), (null, null)");

        assertUpdate("ANALYZE " + tableName, 4);
        assertQuery(
                "SHOW STATS FOR " + tableName,
                """
                        VALUES
                        ('c_int', null, 3.0, 0.25, null, 1, 42),
                        ('c_str', 5.0, 3.0, 0.25, null, null, null),
                        (null, null, null, null, 4.0, null, null)
                        """);

        cleanExternalTable(tableName);
    }

    private String copyResourcesAndRegisterTable(String resourceTable, String resourcePath)
            throws IOException, URISyntaxException
    {
        Path tableLocation = Files.createTempDirectory(null);
        String tableName = resourceTable + randomNameSuffix();
        URI resourcesLocation = getClass().getClassLoader().getResource(resourcePath).toURI();
        copyDirectoryContents(Path.of(resourcesLocation), tableLocation);
        assertUpdate(format("CALL system.register_table('%s', '%s', '%s')", getSession().getSchema().orElseThrow(), tableName, tableLocation.toUri()));
        return tableName;
    }

    private Session withStatsOnWrite(boolean value)
    {
        Session session = getSession();
        return Session.builder(session)
                .setCatalogSessionProperty(session.getCatalog().orElseThrow(), EXTENDED_STATISTICS_COLLECT_ON_WRITE, Boolean.toString(value))
                .build();
    }

    private String getTableLocation(String tableName)
    {
        Pattern locationPattern = Pattern.compile(".*location = '(.*?)'.*", Pattern.DOTALL);
        Matcher m = locationPattern.matcher((String) computeActual("SHOW CREATE TABLE " + tableName).getOnlyValue());
        if (m.find()) {
            String location = m.group(1);
            verify(!m.find(), "Unexpected second match");
            return location;
        }
        throw new IllegalStateException("Location not found in SHOW CREATE TABLE result");
    }

    private void cleanExternalTable(String tableName)
            throws Exception
    {
        String tableLocation = getTableLocation(tableName);
        assertUpdate("DROP TABLE " + tableName);
        deleteRecursively(Path.of(new URI(tableLocation).getPath()), ALLOW_INSECURE);
    }
}
