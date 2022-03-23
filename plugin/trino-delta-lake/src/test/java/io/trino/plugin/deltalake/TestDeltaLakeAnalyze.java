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
import io.trino.operator.OperatorStats;
import io.trino.plugin.deltalake.util.DockerizedMinioDataLake;
import io.trino.spi.QueryId;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.ResultWithQueryId;
import io.trino.testing.sql.TestTable;
import org.testng.annotations.Test;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Optional;

import static com.google.common.collect.MoreCollectors.onlyElement;
import static io.trino.plugin.deltalake.DeltaLakeDockerizedMinioDataLake.createDockerizedMinioDataLakeForDeltaLake;
import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.DELTA_CATALOG;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.INSERT_TABLE;
import static io.trino.testing.TestingAccessControlManager.privilege;
import static io.trino.testing.sql.TestTable.randomTableSuffix;
import static java.lang.String.format;
import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.Assertions.assertThat;

// TODO (https://github.com/trinodb/trino/issues/11325): hadoop container sometimes fails during the test (e.g. testAnalyzeWithFilesModifiedAfter)
//   enable the class by making it non-abstract
// smoke test which covers ANALYZE compatibility with different filesystems is part of AbstractTestDeltaLakeIntegrationSmokeTest
public abstract class TestDeltaLakeAnalyze
        extends AbstractTestQueryFramework
{
    private static final String SCHEMA = "default";
    private final String bucketName;

    public TestDeltaLakeAnalyze()
    {
        this.bucketName = "test-delta-lake-analyze-" + randomTableSuffix();
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DockerizedMinioDataLake dockerizedMinioDataLake = closeAfterClass(createDockerizedMinioDataLakeForDeltaLake(bucketName));

        return DeltaLakeQueryRunner.createS3DeltaLakeQueryRunner(
                DELTA_CATALOG,
                SCHEMA,
                ImmutableMap.of("delta.enable-non-concurrent-writes", "true"),
                dockerizedMinioDataLake.getMinioAddress(),
                dockerizedMinioDataLake.getTestingHadoop());
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
        String tableName = "test_analyze_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName
                + " WITH ("
                + "location = '" + getLocationForTable(tableName) + "'"
                + (checkpointInterval.isPresent() ? format(", checkpoint_interval = %s", checkpointInterval.get()) : "")
                + ")"
                + " AS SELECT * FROM tpch.sf1.nation", 25);

        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', null, null, 0.0, null, 0, 24)," +
                        "('regionkey', null, null, 0.0, null, 0, 4)," +
                        "('comment', null, null, 0.0, null, null, null)," +
                        "('name', null, null, 0.0, null, null, null)," +
                        "(null, null, null, null, 25.0, null, null)");

        runAnalyzeVerifySplitCount(tableName, 1);

        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', null, 25.0, 0.0, null, 0, 24)," +
                        "('regionkey', null, 5.0, 0.0, null, 0, 4)," +
                        "('comment', null, 25.0, 0.0, null, null, null)," +
                        "('name', null, 25.0, 0.0, null, null, null)," +
                        "(null, null, null, null, 25.0, null, null)");

        // reanalyze data (1 split is empty values)
        runAnalyzeVerifySplitCount(tableName, 1);

        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', null, 25.0, 0.0, null, 0, 24)," +
                        "('regionkey', null, 5.0, 0.0, null, 0, 4)," +
                        "('comment', null, 25.0, 0.0, null, null, null)," +
                        "('name', null, 25.0, 0.0, null, null, null)," +
                        "(null, null, null, null, 25.0, null, null)");

        // insert one more copy; should not influence stats other than rowcount
        assertUpdate("INSERT INTO " + tableName + " SELECT * FROM tpch.sf1.nation", 25);

        runAnalyzeVerifySplitCount(tableName, 1);

        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', null, 25.0, 0.0, null, 0, 24)," +
                        "('regionkey', null, 5.0, 0.0, null, 0, 4)," +
                        "('comment', null, 25.0, 0.0, null, null, null)," +
                        "('name', null, 25.0, 0.0, null, null, null)," +
                        "(null, null, null, null, 50.0, null, null)");

        // insert modified rows
        assertUpdate("INSERT INTO " + tableName + " SELECT nationkey + 25, reverse(name), regionkey + 5, reverse(comment) FROM tpch.sf1.nation", 25);

        // without ANALYZE all stats but NDV should be updated
        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', null, 25.0, 0.0, null, 0, 49)," +
                        "('regionkey', null, 5.0, 0.0, null, 0, 9)," +
                        "('comment', null, 25.0, 0.0, null, null, null)," +
                        "('name', null, 25.0, 0.0, null, null, null)," +
                        "(null, null, null, null, 75.0, null, null)");

        // with analyze we should get new NDV
        runAnalyzeVerifySplitCount(tableName, 1);

        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', null, 50.0, 0.0, null, 0, 49)," +
                        "('regionkey', null, 10.0, 0.0, null, 0, 9)," +
                        "('comment', null, 50.0, 0.0, null, null, null)," +
                        "('name', null, 50.0, 0.0, null, null, null)," +
                        "(null, null, null, null, 75.0, null, null)");
    }

    @Test
    public void testAnalyzePartitioned()
    {
        String tableName = "test_analyze_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName
                + " WITH ("
                + "   location = '" + getLocationForTable(tableName) + "',"
                + "   partitioned_by = ARRAY['regionkey']"
                + ")"
                + " AS SELECT * FROM tpch.sf1.nation", 25);

        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', null, null, 0.0, null, 0, 24)," +
                        "('regionkey', null, 5.0, 0.0, null, null, null)," +
                        "('comment', null, null, 0.0, null, null, null)," +
                        "('name', null, null, 0.0, null, null, null)," +
                        "(null, null, null, null, 25.0, null, null)");

        runAnalyzeVerifySplitCount(tableName, 5);

        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', null, 25.0, 0.0, null, 0, 24)," +
                        "('regionkey', null, 5.0, 0.0, null, null, null)," +
                        "('comment', null, 25.0, 0.0, null, null, null)," +
                        "('name', null, 25.0, 0.0, null, null, null)," +
                        "(null, null, null, null, 25.0, null, null)");

        // insert one more copy; should not influence stats other than rowcount
        assertUpdate("INSERT INTO " + tableName + " SELECT * FROM tpch.sf1.nation", 25);

        runAnalyzeVerifySplitCount(tableName, 5);

        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', null, 25.0, 0.0, null, 0, 24)," +
                        "('regionkey', null, 5.0, 0.0, null, null, null)," +
                        "('comment', null, 25.0, 0.0, null, null, null)," +
                        "('name', null, 25.0, 0.0, null, null, null)," +
                        "(null, null, null, null, 50.0, null, null)");

        // insert modified rows
        assertUpdate("INSERT INTO " + tableName + " SELECT nationkey + 25, reverse(name), regionkey + 5, reverse(comment) FROM tpch.sf1.nation", 25);

        // without ANALYZE all stats but NDV should be updated
        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', null, 25.0, 0.0, null, 0, 49)," +
                        "('regionkey', null, 10.0, 0.0, null, null, null)," +
                        "('comment', null, 25.0, 0.0, null, null, null)," +
                        "('name', null, 25.0, 0.0, null, null, null)," +
                        "(null, null, null, null, 75.0, null, null)");

        // with analyze we should get new NDV
        runAnalyzeVerifySplitCount(tableName, 5);
        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', null, 50.0, 0.0, null, 0, 49)," +
                        "('regionkey', null, 10.0, 0.0, null, null, null)," +
                        "('comment', null, 50.0, 0.0, null, null, null)," +
                        "('name', null, 50.0, 0.0, null, null, null)," +
                        "(null, null, null, null, 75.0, null, null)");
    }

    @Test
    public void testAnalyzeEmpty()
    {
        String tableName = "test_analyze_empty_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName
                + " WITH ("
                + "location = '" + getLocationForTable(tableName) + "'"
                + ")"
                + " AS SELECT * FROM tpch.sf1.nation WHERE false", 0);

        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', 0.0, 0.0, 1.0, null, null, null)," +
                        "('regionkey', 0.0, 0.0, 1.0, null, null, null)," +
                        "('comment', 0.0, 0.0, 1.0, null, null, null)," +
                        "('name', 0.0, 0.0, 1.0, null, null, null)," +
                        "(null, null, null, null, 0, null, null)");

        runAnalyzeVerifySplitCount(tableName, 1);

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

        runAnalyzeVerifySplitCount(tableName, 1);

        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', null, 25.0, 0.0, null, 0, 24)," +
                        "('regionkey', null, 5.0, 0.0, null, 0, 4)," +
                        "('comment', null, 25.0, 0.0, null, null, null)," +
                        "('name', null, 25.0, 0.0, null, null, null)," +
                        "(null, null, null, null, 25.0, null, null)");
    }

    @Test
    public void testAnalyzeExtendedStatisticsDisabled()
    {
        String tableName = "test_analyze_extended_stats_disabled" + randomTableSuffix();

        assertUpdate("CREATE TABLE " + tableName
                + " WITH ("
                + "location = '" + getLocationForTable(tableName) + "'"
                + ")"
                + " AS SELECT * FROM tpch.sf1.nation", 25);

        Session extendedStatisticsDisabled = Session.builder(getSession())
                .setCatalogSessionProperty(getSession().getCatalog().orElseThrow(), "extended_statistics_enabled", "false")
                .build();

        assertQueryFails(
                extendedStatisticsDisabled,
                "ANALYZE " + tableName,
                "ANALYZE not supported if extended statistics are disabled. Enable via delta.extended-statistics.enabled config property or extended_statistics_enabled session property.");
    }

    @Test
    public void testAnalyzeWithFilesModifiedAfter()
            throws InterruptedException
    {
        String tableName = "test_analyze_" + randomTableSuffix();

        assertUpdate("CREATE TABLE " + tableName
                + " WITH ("
                + "location = '" + getLocationForTable(tableName) + "'"
                + ")"
                + " AS SELECT * FROM tpch.sf1.nation", 25);

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
                        "('comment', null, 5.0, 0.0, null, null, null)," +
                        "('name', null, 5.0, 0.0, null, null, null)," +
                        "(null, null, null, null, 30.0, null, null)");
    }

    @Test
    public void testAnalyzeSomeColumns()
    {
        String tableName = "test_analyze_some_columns" + randomTableSuffix();

        assertUpdate("CREATE TABLE " + tableName
                + " WITH ("
                + "location = '" + getLocationForTable(tableName) + "'"
                + ")"
                + " AS SELECT * FROM tpch.sf1.nation", 25);

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
                "List of columns to be analyzed must be a subset of previously used. To extend list of analyzed columns drop table statistics");

        // we should not be able to analyze for all columns
        assertQueryFails("ANALYZE " + tableName,
                "List of columns to be analyzed must be a subset of previously used. To extend list of analyzed columns drop table statistics");

        // insert modified rows
        assertUpdate("INSERT INTO " + tableName + " SELECT nationkey + 25, concat(name, '1'), regionkey + 5, concat(comment, '21') FROM tpch.sf1.nation", 25);

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
        assertUpdate(format("CALL %s.system.drop_extended_stats('%s', '%s')", DELTA_CATALOG, SCHEMA, tableName));

        // now we should be able to analyze all columns
        assertUpdate(format("ANALYZE %s", tableName));
        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', null, 50.0, 0.0, null, 0, 49)," +
                        "('regionkey', null, 10.0, 0.0, null, 0, 9)," +
                        "('comment', null, 50.0, 0.0, null, null, null)," +
                        "('name', null, 50.0, 0.0, null, null, null)," +
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
    }

    @Test
    public void testDropExtendedStats()
    {
        String path = "test_drop_extended_stats_" + randomTableSuffix();

        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_drop_extended_stats",
                format("WITH (location = '%s') AS SELECT * FROM tpch.sf1.nation", getLocationForTable(path)))) {
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
                    + "('comment',   null, 25.0,  0.0, null, null, null),"
                    + "('name',      null, 25.0,  0.0, null, null, null),"
                    + "(null,        null, null, null, 25.0, null, null)";

            assertQuery(query, baseStats);

            // Update stats to include distinct count
            runAnalyzeVerifySplitCount(table.getName(), 1);
            assertQuery(query, extendedStats);

            // Dropping extended stats clears distinct count and leaves other stats alone
            assertUpdate(format("CALL %s.system.drop_extended_stats('%s', '%s')", DELTA_CATALOG, SCHEMA, table.getName()));
            assertQuery(query, baseStats);

            // Re-analyzing should work
            runAnalyzeVerifySplitCount(table.getName(), 1);
            assertQuery(query, extendedStats);
        }
    }

    @Test
    public void testDropMissingStats()
    {
        String path = "test_drop_missing_stats_" + randomTableSuffix();

        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_drop_missing_stats",
                format("WITH (location = '%s') AS SELECT * FROM tpch.sf1.nation", getLocationForTable(path)))) {
            // When there are no extended stats, the procedure should have no effect
            assertUpdate(format("CALL %s.system.drop_extended_stats('%s', '%s')", DELTA_CATALOG, SCHEMA, table.getName()));
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
        String path = "test_deny_drop_stats_" + randomTableSuffix();

        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_deny_drop_stats",
                format("WITH (location = '%s') AS SELECT * FROM tpch.sf1.nation", getLocationForTable(path)))) {
            assertAccessDenied(
                    format("CALL %s.system.drop_extended_stats('%s', '%s')", DELTA_CATALOG, SCHEMA, table.getName()),
                    "Cannot insert into table .*",
                    privilege(table.getName(), INSERT_TABLE));
        }
    }

    private void runAnalyzeVerifySplitCount(String tableName, long expectedSplitCount)
    {
        ResultWithQueryId<MaterializedResult> analyzeResult = getDistributedQueryRunner().executeWithQueryId(getSession(), "ANALYZE " + tableName);
        verifySplitCount(analyzeResult.getQueryId(), expectedSplitCount);
    }

    private void verifySplitCount(QueryId queryId, long expectedCount)
    {
        OperatorStats operatorStats = getOperatorStats(queryId);
        assertThat(operatorStats.getTotalDrivers()).isEqualTo(expectedCount);
    }

    private OperatorStats getOperatorStats(QueryId queryId)
    {
        return getDistributedQueryRunner().getCoordinator()
                .getQueryManager()
                .getFullQueryInfo(queryId)
                .getQueryStats()
                .getOperatorSummaries()
                .stream()
                .filter(summary -> summary.getOperatorType().contains("Scan"))
                .collect(onlyElement());
    }

    private String getLocationForTable(String tableName)
    {
        return format("s3://%s/%s", bucketName, tableName);
    }
}
