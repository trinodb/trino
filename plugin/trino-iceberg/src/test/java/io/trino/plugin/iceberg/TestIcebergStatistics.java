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
package io.trino.plugin.iceberg;

import com.google.common.collect.Lists;
import com.google.common.math.IntMath;
import io.trino.Session;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DataProviders;
import io.trino.testing.QueryRunner;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.iceberg.IcebergSessionProperties.COLLECT_EXTENDED_STATISTICS_ON_WRITE;
import static io.trino.plugin.iceberg.IcebergSessionProperties.EXPIRE_SNAPSHOTS_MIN_RETENTION;
import static io.trino.testing.DataProviders.cartesianProduct;
import static io.trino.testing.DataProviders.trueFalse;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.EXECUTE_TABLE_PROCEDURE;
import static io.trino.testing.TestingAccessControlManager.privilege;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tpch.TpchTable.NATION;
import static java.lang.String.format;
import static java.math.RoundingMode.UP;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

public class TestIcebergStatistics
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .setInitialTables(NATION)
                .build();
    }

    @Test(dataProviderClass = DataProviders.class, dataProvider = "trueFalse")
    public void testAnalyze(boolean collectOnStatsOnWrites)
    {
        Session writeSession = withStatsOnWrite(getSession(), collectOnStatsOnWrites);
        String tableName = "test_analyze_" + collectOnStatsOnWrites;

        assertUpdate(writeSession, "CREATE TABLE " + tableName + " AS SELECT * FROM tpch.sf1.nation", 25);
        String goodStatsInitial = """
                VALUES
                  ('nationkey', null, 25, 0, null, '0', '24'),
                  ('regionkey', null, 5, 0, null, '0', '4'),
                  ('comment', null, 25, 0, null, null, null),
                  ('name', null, 25, 0, null, null, null),
                  (null, null, null, null, 25, null, null)""";

        if (collectOnStatsOnWrites) {
            assertQuery("SHOW STATS FOR " + tableName, goodStatsInitial);
        }
        else {
            assertQuery(
                    "SHOW STATS FOR " + tableName,
                    """
                            VALUES
                              ('nationkey', null, null, 0, null, '0', '24'),
                              ('regionkey', null, null, 0, null, '0', '4'),
                              ('comment', null, null, 0, null, null, null),
                              ('name', null, null, 0, null, null, null),
                              (null, null, null, null, 25, null, null)""");
        }

        assertUpdate("ANALYZE " + tableName);
        assertQuery("SHOW STATS FOR " + tableName, goodStatsInitial);

        // reanalyze data
        assertUpdate("ANALYZE " + tableName);
        assertQuery("SHOW STATS FOR " + tableName, goodStatsInitial);

        // insert one more copy; should not influence stats other than rowcount
        assertUpdate(writeSession, "INSERT INTO " + tableName + " SELECT * FROM tpch.sf1.nation", 25);
        String goodStatsAfterFirstInsert = """
                VALUES
                  ('nationkey', null, 25, 0, null, '0', '24'),
                  ('regionkey', null, 5, 0, null, '0', '4'),
                  ('comment', null, 25, 0, null, null, null),
                  ('name', null, 25, 0, null, null, null),
                  (null, null, null, null, 50, null, null)""";
        assertUpdate("ANALYZE " + tableName);
        assertQuery("SHOW STATS FOR " + tableName, goodStatsAfterFirstInsert);

        // insert modified rows
        assertUpdate(writeSession, "INSERT INTO " + tableName + " SELECT nationkey + 25, reverse(name), regionkey + 5, reverse(comment) FROM tpch.sf1.nation", 25);
        String goodStatsAfterSecondInsert = """
                VALUES
                  ('nationkey', null, 50, 0, null, '0', '49'),
                  ('regionkey', null, 10, 0, null, '0', '9'),
                  ('comment', null, 50, 0, null, null, null),
                  ('name', null, 50, 0, null, null, null),
                  (null, null, null, null, 75, null, null)""";

        if (collectOnStatsOnWrites) {
            assertQuery("SHOW STATS FOR " + tableName, goodStatsAfterSecondInsert);
        }
        else {
            // without ANALYZE all stats but NDV should be updated
            assertQuery(
                    "SHOW STATS FOR " + tableName,
                    """
                            VALUES
                              ('nationkey', null, 25, 0, null, '0', '49'),
                              ('regionkey', null, 5, 0, null, '0', '9'),
                              ('comment', null, 25, 0, null, null, null),
                              ('name', null, 25, 0, null, null, null),
                              (null, null, null, null, 75, null, null)""");
        }

        // with analyze we should get new NDV
        assertUpdate("ANALYZE " + tableName);
        assertQuery("SHOW STATS FOR " + tableName, goodStatsAfterSecondInsert);

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testAnalyzeWithSchemaEvolution()
    {
        String tableName = "test_analyze_with_schema_evolution";
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM tpch.sf1.nation", 25);

        assertUpdate("ANALYZE " + tableName);

        assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN info varchar");
        assertUpdate("UPDATE " + tableName + " SET info = format('%s %s', name, comment)", 25);
        assertUpdate("ALTER TABLE " + tableName + " DROP COLUMN comment");

        // schema changed, ANALYZE hasn't been re-run yet
        assertQuery(
                "SHOW STATS FOR " + tableName,
                """
                        VALUES
                          ('nationkey', null, 25, 0, null, '0', '24'),
                          ('regionkey', null, 5, 0, null, '0', '4'),
                          ('name', null, 25, 0, null, null, null),
                          ('info', null, null, 0, null, null, null),
                          (null, null, null, null, 25, null, null)""");

        assertUpdate("ANALYZE " + tableName);
        assertQuery(
                "SHOW STATS FOR " + tableName,
                """
                        VALUES
                          ('nationkey', null, 25, 0, null, '0', '24'),
                          ('regionkey', null, 5, 0, null, '0', '4'),
                          ('name', null, 25, 0, null, null, null),
                          ('info', null, 25, 0, null, null, null),
                          (null, null, null, null, 25, null, null)""");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test(dataProviderClass = DataProviders.class, dataProvider = "trueFalse")
    public void testAnalyzePartitioned(boolean collectOnStatsOnWrites)
    {
        Session writeSession = withStatsOnWrite(getSession(), collectOnStatsOnWrites);
        String tableName = "test_analyze_partitioned_" + collectOnStatsOnWrites;
        assertUpdate(writeSession, "CREATE TABLE " + tableName + " WITH (partitioning = ARRAY['regionkey']) AS SELECT * FROM tpch.sf1.nation", 25);
        String goodStatsInitial = """
                VALUES
                  ('nationkey', null, 25, 0, null, '0', '24'),
                  ('regionkey', null, 5, 0, null, '0', '4'),
                  ('comment', null, 25, 0, null, null, null),
                  ('name', null, 25, 0, null, null, null),
                  (null, null, null, null, 25, null, null)""";

        if (collectOnStatsOnWrites) {
            assertQuery("SHOW STATS FOR " + tableName, goodStatsInitial);
        }
        else {
            assertQuery(
                    "SHOW STATS FOR " + tableName,
                    """
                            VALUES
                              ('nationkey', null, null, 0, null, '0', '24'),
                              ('regionkey', null, null, 0, null, '0', '4'),
                              ('comment', null, null, 0, null, null, null),
                              ('name', null, null, 0, null, null, null),
                              (null, null, null, null, 25, null, null)""");
        }

        assertUpdate("ANALYZE " + tableName);
        assertQuery("SHOW STATS FOR " + tableName, goodStatsInitial);

        // insert one more copy; should not influence stats other than rowcount
        assertUpdate(writeSession, "INSERT INTO " + tableName + " SELECT * FROM tpch.sf1.nation", 25);

        assertUpdate("ANALYZE " + tableName);
        assertQuery(
                "SHOW STATS FOR " + tableName,
                """
                        VALUES
                          ('nationkey', null, 25, 0, null, '0', '24'),
                          ('regionkey', null, 5, 0, null, '0', '4'),
                          ('comment', null, 25, 0, null, null, null),
                          ('name', null, 25, 0, null, null, null),
                          (null, null, null, null, 50, null, null)""");

        // insert modified rows
        assertUpdate(writeSession, "INSERT INTO " + tableName + " SELECT nationkey + 25, reverse(name), regionkey + 5, reverse(comment) FROM tpch.sf1.nation", 25);
        String goodStatsAfterSecondInsert = """
                VALUES
                  ('nationkey', null, 50, 0, null, '0', '49'),
                  ('regionkey', null, 10, 0, null, '0', '9'),
                  ('comment', null, 50, 0, null, null, null),
                  ('name', null, 50, 0, null, null, null),
                  (null, null, null, null, 75, null, null)""";

        if (collectOnStatsOnWrites) {
            assertQuery("SHOW STATS FOR " + tableName, goodStatsAfterSecondInsert);
        }
        else {
            // without ANALYZE all stats but NDV should be updated
            assertQuery(
                    "SHOW STATS FOR " + tableName,
                    """
                            VALUES
                              ('nationkey', null, 25, 0, null, '0', '49'),
                              ('regionkey', null, 5, 0, null, '0', '9'),
                              ('comment', null, 25, 0, null, null, null),
                              ('name', null, 25, 0, null, null, null),
                              (null, null, null, null, 75, null, null)""");
        }

        // with analyze we should get new NDV
        assertUpdate("ANALYZE " + tableName);
        assertQuery("SHOW STATS FOR " + tableName, goodStatsAfterSecondInsert);

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testAnalyzeEmpty()
    {
        String tableName = "test_analyze_empty";
        Session noStatsOnWrite = withStatsOnWrite(getSession(), false);

        assertUpdate(noStatsOnWrite, "CREATE TABLE " + tableName + " AS SELECT * FROM tpch.sf1.nation WITH NO DATA", 0);

        assertQuery(
                "SHOW STATS FOR " + tableName,
                """
                        VALUES
                          ('nationkey', 0, 0, 1, null, null, null),
                          ('regionkey', 0, 0, 1, null, null, null),
                          ('comment', 0, 0, 1, null, null, null),
                          ('name', 0, 0, 1, null, null, null),
                          (null, null, null, null, 0, null, null)""");

        assertUpdate("ANALYZE " + tableName);
        assertQuery(
                "SHOW STATS FOR " + tableName,
                """
                        VALUES
                          ('nationkey', 0, 0, 1, null, null, null),
                          ('regionkey', 0, 0, 1, null, null, null),
                          ('comment', 0, 0, 1, null, null, null),
                          ('name', 0, 0, 1, null, null, null),
                          (null, null, null, null, 0, null, null)""");

        // add some data and reanalyze

        assertUpdate(noStatsOnWrite, "INSERT INTO " + tableName + " SELECT * FROM tpch.sf1.nation", 25);

        assertUpdate("ANALYZE " + tableName);
        assertQuery(
                "SHOW STATS FOR " + tableName,
                """
                        VALUES
                          ('nationkey', null, 25, 0, null, '0', '24'),
                          ('regionkey', null, 5, 0, null, '0', '4'),
                          ('comment', null, 25, 0, null, null, null),
                          ('name', null, 25, 0, null, null, null),
                          (null, null, null, null, 25, null, null)""");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test(dataProvider = "testCollectStatisticsOnWriteDataProvider")
    public void testCollectStatisticsOnWrite(boolean collectOnStatsOnCreateTable, boolean partitioned)
    {
        String tableName = "test_collect_stats_insert_" + collectOnStatsOnCreateTable + partitioned;

        assertUpdate(
                withStatsOnWrite(getSession(), collectOnStatsOnCreateTable),
                "CREATE TABLE " + tableName + " " +
                        (partitioned ? "WITH (partitioning=ARRAY['regionkey']) " : "") +
                        "AS SELECT * FROM tpch.sf1.nation WHERE nationkey < 12 AND regionkey < 3",
                7);
        assertQuery(
                "SHOW STATS FOR " + tableName,
                collectOnStatsOnCreateTable
                        ? """
                        VALUES
                          ('nationkey', null, 7, 0, null, '0', '9'),
                          ('regionkey', null, 3, 0, null, '0', '2'),
                          ('comment', null, 7, 0, null, null, null),
                          ('name', null, 7, 0, null, null, null),
                          (null, null, null, null, 7, null, null)"""
                        : """
                        VALUES
                          ('nationkey', null, null, 0, null, '0', '9'),
                          ('regionkey', null, null, 0, null, '0', '2'),
                          ('comment', null, null, 0, null, null, null),
                          ('name', null, null, 0, null, null, null),
                          (null, null, null, null, 7, null, null)""");

        assertUpdate(withStatsOnWrite(getSession(), true), "INSERT INTO " + tableName + " SELECT * FROM tpch.sf1.nation WHERE nationkey >= 12 OR regionkey >= 3", 18);
        assertQuery(
                "SHOW STATS FOR " + tableName,
                collectOnStatsOnCreateTable
                        ? """
                        VALUES
                          ('nationkey', null, 25, 0, null, '0', '24'),
                          ('regionkey', null, 5, 0, null, '0', '4'),
                          ('comment', null, 25, 0, null, null, null),
                          ('name', null, 25, 0, null, null, null),
                          (null, null, null, null, 25, null, null)"""
                        : """
                        VALUES
                          ('nationkey', null, null, 0, null, '0', '24'),
                          ('regionkey', null, null, 0, null, '0', '4'),
                          ('comment', null, null, 0, null, null, null),
                          ('name', null, null, 0, null, null, null),
                          (null, null, null, null, 25, null, null)""");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test(dataProvider = "testCollectStatisticsOnWriteDataProvider")
    public void testCollectStatisticsOnWriteToEmptyTable(boolean collectOnStatsOnCreateTable, boolean partitioned)
    {
        String tableName = "test_collect_stats_insert_into_empty_" + collectOnStatsOnCreateTable + partitioned;

        assertUpdate(
                withStatsOnWrite(getSession(), collectOnStatsOnCreateTable),
                "CREATE TABLE " + tableName + " " +
                        (partitioned ? "WITH (partitioning=ARRAY['regionkey']) " : "") +
                        "AS TABLE tpch.sf1.nation WITH NO DATA",
                0);
        assertQuery(
                "SHOW STATS FOR " + tableName,
                """
                        VALUES
                          ('nationkey', 0, 0, 1, null, null, null),
                          ('regionkey', 0, 0, 1, null, null, null),
                          ('comment', 0, 0, 1, null, null, null),
                          ('name', 0, 0, 1, null, null, null),
                          (null, null, null, null, 0, null, null)""");

        assertUpdate(withStatsOnWrite(getSession(), true), "INSERT INTO " + tableName + " TABLE tpch.sf1.nation", 25);
        assertQuery(
                "SHOW STATS FOR " + tableName,
                """
                        VALUES
                          ('nationkey', null, 25, 0, null, '0', '24'),
                          ('regionkey', null, 5, 0, null, '0', '4'),
                          ('comment', null, 25, 0, null, null, null),
                          ('name', null, 25, 0, null, null, null),
                          (null, null, null, null, 25, null, null)""");

        assertUpdate("DROP TABLE " + tableName);
    }

    @DataProvider
    public Object[][] testCollectStatisticsOnWriteDataProvider()
    {
        return cartesianProduct(trueFalse(), trueFalse());
    }

    @Test(dataProviderClass = DataProviders.class, dataProvider = "trueFalse")
    public void testAnalyzeAfterStatsDrift(boolean withOptimize)
    {
        String tableName = "test_analyze_stats_drift_" + withOptimize;
        Session session = withStatsOnWrite(getSession(), true);

        assertUpdate(session, "CREATE TABLE " + tableName + " AS SELECT nationkey, regionkey FROM tpch.sf1.nation", 25);
        assertQuery(
                "SHOW STATS FOR " + tableName,
                """
                        VALUES
                          ('nationkey', null, 25, 0, null, '0', '24'),
                          ('regionkey', null, 5, 0, null, '0', '4'),
                          (null, null, null, null, 25, null, null)""");

        // remove two regions in multiple queries
        List<String> idsToRemove = computeActual("SELECT nationkey FROM tpch.sf1.nation WHERE regionkey IN (2, 4)").getOnlyColumn()
                .map(value -> Long.toString((Long) value))
                .collect(toImmutableList());
        for (List<String> ids : Lists.partition(idsToRemove, IntMath.divide(idsToRemove.size(), 2, UP))) {
            String idsLiteral = ids.stream().collect(joining(", ", "(", ")"));
            assertUpdate("DELETE FROM " + tableName + " WHERE nationkey IN " + idsLiteral, ids.size());
        }

        // Stats not updated during deletes
        assertQuery(
                "SHOW STATS FOR " + tableName,
                """
                        VALUES
                          ('nationkey', null, 25, 0, null, '0', '24'),
                          ('regionkey', null, 5, 0, null, '0', '4'),
                          (null, null, null, null, 25, null, null)""");

        if (withOptimize) {
            assertUpdate("ALTER TABLE " + tableName + " EXECUTE optimize");
            assertQuery(
                    "SHOW STATS FOR " + tableName,
                    """
                            VALUES
                              ('nationkey', null, 15, 0, null, '0', '24'),
                              ('regionkey', null, 4, 0, null, '0', '3'),
                              (null, null, null, null, 15, null, null)""");
        }

        // ANALYZE can be used to update stats and prevent them from drifting over time
        assertUpdate("ANALYZE " + tableName + " WITH(columns=ARRAY['nationkey'])");
        assertQuery(
                "SHOW STATS FOR " + tableName,
                withOptimize
                        ? """
                        VALUES
                          ('nationkey', null, 15, 0, null, '0', '24'),
                          ('regionkey', null, 4, 0, null, '0', '3'), -- not updated yet
                          (null, null, null, null, 15, null, null)"""
                        :
                        // TODO row count and min/max values are incorrect as they are taken from manifest file list
                        """
                                VALUES
                                  ('nationkey', null, 15, 0, null, '0', '24'),
                                  ('regionkey', null, 5, 0, null, '0', '4'), -- not updated yet
                                  (null, null, null, null, 25, null, null)""");

        // ANALYZE all columns
        assertUpdate("ANALYZE " + tableName);
        assertQuery(
                "SHOW STATS FOR " + tableName,
                withOptimize
                        ? """
                        VALUES
                          ('nationkey', null, 15, 0, null, '0', '24'),
                          ('regionkey', null, 3, 0, null, '0', '3'),
                          (null, null, null, null, 15, null, null)"""
                        :
                        // TODO row count and min/max values are incorrect as they are taken from manifest file list
                        """
                                VALUES
                                  ('nationkey', null, 15, 0, null, '0', '24'),
                                  ('regionkey', null, 3, 0, null, '0', '4'),
                                  (null, null, null, null, 25, null, null)""");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testAnalyzeSomeColumns()
    {
        String tableName = "test_analyze_some_columns";
        Session noStatsOnWrite = withStatsOnWrite(getSession(), false);
        assertUpdate(noStatsOnWrite, "CREATE TABLE " + tableName + " AS SELECT * FROM tpch.sf1.nation", 25);

        // analyze NULL list of columns
        assertQueryFails("ANALYZE " + tableName + " WITH (columns = NULL)", "\\QInvalid null value for catalog 'iceberg' analyze property 'columns' from [null]");

        // analyze empty list of columns
        assertQueryFails("ANALYZE " + tableName + " WITH (columns = ARRAY[])", "\\QCannot specify empty list of columns for analysis");

        // specify non-existent column
        assertQueryFails("ANALYZE " + tableName + " WITH (columns = ARRAY['nationkey', 'blah'])", "\\QInvalid columns specified for analysis: [blah]");

        // specify column with wrong case
        assertQueryFails("ANALYZE " + tableName + " WITH (columns = ARRAY['NationKey'])", "\\QInvalid columns specified for analysis: [NationKey]");

        // specify NULL column
        assertQueryFails(
                "ANALYZE " + tableName + " WITH (columns = ARRAY['nationkey', NULL])",
                "\\QUnable to set catalog 'iceberg' analyze property 'columns' to [ARRAY['nationkey',null]]: Invalid null value in analyze columns property");

        // analyze nationkey and regionkey
        assertUpdate("ANALYZE " + tableName + " WITH (columns = ARRAY['nationkey', 'regionkey'])");
        assertQuery(
                "SHOW STATS FOR " + tableName,
                """
                        VALUES
                          ('nationkey', null, 25, 0, null, '0', '24'),
                          ('regionkey', null, 5, 0, null, '0', '4'),
                          ('comment', null, null, 0, null, null, null),
                          ('name', null, null, 0, null, null, null),
                          (null, null, null, null, 25, null, null)""");

        // insert modified rows
        assertUpdate(noStatsOnWrite, "INSERT INTO " + tableName + " SELECT nationkey + 25, concat(name, '1'), regionkey + 5, concat(comment, '21') FROM tpch.sf1.nation", 25);

        // perform one more analyze for nationkey and regionkey
        assertUpdate("ANALYZE " + tableName + " WITH (columns = ARRAY['nationkey', 'regionkey'])");
        assertQuery(
                "SHOW STATS FOR " + tableName,
                """
                        VALUES
                          ('nationkey', null, 50, 0, null, '0', '49'),
                          ('regionkey', null, 10, 0, null, '0', '9'),
                          ('comment', null, null, 0, null, null, null),
                          ('name', null, null, 0, null, null, null),
                          (null, null, null, null, 50, null, null)""");

        // drop stats
        assertUpdate("ALTER TABLE " + tableName + " EXECUTE DROP_EXTENDED_STATS");

        // analyze all columns
        assertUpdate("ANALYZE " + tableName);
        assertQuery(
                "SHOW STATS FOR " + tableName,
                """
                        VALUES
                          ('nationkey', null, 50, 0, null, '0', '49'),
                          ('regionkey', null, 10, 0, null, '0', '9'),
                          ('comment', null, 50, 0, null, null, null),
                          ('name', null, 50, 0, null, null, null),
                          (null, null, null, null, 50, null, null)""");

        // insert modified rows
        assertUpdate(noStatsOnWrite, "INSERT INTO " + tableName + " SELECT nationkey + 50, concat(name, '2'), regionkey + 10, concat(comment, '22') FROM tpch.sf1.nation", 25);

        // without ANALYZE all stats but NDV should be updated
        assertQuery(
                "SHOW STATS FOR " + tableName,
                """
                        VALUES
                          ('nationkey', null, 50, 0, null, '0', '74'),
                          ('regionkey', null, 10, 0, null, '0', '14'),
                          ('comment', null, 50, 0, null, null, null),
                          ('name', null, 50, 0, null, null, null),
                          (null, null, null, null, 75, null, null)""");

        // reanalyze with a subset of columns
        assertUpdate("ANALYZE " + tableName + " WITH (columns = ARRAY['nationkey', 'regionkey'])");
        assertQuery(
                "SHOW STATS FOR " + tableName,
                """
                        VALUES
                          ('nationkey', null, 75, 0, null, '0', '74'),
                          ('regionkey', null, 15, 0, null, '0', '14'),
                          ('comment', null, 50, 0, null, null, null), -- result of previous analyze
                          ('name', null, 50, 0, null, null, null), -- result of previous analyze
                          (null, null, null, null, 75, null, null)""");

        // analyze all columns
        assertUpdate("ANALYZE " + tableName);
        assertQuery(
                "SHOW STATS FOR " + tableName,
                """
                        VALUES
                          ('nationkey', null, 75, 0, null, '0', '74'),
                          ('regionkey', null, 15, 0, null, '0', '14'),
                          ('comment', null, 75, 0, null, null, null),
                          ('name', null, 75, 0, null, null, null),
                          (null, null, null, null, 75, null, null)""");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testAnalyzeSnapshot()
    {
        String tableName = "test_analyze_snapshot_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " (a) AS VALUES 11", 1);
        long snapshotId = getCurrentSnapshotId(tableName);
        assertUpdate("INSERT INTO " + tableName + " VALUES 22", 1);
        assertThatThrownBy(() -> query("ANALYZE \"%s@%d\"".formatted(tableName, snapshotId)))
                .hasMessage(format("Invalid Iceberg table name: %s@%d", tableName, snapshotId));
        assertThat(query("SELECT * FROM " + tableName))
                .matches("VALUES 11, 22");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testAnalyzeSystemTable()
    {
        assertThatThrownBy(() -> query("ANALYZE \"nation$files\""))
                // The error message isn't clear to the user, but it doesn't matter
                .hasMessage("Cannot record write for catalog not part of transaction");
        assertThatThrownBy(() -> query("ANALYZE \"nation$snapshots\""))
                // The error message isn't clear to the user, but it doesn't matter
                .hasMessage("Cannot record write for catalog not part of transaction");
    }

    @Test
    public void testDropExtendedStats()
    {
        String tableName = "test_drop_extended_stats";
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM tpch.sf1.nation", 25);

        String baseStats = """
                VALUES
                  ('nationkey', null, null, 0, null, '0', '24'),
                  ('regionkey', null, null, 0, null, '0', '4'),
                  ('comment', null, null, 0, null, null, null),
                  ('name',  null, null, 0, null, null, null),
                  (null,  null, null, null, 25, null, null)""";
        String extendedStats = """
                VALUES
                  ('nationkey', null, 25, 0, null, '0', '24'),
                  ('regionkey', null, 5, 0, null, '0', '4'),
                  ('comment', null, 25, 0, null, null, null),
                  ('name',  null, 25, 0, null, null, null),
                  (null,  null, null, null, 25, null, null)""";

        assertQuery("SHOW STATS FOR " + tableName, extendedStats);

        // Dropping extended stats clears distinct count and leaves other stats alone
        assertUpdate("ALTER TABLE " + tableName + " EXECUTE DROP_EXTENDED_STATS");
        assertQuery("SHOW STATS FOR " + tableName, baseStats);

        // Re-analyzing should work
        assertUpdate("ANALYZE " + tableName);
        assertQuery("SHOW STATS FOR " + tableName, extendedStats);

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testDropMissingStats()
    {
        String tableName = "test_drop_missing_stats";
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM tpch.sf1.nation", 25);

        // When there are no extended stats, the procedure should have no effect
        assertUpdate("ALTER TABLE " + tableName + " EXECUTE DROP_EXTENDED_STATS");
        assertQuery(
                "SHOW STATS FOR " + tableName,
                """
                        VALUES
                          ('nationkey', null, null, 0, null, '0', '24'),
                          ('regionkey', null, null, 0, null, '0', '4'),
                          ('comment', null, null, 0, null, null, null),
                          ('name',  null, null, 0, null, null, null),
                          (null,  null, null, null, 25, null, null)""");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testDropStatsAccessControl()
    {
        String catalog = getSession().getCatalog().orElseThrow();
        String schema = getSession().getSchema().orElseThrow();
        String tableName = "test_deny_drop_stats";
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM tpch.sf1.nation", 25);

        assertAccessDenied(
                "ALTER TABLE " + tableName + " EXECUTE DROP_EXTENDED_STATS",
                "Cannot execute table procedure DROP_EXTENDED_STATS on iceberg.tpch.test_deny_drop_stats",
                privilege(format("%s.%s.%s.DROP_EXTENDED_STATS", catalog, schema, tableName), EXECUTE_TABLE_PROCEDURE));

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testDropStatsSnapshot()
    {
        String tableName = "test_drop_stats_snapshot_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " (a) AS VALUES 11", 1);
        long snapshotId = getCurrentSnapshotId(tableName);
        assertUpdate("INSERT INTO " + tableName + " VALUES 22", 1);
        assertThatThrownBy(() -> query("ALTER TABLE \"%s@%d\" EXECUTE DROP_EXTENDED_STATS".formatted(tableName, snapshotId)))
                .hasMessage(format("Invalid Iceberg table name: %s@%d", tableName, snapshotId));
        assertThat(query("SELECT * FROM " + tableName))
                .matches("VALUES 11, 22");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testDropStatsSystemTable()
    {
        assertThatThrownBy(() -> query("ALTER TABLE \"nation$files\" EXECUTE DROP_EXTENDED_STATS"))
                .hasMessage("This connector does not support table procedures");
        assertThatThrownBy(() -> query("ALTER TABLE \"nation$snapshots\" EXECUTE DROP_EXTENDED_STATS"))
                .hasMessage("This connector does not support table procedures");
    }

    @Test
    public void testAnalyzeAndRollbackToSnapshot()
    {
        String schema = getSession().getSchema().orElseThrow();
        String tableName = "test_analyze_and_rollback";
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM tpch.sf1.nation", 25);
        long createSnapshot = getCurrentSnapshotId(tableName);
        assertUpdate("ANALYZE " + tableName);
        long analyzeSnapshot = getCurrentSnapshotId(tableName);
        // ANALYZE currently does not create a new snapshot
        assertEquals(analyzeSnapshot, createSnapshot);

        assertUpdate("INSERT INTO " + tableName + " SELECT * FROM tpch.sf1.nation WHERE nationkey = 1", 1);
        assertNotEquals(getCurrentSnapshotId(tableName), createSnapshot);
        // NDV information present after INSERT
        assertQuery(
                "SHOW STATS FOR " + tableName,
                """
                        VALUES
                          ('nationkey', null, 25, 0, null, '0', '24'),
                          ('regionkey', null, 5, 0, null, '0', '4'),
                          ('comment', null, 25, 0, null, null, null),
                          ('name',  null, 25, 0, null, null, null),
                          (null,  null, null, null, 26, null, null)""");

        assertUpdate(format("CALL system.rollback_to_snapshot('%s', '%s', %s)", schema, tableName, createSnapshot));
        // NDV information still present after rollback_to_snapshot
        assertQuery(
                "SHOW STATS FOR " + tableName,
                """
                        VALUES
                          ('nationkey', null, 25, 0, null, '0', '24'),
                          ('regionkey', null, 5, 0, null, '0', '4'),
                          ('comment', null, 25, 0, null, null, null),
                          ('name',  null, 25, 0, null, null, null),
                          (null,  null, null, null, 25, null, null)""");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testAnalyzeAndDeleteOrphanFiles()
    {
        String tableName = "test_analyze_and_delete_orphan_files";
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM tpch.sf1.nation", 25);
        assertUpdate("ANALYZE " + tableName);

        assertQuerySucceeds(
                Session.builder(getSession())
                        .setCatalogSessionProperty("iceberg", "remove_orphan_files_min_retention", "0s")
                        .build(),
                "ALTER TABLE " + tableName + " EXECUTE REMOVE_ORPHAN_FILES (retention_threshold => '0s')");
        // NDV information still present
        assertQuery(
                "SHOW STATS FOR " + tableName,
                """
                        VALUES
                          ('nationkey', null, 25, 0, null, '0', '24'),
                          ('regionkey', null, 5, 0, null, '0', '4'),
                          ('comment', null, 25, 0, null, null, null),
                          ('name',  null, 25, 0, null, null, null),
                          (null,  null, null, null, 25, null, null)""");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testEmptyNoScalarColumns()
    {
        // Currently, only scalar columns can be analyzed
        String tableName = "empty_table_without_scalar_columns";

        assertUpdate("CREATE TABLE " + tableName + " (a row(x integer), b row(y varchar))");
        assertQuery(
                "SHOW STATS FOR " + tableName,
                """
                        VALUES
                          ('a', 0, 0, 1, null, null, null),
                          ('b', 0, 0, 1, null, null, null),
                          (null,  null, null, null, 0, null, null)""");

        // On empty table
        assertQueryFails("ANALYZE " + tableName + " WITH (columns = ARRAY[])", "Cannot specify empty list of columns for analysis");
        assertQueryFails("ANALYZE " + tableName + " WITH (columns = ARRAY['a'])", "Invalid columns specified for analysis: \\[a]");
        assertQueryFails("ANALYZE " + tableName + " WITH (columns = ARRAY['a.x'])", "Invalid columns specified for analysis: \\[a.x]");
        assertQueryFails("ANALYZE " + tableName + " WITH (columns = ARRAY['b'])", "Invalid columns specified for analysis: \\[b]");
        assertUpdate("ANALYZE " + tableName);
        assertQuery(
                "SHOW STATS FOR " + tableName,
                """
                        VALUES
                          ('a', 0, 0, 1, null, null, null),
                          ('b', 0, 0, 1, null, null, null),
                          (null,  null, null, null, 0, null, null)""");

        // write with stats collection
        assertUpdate(
                withStatsOnWrite(getSession(), true),
                "INSERT INTO " + tableName + " VALUES (ROW(52), ROW('hot')), (ROW(53), ROW('dog'))",
                2);
        assertQuery(
                "SHOW STATS FOR " + tableName,
                """
                        VALUES
                          ('a', null, null, 0, null, null, null),
                          ('b', null, null, 0, null, null, null),
                          (null,  null, null, null, 2, null, null)""");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testNoScalarColumns()
    {
        // Currently, only scalar columns can be analyzed
        String tableName = "table_without_scalar_columns";

        assertUpdate("CREATE TABLE " + tableName + " (a row(x integer), b row(y varchar))");
        assertUpdate(
                withStatsOnWrite(getSession(), false),
                "INSERT INTO " + tableName + " VALUES (ROW(42), ROW('ala')), (ROW(43), ROW('has a cat'))",
                2);
        assertQuery(
                "SHOW STATS FOR " + tableName,
                """
                        VALUES
                          ('a', null, null, 0, null, null, null),
                          ('b', null, null, 0, null, null, null),
                          (null,  null, null, null, 2, null, null)""");

        // On non-empty table
        assertQueryFails("ANALYZE " + tableName + " WITH (columns = ARRAY[])", "Cannot specify empty list of columns for analysis");
        assertQueryFails("ANALYZE " + tableName + " WITH (columns = ARRAY['a'])", "Invalid columns specified for analysis: \\[a]");
        assertQueryFails("ANALYZE " + tableName + " WITH (columns = ARRAY['a.x'])", "Invalid columns specified for analysis: \\[a.x]");
        assertQueryFails("ANALYZE " + tableName + " WITH (columns = ARRAY['b'])", "Invalid columns specified for analysis: \\[b]");
        assertUpdate("ANALYZE " + tableName);
        assertQuery(
                "SHOW STATS FOR " + tableName,
                """
                        VALUES
                          ('a', null, null, 0, null, null, null),
                          ('b', null, null, 0, null, null, null),
                          (null,  null, null, null, 2, null, null)""");

        // write with stats collection
        assertUpdate(
                withStatsOnWrite(getSession(), true),
                "INSERT INTO " + tableName + " VALUES (ROW(52), ROW('hot')), (ROW(53), ROW('dog'))",
                2);
        assertQuery(
                "SHOW STATS FOR " + tableName,
                """
                        VALUES
                          ('a', null, null, 0, null, null, null),
                          ('b', null, null, 0, null, null, null),
                          (null,  null, null, null, 4, null, null)""");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testShowStatsAsOf()
    {
        Session writeSession = withStatsOnWrite(getSession(), false);
        assertUpdate(writeSession, "CREATE TABLE show_stats_as_of(key integer)");

        assertUpdate(writeSession, "INSERT INTO show_stats_as_of VALUES 3", 1);
        long beforeAnalyzedSnapshot = getCurrentSnapshotId("show_stats_as_of");

        assertUpdate(writeSession, "INSERT INTO show_stats_as_of VALUES 4", 1);
        assertUpdate("ANALYZE show_stats_as_of");
        long analyzedSnapshot = getCurrentSnapshotId("show_stats_as_of");

        assertUpdate(writeSession, "INSERT INTO show_stats_as_of VALUES 5", 1);
        long laterSnapshot = getCurrentSnapshotId("show_stats_as_of");

        assertQuery(
                "SHOW STATS FOR (SELECT * FROM show_stats_as_of FOR VERSION AS OF " + beforeAnalyzedSnapshot + ")",
                """
                        VALUES
                          ('key', null, null, 0, null, '3', '3'), -- NDV not present, as ANALYZE was run on a later snapshot
                          (null,  null, null, null, 1, null, null)""");

        assertQuery(
                "SHOW STATS FOR (SELECT * FROM show_stats_as_of FOR VERSION AS OF " + analyzedSnapshot + ")",
                """
                        VALUES
                          ('key', null, 2, 0, null, '3', '4'), -- NDV present, this is the snapshot ANALYZE was run for
                          (null,  null, null, null, 2, null, null)""");

        assertQuery(
                "SHOW STATS FOR (SELECT * FROM show_stats_as_of FOR VERSION AS OF " + laterSnapshot + ")",
                """
                        VALUES
                          ('key', null, 2, 0, null, '3', '5'), -- NDV present, stats "inherited" from previous snapshot
                          (null,  null, null, null, 3, null, null)""");

        assertUpdate("DROP TABLE show_stats_as_of");
    }

    @Test
    public void testShowStatsAfterExpiration()
    {
        String catalog = getSession().getCatalog().orElseThrow();
        Session writeSession = withStatsOnWrite(getSession(), false);

        assertUpdate(writeSession, "CREATE TABLE show_stats_after_expiration(key integer)");
        // create several snapshots
        assertUpdate(writeSession, "INSERT INTO show_stats_after_expiration VALUES 1", 1);
        assertUpdate(writeSession, "INSERT INTO show_stats_after_expiration VALUES 2", 1);
        assertUpdate(writeSession, "INSERT INTO show_stats_after_expiration VALUES 3", 1);

        long beforeAnalyzedSnapshot = getCurrentSnapshotId("show_stats_after_expiration");

        assertUpdate(
                Session.builder(getSession())
                        .setCatalogSessionProperty(catalog, EXPIRE_SNAPSHOTS_MIN_RETENTION, "0s")
                        .build(),
                "ALTER TABLE show_stats_after_expiration EXECUTE expire_snapshots(retention_threshold => '0d')");
        assertThat(query("SELECT count(*) FROM \"show_stats_after_expiration$snapshots\""))
                .matches("VALUES BIGINT '1'");

        assertUpdate(writeSession, "INSERT INTO show_stats_after_expiration VALUES 4", 1);
        assertUpdate("ANALYZE show_stats_after_expiration");
        long analyzedSnapshot = getCurrentSnapshotId("show_stats_after_expiration");

        assertUpdate(writeSession, "INSERT INTO show_stats_after_expiration VALUES 5", 1);
        long laterSnapshot = getCurrentSnapshotId("show_stats_after_expiration");

        assertQuery(
                "SHOW STATS FOR (SELECT * FROM show_stats_after_expiration FOR VERSION AS OF " + beforeAnalyzedSnapshot + ")",
                """
                        VALUES
                          ('key', null, null, 0, null, '1', '3'), -- NDV not present, as ANALYZE was run on a later snapshot
                          (null,  null, null, null, 3, null, null)""");

        assertQuery(
                "SHOW STATS FOR (SELECT * FROM show_stats_after_expiration FOR VERSION AS OF " + analyzedSnapshot + ")",
                """
                        VALUES
                          ('key', null, 4, 0, null, '1', '4'), -- NDV present, this is the snapshot ANALYZE was run for
                          (null,  null, null, null, 4, null, null)""");

        assertQuery(
                "SHOW STATS FOR (SELECT * FROM show_stats_after_expiration FOR VERSION AS OF " + laterSnapshot + ")",
                """
                        VALUES
                          ('key', null, 4, 0, null, '1', '5'), -- NDV present, stats "inherited" from previous snapshot
                          (null,  null, null, null, 5, null, null)""");

        // Same as laterSnapshot but implicitly
        assertQuery(
                "SHOW STATS FOR show_stats_after_expiration",
                """
                        VALUES
                          ('key', null, 4, 0, null, '1', '5'), -- NDV present, stats "inherited" from previous snapshot
                          (null,  null, null, null, 5, null, null)""");

        // Re-analyzing after snapshot expired
        assertUpdate("ANALYZE show_stats_after_expiration");

        assertQuery(
                "SHOW STATS FOR show_stats_after_expiration",
                """
                        VALUES
                          ('key', null, 5, 0, null, '1', '5'), -- NDV present, stats "inherited" from previous snapshot
                          (null,  null, null, null, 5, null, null)""");

        assertUpdate("DROP TABLE show_stats_after_expiration");
    }

    private long getCurrentSnapshotId(String tableName)
    {
        return (long) computeActual(format("SELECT snapshot_id FROM \"%s$snapshots\" ORDER BY committed_at DESC FETCH FIRST 1 ROW WITH TIES", tableName))
                .getOnlyValue();
    }

    private static Session withStatsOnWrite(Session session, boolean enabled)
    {
        String catalog = session.getCatalog().orElseThrow();
        return Session.builder(session)
                .setCatalogSessionProperty(catalog, COLLECT_EXTENDED_STATISTICS_ON_WRITE, Boolean.toString(enabled))
                .build();
    }
}
