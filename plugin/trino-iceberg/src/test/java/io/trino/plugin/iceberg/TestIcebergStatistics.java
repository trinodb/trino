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
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.MoreCollectors.onlyElement;
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

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testAnalyze(boolean collectOnStatsOnWrites)
    {
        Session writeSession = withStatsOnWrite(getSession(), collectOnStatsOnWrites);
        String tableName = "test_analyze_" + collectOnStatsOnWrites;

        assertUpdate(writeSession, "CREATE TABLE " + tableName + " AS SELECT * FROM tpch.sf1.nation", 25);
        String goodStatsInitial =
                """
                VALUES
                  ('nationkey', null, 25, 0, null, '0', '24'),
                  ('regionkey', null, 5, 0, null, '0', '4'),
                  ('comment', 2162.0, 25, 0, null, null, null),
                  ('name', 583.0, 25, 0, null, null, null),
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
                      ('comment', 2162.0, null, 0, null, null, null),
                      ('name', 583.0, null, 0, null, null, null),
                      (null, null, null, null, 25, null, null)""");
        }

        assertUpdate("ANALYZE " + tableName);
        assertQuery("SHOW STATS FOR " + tableName, goodStatsInitial);

        // reanalyze data
        assertUpdate("ANALYZE " + tableName);
        assertQuery("SHOW STATS FOR " + tableName, goodStatsInitial);

        // insert one more copy; should not influence stats other than rowcount
        assertUpdate(writeSession, "INSERT INTO " + tableName + " SELECT * FROM tpch.sf1.nation", 25);
        String goodStatsAfterFirstInsert =
                """
                VALUES
                  ('nationkey', null, 25, 0, null, '0', '24'),
                  ('regionkey', null, 5, 0, null, '0', '4'),
                  ('comment', 4325.0, 25, 0, null, null, null),
                  ('name', 1166.0, 25, 0, null, null, null),
                  (null, null, null, null, 50, null, null)""";
        assertUpdate("ANALYZE " + tableName);
        assertQuery("SHOW STATS FOR " + tableName, goodStatsAfterFirstInsert);

        // insert modified rows
        assertUpdate(writeSession, "INSERT INTO " + tableName + " SELECT nationkey + 25, reverse(name), regionkey + 5, reverse(comment) FROM tpch.sf1.nation", 25);
        String goodStatsAfterSecondInsert =
                """
                VALUES
                  ('nationkey', null, 50, 0, null, '0', '49'),
                  ('regionkey', null, 10, 0, null, '0', '9'),
                  ('comment', 6463.0, 50, 0, null, null, null),
                  ('name', 1768.0, 50, 0, null, null, null),
                  (null, null, null, null, 75, null, null)
                """;

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
                      ('comment', 6463.0, 25, 0, null, null, null),
                      ('name', 1768.0, 25, 0, null, null, null),
                      (null, null, null, null, 75, null, null)
                    """);
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
        double nameDataSize = (double) computeActual("SHOW STATS FOR " + tableName).getMaterializedRows().stream()
                .filter(row -> "name".equals(row.getField(0)))
                .collect(onlyElement()).getField(1);
        assertThat(nameDataSize).isBetween(1000.0, 3000.0);
        assertQuery(
                "SHOW STATS FOR " + tableName,
                """
                VALUES
                  ('nationkey', null, 25, 0, null, '0', '24'),
                  ('regionkey', null, 5, 0, null, '0', '4'),
                  ('name', %s, 25, 0, null, null, null),
                  ('info', null, null, null, null, null, null),
                  (null, null, null, null, 50, null, null)
                """.formatted(nameDataSize));

        assertUpdate("ANALYZE " + tableName);
        double infoDataSize = (double) computeActual("SHOW STATS FOR " + tableName).getMaterializedRows().stream()
                .filter(row -> "info".equals(row.getField(0)))
                .collect(onlyElement()).getField(1);
        assertThat(infoDataSize).isBetween(2000.0, 4000.0);
        assertQuery(
                "SHOW STATS FOR " + tableName,
                """
                VALUES
                  ('nationkey', null, 25, 0, null, '0', '24'),
                  ('regionkey', null, 5, 0, null, '0', '4'),
                  ('name', %s, 25, 0, null, null, null),
                  ('info', %s, 25, 0.1, null, null, null),
                  (null, null, null, null, 50, null, null)
                """.formatted(nameDataSize, infoDataSize)); // Row count statistics do not yet account for position deletes

        assertUpdate("DROP TABLE " + tableName);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testAnalyzePartitioned(boolean collectOnStatsOnWrites)
    {
        Session writeSession = withStatsOnWrite(getSession(), collectOnStatsOnWrites);
        String tableName = "test_analyze_partitioned_" + collectOnStatsOnWrites;
        assertUpdate(writeSession, "CREATE TABLE " + tableName + " WITH (partitioning = ARRAY['regionkey']) AS SELECT * FROM tpch.sf1.nation", 25);
        String goodStatsInitial =
                """
                VALUES
                  ('nationkey', null, 25, 0, null, '0', '24'),
                  ('regionkey', null, 5, 0, null, '0', '4'),
                  ('comment', 3507.0, 25, 0, null, null, null),
                  ('name', 1182.0, 25, 0, null, null, null),
                  (null, null, null, null, 25, null, null)
                """;

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
                      ('comment', 3507.0, null, 0, null, null, null),
                      ('name', 1182.0, null, 0, null, null, null),
                      (null, null, null, null, 25, null, null)
                    """);
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
                  ('comment', 7014.0, 25, 0, null, null, null),
                  ('name', 2365.0, 25, 0, null, null, null),
                  (null, null, null, null, 50, null, null)
                """);

        // insert modified rows
        assertUpdate(writeSession, "INSERT INTO " + tableName + " SELECT nationkey + 25, reverse(name), regionkey + 5, reverse(comment) FROM tpch.sf1.nation", 25);
        String goodStatsAfterSecondInsert =
                """
                VALUES
                  ('nationkey', null, 50, 0, null, '0', '49'),
                  ('regionkey', null, 10, 0, null, '0', '9'),
                  ('comment', 10493.999999999998, 50, 0, null, null, null),
                  ('name', 3564.0000000000005, 50, 0, null, null, null),
                  (null, null, null, null, 75, null, null)
                """;

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
                      ('comment', 10493.999999999998, 25, 0, null, null, null),
                      ('name', 3564.0000000000005, 25, 0, null, null, null),
                      (null, null, null, null, 75, null, null)
                    """);
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
                  (null, null, null, null, 0, null, null)
                """);

        assertUpdate("ANALYZE " + tableName);
        assertQuery(
                "SHOW STATS FOR " + tableName,
                """
                VALUES
                  ('nationkey', 0, 0, 1, null, null, null),
                  ('regionkey', 0, 0, 1, null, null, null),
                  ('comment', 0, 0, 1, null, null, null),
                  ('name', 0, 0, 1, null, null, null),
                  (null, null, null, null, 0, null, null)
                """);

        // add some data and reanalyze

        assertUpdate(noStatsOnWrite, "INSERT INTO " + tableName + " SELECT * FROM tpch.sf1.nation", 25);

        assertUpdate("ANALYZE " + tableName);
        assertQuery(
                "SHOW STATS FOR " + tableName,
                """
                VALUES
                  ('nationkey', null, 25, 0, null, '0', '24'),
                  ('regionkey', null, 5, 0, null, '0', '4'),
                  ('comment', 2162.0, 25, 0, null, null, null),
                  ('name', 583.0, 25, 0, null, null, null),
                  (null, null, null, null, 25, null, null)
                """);

        assertUpdate("DROP TABLE " + tableName);
    }

    @ParameterizedTest
    @MethodSource("testCollectStatisticsOnWriteDataProvider")
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
                            ('comment', %s, 7, 0, null, null, null),
                            ('name', %s, 7, 0, null, null, null),
                            (null, null, null, null, 7, null, null)
                          """
                        .formatted(partitioned ? "1301.0" : "936.0", partitioned ? "469.0" : "270.0")
                        : """
                          VALUES
                            ('nationkey', null, null, 0, null, '0', '9'),
                            ('regionkey', null, null, 0, null, '0', '2'),
                            ('comment', %s, null, 0, null, null, null),
                            ('name', %s, null, 0, null, null, null),
                            (null, null, null, null, 7, null, null)
                          """
                        .formatted(partitioned ? "1301.0" : "936.0", partitioned ? "469.0" : "270.0"));

        assertUpdate(withStatsOnWrite(getSession(), true), "INSERT INTO " + tableName + " SELECT * FROM tpch.sf1.nation WHERE nationkey >= 12 OR regionkey >= 3", 18);
        assertQuery(
                "SHOW STATS FOR " + tableName,
                collectOnStatsOnCreateTable
                        ? """
                          VALUES
                            ('nationkey', null, 25, 0, null, '0', '24'),
                            ('regionkey', null, 5, 0, null, '0', '4'),
                            ('comment', %s, 25, 0, null, null, null),
                            ('name', %s, 25, 0, null, null, null),
                            (null, null, null, null, 25, null, null)
                          """
                        .formatted(partitioned ? "4058.0" : "2627.0", partitioned ? "1447.0" : "726.0")
                        : """
                          VALUES
                            ('nationkey', null, null, 0, null, '0', '24'),
                            ('regionkey', null, null, 0, null, '0', '4'),
                            ('comment', %s, null, 0, null, null, null),
                            ('name', %s, null, 0, null, null, null),
                            (null, null, null, null, 25, null, null)
                          """
                        .formatted(partitioned ? "4058.0" : "2627.0", partitioned ? "1447.0" : "726.0"));

        assertUpdate("DROP TABLE " + tableName);
    }

    @ParameterizedTest
    @MethodSource("testCollectStatisticsOnWriteDataProvider")
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
                  (null, null, null, null, 0, null, null)
                """);

        assertUpdate(withStatsOnWrite(getSession(), true), "INSERT INTO " + tableName + " TABLE tpch.sf1.nation", 25);
        assertQuery(
                "SHOW STATS FOR " + tableName,
                """
                VALUES
                  ('nationkey', null, 25, 0, null, '0', '24'),
                  ('regionkey', null, 5, 0, null, '0', '4'),
                  ('comment', %f, 25, 0, null, null, null),
                  ('name', %f, 25, 0, null, null, null),
                  (null, null, null, null, 25, null, null)
                """
                        .formatted(partitioned ? 3507.0 : 2162.0, partitioned ? 1182.0 : 583));

        assertUpdate("DROP TABLE " + tableName);
    }

    public Object[][] testCollectStatisticsOnWriteDataProvider()
    {
        return cartesianProduct(trueFalse(), trueFalse());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
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
                  (null, null, null, null, 25, null, null)
                """);

        if (withOptimize) {
            assertUpdate("ALTER TABLE " + tableName + " EXECUTE optimize");
            assertQuery(
                    "SHOW STATS FOR " + tableName,
                    """
                    VALUES
                      ('nationkey', null, 15, 0, null, '0', '24'),
                      ('regionkey', null, 4, 0, null, '0', '3'),
                      (null, null, null, null, 15, null, null)
                    """);
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
                            (null, null, null, null, 15, null, null)
                          """
                        :
                        // TODO row count and min/max values are incorrect as they are taken from manifest file list
                        """
                        VALUES
                          ('nationkey', null, 15, 0, null, '0', '24'),
                          ('regionkey', null, 5, 0, null, '0', '4'), -- not updated yet
                          (null, null, null, null, 25, null, null)
                        """);

        // ANALYZE all columns
        assertUpdate("ANALYZE " + tableName);
        assertQuery(
                "SHOW STATS FOR " + tableName,
                withOptimize
                        ? """
                          VALUES
                            ('nationkey', null, 15, 0, null, '0', '24'),
                            ('regionkey', null, 3, 0, null, '0', '3'),
                            (null, null, null, null, 15, null, null)
                          """
                        :
                        // TODO row count and min/max values are incorrect as they are taken from manifest file list
                        """
                        VALUES
                          ('nationkey', null, 15, 0, null, '0', '24'),
                          ('regionkey', null, 3, 0, null, '0', '4'),
                          (null, null, null, null, 25, null, null)
                        """);

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testAnalyzeSomeColumns()
    {
        String tableName = "test_analyze_some_columns";
        Session noStatsOnWrite = withStatsOnWrite(getSession(), false);
        assertUpdate(noStatsOnWrite, "CREATE TABLE " + tableName + " AS SELECT * FROM tpch.sf1.nation", 25);

        // analyze NULL list of columns
        assertQueryFails("ANALYZE " + tableName + " WITH (columns = NULL)", "\\Qline 1:41: Invalid null value for catalog 'iceberg' analyze property 'columns' from [null]");

        // analyze empty list of columns
        assertQueryFails("ANALYZE " + tableName + " WITH (columns = ARRAY[])", "\\QCannot specify empty list of columns for analysis");

        // specify non-existent column
        assertQueryFails("ANALYZE " + tableName + " WITH (columns = ARRAY['nationkey', 'blah'])", "\\QInvalid columns specified for analysis: [blah]");

        // specify column with wrong case
        assertQueryFails("ANALYZE " + tableName + " WITH (columns = ARRAY['NationKey'])", "\\QInvalid columns specified for analysis: [NationKey]");

        // specify NULL column
        assertQueryFails(
                "ANALYZE " + tableName + " WITH (columns = ARRAY['nationkey', NULL])",
                "\\Qline 1:41: Unable to set catalog 'iceberg' analyze property 'columns' to [ARRAY['nationkey',null]]: Invalid null value in analyze columns property");

        // analyze nationkey and regionkey
        assertUpdate("ANALYZE " + tableName + " WITH (columns = ARRAY['nationkey', 'regionkey'])");
        assertQuery(
                "SHOW STATS FOR " + tableName,
                """
                VALUES
                  ('nationkey', null, 25, 0, null, '0', '24'),
                  ('regionkey', null, 5, 0, null, '0', '4'),
                  ('comment', 2162.0, null, 0, null, null, null),
                  ('name', 583.0, null, 0, null, null, null),
                  (null, null, null, null, 25, null, null)
                """);

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
                  ('comment', 4441.0, null, 0, null, null, null),
                  ('name', 1193.0, null, 0, null, null, null),
                  (null, null, null, null, 50, null, null)
                """);

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
                  ('comment', 4441.0, 50, 0, null, null, null),
                  ('name', 1193.0, 50, 0, null, null, null),
                  (null, null, null, null, 50, null, null)
                """);

        // insert modified rows
        assertUpdate(noStatsOnWrite, "INSERT INTO " + tableName + " SELECT nationkey + 50, concat(name, '2'), regionkey + 10, concat(comment, '22') FROM tpch.sf1.nation", 25);

        // without ANALYZE all stats but NDV should be updated
        assertQuery(
                "SHOW STATS FOR " + tableName,
                """
                VALUES
                  ('nationkey', null, 50, 0, null, '0', '74'),
                  ('regionkey', null, 10, 0, null, '0', '14'),
                  ('comment', 6701.0, 50, 0, null, null, null),
                  ('name', 1803.0, 50, 0, null, null, null),
                  (null, null, null, null, 75, null, null)
                """);

        // reanalyze with a subset of columns
        assertUpdate("ANALYZE " + tableName + " WITH (columns = ARRAY['nationkey', 'regionkey'])");
        assertQuery(
                "SHOW STATS FOR " + tableName,
                """
                VALUES
                  ('nationkey', null, 75, 0, null, '0', '74'),
                  ('regionkey', null, 15, 0, null, '0', '14'),
                  ('comment', 6701.0, 50, 0, null, null, null), -- result of previous analyze
                  ('name', 1803.0, 50, 0, null, null, null), -- result of previous analyze
                  (null, null, null, null, 75, null, null)
                """);

        // analyze all columns
        assertUpdate("ANALYZE " + tableName);
        assertQuery(
                "SHOW STATS FOR " + tableName,
                """
                VALUES
                  ('nationkey', null, 75, 0, null, '0', '74'),
                  ('regionkey', null, 15, 0, null, '0', '14'),
                  ('comment', 6701.0, 75, 0, null, null, null),
                  ('name', 1803.0, 75, 0, null, null, null),
                  (null, null, null, null, 75, null, null)
                """);

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testAnalyzeSnapshot()
    {
        String tableName = "test_analyze_snapshot_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " (a) AS VALUES 11", 1);
        long snapshotId = getCurrentSnapshotId(tableName);
        assertUpdate("INSERT INTO " + tableName + " VALUES 22", 1);
        assertThat(query("ANALYZE \"%s@%d\"".formatted(tableName, snapshotId)))
                .failure().hasMessage(format("line 1:1: Table 'iceberg.tpch.\"%s@%s\"' does not exist", tableName, snapshotId));
        assertThat(query("SELECT * FROM " + tableName))
                .matches("VALUES 11, 22");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testAnalyzeSystemTable()
    {
        assertThat(query("ANALYZE \"nation$files\""))
                // The error message isn't clear to the user, but it doesn't matter
                .nonTrinoExceptionFailure().hasMessage("Cannot record write for catalog not part of transaction");
        assertThat(query("ANALYZE \"nation$snapshots\""))
                // The error message isn't clear to the user, but it doesn't matter
                .nonTrinoExceptionFailure().hasMessage("Cannot record write for catalog not part of transaction");
    }

    @Test
    public void testDropExtendedStats()
    {
        String tableName = "test_drop_extended_stats";
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM tpch.sf1.nation", 25);

        String baseStats =
                """
                VALUES
                  ('nationkey', null, null, 0, null, '0', '24'),
                  ('regionkey', null, null, 0, null, '0', '4'),
                  ('comment', 2162.0, null, 0, null, null, null),
                  ('name',  583.0, null, 0, null, null, null),
                  (null,  null, null, null, 25, null, null)
                """;
        String extendedStats =
                """
                VALUES
                  ('nationkey', null, 25, 0, null, '0', '24'),
                  ('regionkey', null, 5, 0, null, '0', '4'),
                  ('comment', 2162.0, 25, 0, null, null, null),
                  ('name',  583.0, 25, 0, null, null, null),
                  (null,  null, null, null, 25, null, null)
                """;

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
                  ('comment', 2162.0, null, 0, null, null, null),
                  ('name',  583.0, null, 0, null, null, null),
                  (null,  null, null, null, 25, null, null)
                """);

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
        assertThat(query("ALTER TABLE \"%s@%d\" EXECUTE DROP_EXTENDED_STATS".formatted(tableName, snapshotId)))
                .failure().hasMessage(format("line 1:7: Table 'iceberg.tpch.\"%s@%s\"' does not exist", tableName, snapshotId));
        assertThat(query("SELECT * FROM " + tableName))
                .matches("VALUES 11, 22");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testDropStatsSystemTable()
    {
        assertThat(query("ALTER TABLE \"nation$files\" EXECUTE DROP_EXTENDED_STATS"))
                .failure().hasMessage("This connector does not support table procedures");
        assertThat(query("ALTER TABLE \"nation$snapshots\" EXECUTE DROP_EXTENDED_STATS"))
                .failure().hasMessage("This connector does not support table procedures");
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
        assertThat(analyzeSnapshot).isEqualTo(createSnapshot);

        assertUpdate("INSERT INTO " + tableName + " SELECT * FROM tpch.sf1.nation WHERE nationkey = 1", 1);
        assertThat(getCurrentSnapshotId(tableName))
                .isNotEqualTo(createSnapshot);
        // NDV information present after INSERT
        assertQuery(
                "SHOW STATS FOR " + tableName,
                """
                VALUES
                  ('nationkey', null, 25, 0, null, '0', '24'),
                  ('regionkey', null, 5, 0, null, '0', '4'),
                  ('comment', 2448.0, 25, 0, null, null, null),
                  ('name',  704.0, 25, 0, null, null, null),
                  (null,  null, null, null, 26, null, null)
                """);

        assertUpdate(format("ALTER TABLE %s.%s EXECUTE rollback_to_snapshot(%s)", schema, tableName, createSnapshot));
        // NDV information still present after rollback_to_snapshot
        assertQuery(
                "SHOW STATS FOR " + tableName,
                """
                VALUES
                  ('nationkey', null, 25, 0, null, '0', '24'),
                  ('regionkey', null, 5, 0, null, '0', '4'),
                  ('comment', 2162.0, 25, 0, null, null, null),
                  ('name',  583.0, 25, 0, null, null, null),
                  (null,  null, null, null, 25, null, null)
                """);

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
                  ('comment', 2162.0, 25, 0, null, null, null),
                  ('name',  583.0, 25, 0, null, null, null),
                  (null,  null, null, null, 25, null, null)
                """);

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
                  (null,  null, null, null, 0, null, null)
                """);

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
                  (null,  null, null, null, 0, null, null)
                """);

        // write with stats collection
        assertUpdate(
                withStatsOnWrite(getSession(), true),
                "INSERT INTO " + tableName + " VALUES (ROW(52), ROW('hot')), (ROW(53), ROW('dog'))",
                2);
        assertQuery(
                "SHOW STATS FOR " + tableName,
                """
                VALUES
                  ('a', null, null, null, null, null, null),
                  ('b', null, null, null, null, null, null),
                  (null,  null, null, null, 2, null, null)
                """);

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
                  ('a', null, null, null, null, null, null),
                  ('b', null, null, null, null, null, null),
                  (null,  null, null, null, 2, null, null)
                """);

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
                  ('a', null, null, null, null, null, null),
                  ('b', null, null, null, null, null, null),
                  (null,  null, null, null, 2, null, null)
                """);

        // write with stats collection
        assertUpdate(
                withStatsOnWrite(getSession(), true),
                "INSERT INTO " + tableName + " VALUES (ROW(52), ROW('hot')), (ROW(53), ROW('dog'))",
                2);
        assertQuery(
                "SHOW STATS FOR " + tableName,
                """
                VALUES
                  ('a', null, null, null, null, null, null),
                  ('b', null, null, null, null, null, null),
                  (null,  null, null, null, 4, null, null)
                """);

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
                  (null,  null, null, null, 1, null, null)
                """);

        assertQuery(
                "SHOW STATS FOR (SELECT * FROM show_stats_as_of FOR VERSION AS OF " + analyzedSnapshot + ")",
                """
                VALUES
                  ('key', null, 2, 0, null, '3', '4'), -- NDV present, this is the snapshot ANALYZE was run for
                  (null,  null, null, null, 2, null, null)
                """);

        assertQuery(
                "SHOW STATS FOR (SELECT * FROM show_stats_as_of FOR VERSION AS OF " + laterSnapshot + ")",
                """
                VALUES
                  ('key', null, 2, 0, null, '3', '5'), -- NDV present, stats "inherited" from previous snapshot
                  (null,  null, null, null, 3, null, null)
                """);

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
                  (null,  null, null, null, 3, null, null)
                """);

        assertQuery(
                "SHOW STATS FOR (SELECT * FROM show_stats_after_expiration FOR VERSION AS OF " + analyzedSnapshot + ")",
                """
                VALUES
                  ('key', null, 4, 0, null, '1', '4'), -- NDV present, this is the snapshot ANALYZE was run for
                  (null,  null, null, null, 4, null, null)
                """);

        assertQuery(
                "SHOW STATS FOR (SELECT * FROM show_stats_after_expiration FOR VERSION AS OF " + laterSnapshot + ")",
                """
                VALUES
                  ('key', null, 4, 0, null, '1', '5'), -- NDV present, stats "inherited" from previous snapshot
                  (null,  null, null, null, 5, null, null)
                """);

        // Same as laterSnapshot but implicitly
        assertQuery(
                "SHOW STATS FOR show_stats_after_expiration",
                """
                VALUES
                  ('key', null, 4, 0, null, '1', '5'), -- NDV present, stats "inherited" from previous snapshot
                  (null,  null, null, null, 5, null, null)
                """);

        // Re-analyzing after snapshot expired
        assertUpdate("ANALYZE show_stats_after_expiration");

        assertQuery(
                "SHOW STATS FOR show_stats_after_expiration",
                """
                VALUES
                  ('key', null, 5, 0, null, '1', '5'), -- NDV present, stats "inherited" from previous snapshot
                  (null,  null, null, null, 5, null, null)
                """);

        assertUpdate("DROP TABLE show_stats_after_expiration");
    }

    @Test
    public void testShowStatsAfterOptimize()
    {
        String tableName = "show_stats_after_optimize_" + randomNameSuffix();

        String catalog = getSession().getCatalog().orElseThrow();
        Session writeSession = withStatsOnWrite(getSession(), false);
        Session minimalSnapshotRetentionSession = Session.builder(getSession())
                .setCatalogSessionProperty(catalog, EXPIRE_SNAPSHOTS_MIN_RETENTION, "0s")
                .build();

        String expireSnapshotQuery = "ALTER TABLE " + tableName + " EXECUTE expire_snapshots(retention_threshold => '0d')";

        assertUpdate(writeSession, "CREATE TABLE " + tableName + "(key integer)");
        // create several snapshots
        assertUpdate(writeSession, "INSERT INTO " + tableName + " VALUES 1", 1);
        assertUpdate(writeSession, "INSERT INTO " + tableName + " VALUES 2", 1);
        assertUpdate(writeSession, "INSERT INTO " + tableName + " VALUES 3", 1);

        assertUpdate("ANALYZE " + tableName);
        assertUpdate(writeSession, "INSERT INTO " + tableName + " VALUES 4", 1);

        assertQuery(
                "SHOW STATS FOR " + tableName,
                """
                VALUES
                  ('key', null, 3, 0, null, '1', '4'), -- NDV present, stats "inherited" from previous snapshot
                  (null,  null, null, null, 4, null, null)
                """);

        assertUpdate(minimalSnapshotRetentionSession, expireSnapshotQuery);

        // NDV is not present after expire_snapshot as last snapshot did not contained stats
        assertQuery(
                "SHOW STATS FOR " + tableName,
                """
                VALUES
                  ('key', null, null, 0, null, '1', '4'), -- NDV not present as expire_snapshot removed stats for previous snapshots
                  (null,  null, null, null, 4, null, null)
                """);

        assertUpdate("ANALYZE " + tableName);

        assertQuery(
                "SHOW STATS FOR " + tableName,
                """
                VALUES
                  ('key', null, 4, 0, null, '1', '4'), -- NDV present
                  (null,  null, null, null, 4, null, null)
                """);

        // Optimize should rewrite stats file
        assertUpdate("ALTER TABLE " + tableName + " EXECUTE optimize");
        assertUpdate(minimalSnapshotRetentionSession, expireSnapshotQuery);

        assertQuery(
                "SHOW STATS FOR " + tableName,
                """
                VALUES
                  ('key', null, 4, 0, null, '1', '4'), -- NDV present
                  (null,  null, null, null, 4, null, null)
                """);

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testStatsAfterDeletingAllRows()
    {
        String tableName = "test_stats_after_deleting_all_rows_";
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM tpch.sf1.nation", 25);

        assertThat(query("SHOW STATS FOR " + tableName))
                .result()
                .projected("column_name", "distinct_values_count", "row_count")
                .skippingTypesCheck()
                .containsAll("VALUES " +
                        "('nationkey', DOUBLE '25', null), " +
                        "('name', DOUBLE '25', null), " +
                        "('regionkey', DOUBLE '5', null), " +
                        "('comment', DOUBLE '25', null), " +
                        "(null, null, DOUBLE '25')");
        assertUpdate("DELETE FROM " + tableName + " WHERE nationkey < 50", 25);
        assertThat(query("SHOW STATS FOR " + tableName))
                .result()
                .projected("column_name", "distinct_values_count", "row_count")
                .skippingTypesCheck()
                .containsAll("VALUES " +
                        "('nationkey', DOUBLE '25', null), " +
                        "('name', DOUBLE '25', null), " +
                        "('regionkey', DOUBLE '5', null), " +
                        "('comment', DOUBLE '25', null), " +
                        "(null, null, DOUBLE '25')");
    }

    @Test
    public void testNaN()
    {
        String tableName = "test_nan";
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 1 AS c1, double 'NaN' AS c2", 1);
        assertQuery(
                "SHOW STATS FOR " + tableName,
                """
                VALUES
                  ('c1', null, 1.0, 0.0, null, 1, 1),
                  ('c2', null, 1.0, 0.0, null, null, null),
                  (null, null, null, null, 1.0, null, null)
                """);
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
