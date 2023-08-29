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

import com.google.common.collect.ContiguousSet;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import io.trino.plugin.hive.containers.HiveMinioDataLake;
import io.trino.spi.QueryId;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedResultWithQueryId;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import org.testng.annotations.Test;
import org.testng.asserts.SoftAssert;

import java.nio.file.Path;
import java.util.OptionalLong;
import java.util.Set;

import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.DELTA_CATALOG;
import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.createS3DeltaLakeQueryRunner;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

public class TestPredicatePushdown
        extends AbstractTestQueryFramework
{
    private static final Path RESOURCE_PATH = Path.of("databricks73/pushdown/");
    private static final String TEST_SCHEMA = "default";

    private final String bucketName = "delta-test-pushdown-" + randomNameSuffix();
    /**
     * This single-file Parquet table has known row groups. See the test
     * resource {@code pushdown/custkey_15rowgroups/README.md} for details.
     */
    private final TableResource testTable = new TableResource("custkey_15rowgroups");

    private HiveMinioDataLake hiveMinioDataLake;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        hiveMinioDataLake = closeAfterClass(new HiveMinioDataLake(bucketName));
        hiveMinioDataLake.start();
        return createS3DeltaLakeQueryRunner(
                DELTA_CATALOG,
                TEST_SCHEMA,
                ImmutableMap.of(
                        "delta.enable-non-concurrent-writes", "true",
                        "delta.register-table-procedure.enabled", "true"),
                hiveMinioDataLake.getMinio().getMinioAddress(),
                hiveMinioDataLake.getHiveHadoop());
    }

    @Test
    public void testSelectPushdown()
    {
        String table = testTable.register("select_pushdown");

        assertPushdown(
                format("SELECT custkey FROM %s WHERE custkey > 1495", table),
                "SELECT * FROM UNNEST(ARRAY[1496, 1497, 1498, 1499, 1500])",
                100);

        // 7 row groups include 500 in their range
        assertPushdown(
                format("SELECT custkey FROM %s WHERE custkey = 500", table),
                "SELECT 500",
                700);
    }

    @Test
    public void testDeletePushdown()
    {
        String table;

        table = testTable.register("delete_pushdown");
        // Only 5 row groups have data above 1300, so pushdown to Parquet
        // should ensure only 500 rows are read.
        assertPushdownUpdate(
                format("DELETE FROM %s WHERE custkey > 1300", table),
                200,
                500);
        // Check that the correct data was deleted
        assertEquals(
                execute(format("SELECT custkey FROM %s", table)).getOnlyColumnAsSet(),
                ContiguousSet.closed(1L, 1300L));

        table = testTable.register("delete_pushdown_disjoint");
        // 11 groups have data outside of (500, 1100]
        assertPushdownUpdate(
                format("DELETE FROM %s WHERE custkey <= 500 OR custkey > 1100", table),
                900,
                1100);
        assertEquals(
                execute(format("SELECT custkey FROM %s", table)).getOnlyColumnAsSet(),
                ContiguousSet.closed(501L, 1100L));
    }

    @Test
    public void testUpdatePushdown()
    {
        String table;

        table = testTable.register("update_pushdown_simple");
        // 7 row groups include 500 in their range
        assertPushdownUpdate(
                format("UPDATE %s SET phone = 'phone number' WHERE custkey = 500", table),
                1,
                700);
        assertQuery(format("SELECT phone FROM %s WHERE custkey = 500", table), "VALUES 'phone number'");

        table = testTable.register("update_pushdown_range");
        // 9 groups have data on (1000, 1200]
        assertPushdownUpdate(
                format("UPDATE %s SET mktsegment = phone WHERE 1000 < custkey AND custkey <= 1200", table),
                200,
                900);
        assertQueryReturnsEmptyResult(format(
                "SELECT * FROM %s WHERE mktsegment = phone AND NOT (1000 < custkey AND custkey <= 1200)",
                table));
    }

    /**
     * Assert on the number of rows read and updated by a read operation
     *
     * @param actual The query to test
     * @param expected The expected results as an SQL expression
     * @param countProcessed The number of rows expected to be processed
     */
    private void assertPushdown(String actual, String expected, long countProcessed)
    {
        MaterializedResultWithQueryId result = executeWithQueryId(actual);
        Set<MaterializedRow> actualRows = Set.copyOf(result.getResult().getMaterializedRows());
        Set<MaterializedRow> expectedRows = Set.copyOf(
                computeExpected(expected, result.getResult().getTypes()).getMaterializedRows());

        SoftAssert softly = new SoftAssert();
        softly.assertTrue(
                result.getResult().getUpdateType().isEmpty(),
                "Query should not have update type");
        softly.assertEqualsNoOrder(
                actualRows.toArray(),
                expectedRows.toArray(),
                format(
                        "Wrong query results:\n"
                                + "\t\tmissing rows: %s\n"
                                + "\t\textra rows: %s",
                        Sets.difference(expectedRows, actualRows),
                        Sets.difference(actualRows, expectedRows)));
        softly.assertEquals(
                getProcessedPositions(result.getQueryId()),
                countProcessed,
                "Wrong number of rows processed after pushdown to Parquet");
        softly.assertAll();
    }

    /**
     * Assert on the number of rows read and updated by a write operation.
     *
     * @param sql The query to test
     * @param count The number of rows expected to be modified (query result)
     * @param countProcessed The number of rows expected to be processed
     */
    private void assertPushdownUpdate(String sql, long count, long countProcessed)
    {
        MaterializedResultWithQueryId result = executeWithQueryId(sql);
        OptionalLong actualCount = result.getResult().getUpdateCount();

        SoftAssert softly = new SoftAssert();
        softly.assertTrue(actualCount.isPresent(), "Missing update count");
        softly.assertEquals(actualCount.getAsLong(), count, "Wrong number of rows updated");
        softly.assertEquals(
                getProcessedPositions(result.getQueryId()),
                countProcessed,
                "Wrong amount of data filtered by pushdown to Parquet");
        softly.assertAll();
    }

    private MaterializedResultWithQueryId executeWithQueryId(String sql)
    {
        return getDistributedQueryRunner().executeWithQueryId(getSession(), sql);
    }

    private MaterializedResult execute(String sql)
    {
        return getQueryRunner().execute(sql);
    }

    private long getProcessedPositions(QueryId query)
    {
        return getDistributedQueryRunner().getCoordinator().getQueryManager()
                .getFullQueryInfo(query).getQueryStats().getProcessedInputPositions();
    }

    /**
     * Represents a table stored as data in the test resources.
     */
    private class TableResource
    {
        private final String resourcePath;

        private TableResource(String resourcePath)
        {
            this.resourcePath = resourcePath;
        }

        /**
         * Register a table in the metastore using the described resource and the given name prefix.
         *
         * @return The name of the registered table.
         */
        String register(String namePrefix)
        {
            String name = format("%s_%s", namePrefix, randomNameSuffix());
            hiveMinioDataLake.copyResources(RESOURCE_PATH.resolve(resourcePath).toString(), name);
            getQueryRunner().execute(format(
                    "CALL system.register_table(CURRENT_SCHEMA, '%2$s', 's3://%1$s/%2$s')",
                    bucketName,
                    name));
            return name;
        }
    }
}
