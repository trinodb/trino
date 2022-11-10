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

import com.google.common.collect.ImmutableList;
import io.trino.testing.BaseConnectorSmokeTest;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.TestTable;
import org.apache.iceberg.FileFormat;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertTrue;

public abstract class BaseIcebergConnectorSmokeTest
        extends BaseConnectorSmokeTest
{
    protected final FileFormat format;

    public BaseIcebergConnectorSmokeTest(FileFormat format)
    {
        this.format = requireNonNull(format, "format is null");
    }

    @SuppressWarnings("DuplicateBranchesInSwitch")
    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_TOPN_PUSHDOWN:
                return false;

            case SUPPORTS_CREATE_VIEW:
                return true;

            case SUPPORTS_CREATE_MATERIALIZED_VIEW:
                return true;

            case SUPPORTS_DELETE:
            case SUPPORTS_UPDATE:
            case SUPPORTS_MERGE:
                return true;

            default:
                return super.hasBehavior(connectorBehavior);
        }
    }

    @Test
    @Override
    public void testShowCreateTable()
    {
        String schemaName = getSession().getSchema().orElseThrow();
        assertThat((String) computeScalar("SHOW CREATE TABLE region"))
                .matches("" +
                        "CREATE TABLE iceberg." + schemaName + ".region \\(\n" +
                        "   regionkey bigint,\n" +
                        "   name varchar,\n" +
                        "   comment varchar\n" +
                        "\\)\n" +
                        "WITH \\(\n" +
                        "   format = '" + format.name() + "',\n" +
                        "   format_version = 2,\n" +
                        format("   location = '.*/" + schemaName + "/region.*'\n") +
                        "\\)");
    }

    @Test
    public void testHiddenPathColumn()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "hidden_file_path", "(a int, b VARCHAR)", ImmutableList.of("(1, 'a')"))) {
            String filePath = (String) computeScalar(format("SELECT file_path FROM \"%s$files\"", table.getName()));

            assertQuery("SELECT DISTINCT \"$path\" FROM " + table.getName(), "VALUES " + "'" + filePath + "'");

            // Check whether the "$path" hidden column is correctly evaluated in the filter expression
            assertQuery(format("SELECT a FROM %s WHERE \"$path\" = '%s'", table.getName(), filePath), "VALUES 1");
        }
    }

    // Repeat test with invocationCount for better test coverage, since the tested aspect is inherently non-deterministic.
    @Test(timeOut = 120_000, invocationCount = 4)
    public void testDeleteRowsConcurrently()
            throws Exception
    {
        int threads = 4;
        CyclicBarrier barrier = new CyclicBarrier(threads);
        ExecutorService executor = newFixedThreadPool(threads);
        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_concurrent_delete",
                "(col0 INTEGER, col1 INTEGER, col2 INTEGER, col3 INTEGER)")) {
            String tableName = table.getName();
            assertUpdate("INSERT INTO " + tableName + " VALUES (0, 0, 0, 0)", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 0, 0, 0)", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (0, 1, 0, 0)", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (0, 0, 1, 0)", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (0, 0, 0, 1)", 1);

            List<Future<Boolean>> futures = IntStream.range(0, threads)
                    .mapToObj(threadNumber -> executor.submit(() -> {
                        barrier.await(10, SECONDS);
                        try {
                            String columnName = "col" + threadNumber;
                            getQueryRunner().execute(format("DELETE FROM %s WHERE %s = 1", tableName, columnName));
                            return true;
                        }
                        catch (Exception e) {
                            return false;
                        }
                    }))
                    .collect(toImmutableList());

            futures.forEach(future -> assertTrue(getFutureValue(future)));
            assertThat(query("SELECT max(col0), max(col1), max(col2), max(col3) FROM " + tableName)).matches("VALUES (0, 0, 0, 0)");
        }
        finally {
            executor.shutdownNow();
            executor.awaitTermination(10, SECONDS);
        }
    }
}
