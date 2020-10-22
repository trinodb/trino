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
package io.prestosql.plugin.bigquery;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.common.collect.ImmutableMap;
import io.prestosql.testing.AbstractTestIntegrationSmokeTest;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.testing.QueryRunner;
import org.testng.annotations.Test;

import static io.prestosql.plugin.bigquery.BigQueryQueryRunner.createBigQueryClient;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.testing.MaterializedResult.resultBuilder;
import static io.prestosql.testing.assertions.Assert.assertEquals;
import static java.lang.String.format;

@Test
public class TestBigQueryIntegrationSmokeTest
        extends AbstractTestIntegrationSmokeTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return BigQueryQueryRunner.createQueryRunner(ImmutableMap.of());
    }

    @Override
    public void testDescribeTable()
    {
        MaterializedResult expectedColumns = resultBuilder(getQueryRunner().getDefaultSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("orderkey", "bigint", "", "")
                .row("custkey", "bigint", "", "")
                .row("orderstatus", "varchar", "", "")
                .row("totalprice", "double", "", "")
                .row("orderdate", "date", "", "")
                .row("orderpriority", "varchar", "", "")
                .row("clerk", "varchar", "", "")
                .row("shippriority", "bigint", "", "")
                .row("comment", "varchar", "", "")
                .build();
        MaterializedResult actualColumns = computeActual("DESCRIBE orders");
        assertEquals(actualColumns, expectedColumns);
    }

    @Test(enabled = false)
    public void testSelectFromHourlyPartitionedTable()
    {
        BigQuery client = createBigQueryClient();

        executeBigQuerySql(client, "DROP TABLE IF EXISTS test.hourly_partitioned");
        executeBigQuerySql(client, "CREATE TABLE test.hourly_partitioned (value INT64, ts TIMESTAMP) PARTITION BY TIMESTAMP_TRUNC(ts, HOUR)");
        executeBigQuerySql(client, "INSERT INTO test.hourly_partitioned (value, ts) VALUES (1000, '2018-01-01 10:00:00')");

        MaterializedResult actualValues = computeActual("SELECT COUNT(1) FROM test.hourly_partitioned");

        assertEquals((long) actualValues.getOnlyValue(), 1L);
    }

    @Test(enabled = false)
    public void testSelectFromYearlyPartitionedTable()
    {
        BigQuery client = createBigQueryClient();

        executeBigQuerySql(client, "DROP TABLE IF EXISTS test.yearly_partitioned");
        executeBigQuerySql(client, "CREATE TABLE test.yearly_partitioned (value INT64, ts TIMESTAMP) PARTITION BY TIMESTAMP_TRUNC(ts, YEAR)");
        executeBigQuerySql(client, "INSERT INTO test.yearly_partitioned (value, ts) VALUES (1000, '2018-01-01 10:00:00')");

        MaterializedResult actualValues = computeActual("SELECT COUNT(1) FROM test.yearly_partitioned");

        assertEquals((long) actualValues.getOnlyValue(), 1L);
    }

    private static void executeBigQuerySql(BigQuery bigquery, String query)
    {
        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query)
                .setUseLegacySql(false)
                .build();

        JobId jobId = JobId.of();
        Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

        try {
            queryJob = queryJob.waitFor();

            if (queryJob == null) {
                throw new RuntimeException(format("Job with uuid %s does not longer exists", jobId.getJob()));
            }

            if (queryJob.getStatus().getError() != null) {
                throw new RuntimeException(format("Query '%s' failed: %s", query, queryJob.getStatus().getError()));
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }
}
