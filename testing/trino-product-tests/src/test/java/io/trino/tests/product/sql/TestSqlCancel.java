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
package io.trino.tests.product.sql;

import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.tests.product.TestGroup;
import io.trino.tests.product.tpc.TpchEnvironment;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import static java.lang.System.nanoTime;
import static java.net.http.HttpResponse.BodyHandlers.discarding;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

@ProductTest
@RequiresEnvironment(TpchEnvironment.class)
@TestGroup.SqlCancel
class TestSqlCancel
{
    private static final HttpClient HTTP_CLIENT = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .build();

    @Test
    @Timeout(60)
    void cancelCreateTable(TpchEnvironment environment)
            throws Exception
    {
        String tableName = "cancel_createtable_" + nanoTime();
        runAndCancelQuery(environment, "CREATE TABLE %s AS SELECT * FROM tpch.sf1.lineitem".formatted(tableName));

        assertThatThrownBy(() -> environment.executeTrino("SELECT * FROM " + tableName))
                .hasMessageContaining("Table 'hive.tpch.%s' does not exist".formatted(tableName));
    }

    @Test
    @Timeout(60)
    void cancelInsertInto(TpchEnvironment environment)
            throws Exception
    {
        String tableName = "cancel_insertinto_" + nanoTime();
        environment.executeTrinoUpdate("CREATE TABLE %s (orderkey BIGINT, partkey BIGINT, shipinstruct VARCHAR(25))".formatted(tableName));
        try {
            runAndCancelQuery(
                    environment,
                    "INSERT INTO %s SELECT orderkey, partkey, shipinstruct FROM tpch.sf1.lineitem".formatted(tableName));
            assertThat(environment.executeTrino("SELECT * FROM " + tableName).rows()).isEmpty();
        }
        finally {
            environment.executeTrinoUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    @Timeout(60)
    void cancelSelect(TpchEnvironment environment)
            throws Exception
    {
        runAndCancelQuery(environment, "SELECT * FROM tpch.sf1.lineitem AS cancel_select_" + nanoTime());
    }

    private static void runAndCancelQuery(TpchEnvironment environment, String sql)
            throws Exception
    {
        ExecutorService executor = newSingleThreadExecutor();
        try {
            Future<?> queryExecution = executor.submit(() -> {
                executeAndConsume(environment, sql);
                return null;
            });

            cancelQuery(environment, sql);

            try {
                queryExecution.get(30, SECONDS);
                fail("Query failure was expected");
            }
            catch (TimeoutException e) {
                queryExecution.cancel(true);
                throw e;
            }
            catch (ExecutionException expected) {
                assertThat(expected.getCause()).hasMessageEndingWith("Query was canceled");
            }
        }
        finally {
            executor.shutdownNow();
        }
    }

    private static void executeAndConsume(TpchEnvironment environment, String sql)
            throws Exception
    {
        try (Connection connection = environment.createTrinoConnection();
                Statement statement = connection.createStatement()) {
            if (statement.execute(sql)) {
                try (ResultSet resultSet = statement.getResultSet()) {
                    while (resultSet.next()) {
                        // Consume the result so SELECT remains active until completion or cancellation.
                    }
                }
            }
        }
    }

    private static void cancelQuery(TpchEnvironment environment, String sql)
            throws Exception
    {
        long deadline = nanoTime() + SECONDS.toNanos(30);
        while (nanoTime() < deadline) {
            String escapedSql = sql.replace("'", "''");
            var queryResult = environment.executeTrino(
                    "SELECT query_id FROM system.runtime.queries WHERE query = '%s' AND state IN ('QUEUED', 'RUNNING') LIMIT 2"
                            .formatted(escapedSql));
            assertThat(queryResult.getRowsCount()).as("matching active queries").isLessThan(2);
            if (queryResult.getRowsCount() == 1) {
                String queryId = (String) queryResult.getOnlyValue();
                URI coordinatorUri = URI.create(environment.getTrinoJdbcUrl().replace("jdbc:trino:", "http:"));
                HttpRequest request = HttpRequest.newBuilder(coordinatorUri.resolve("/v1/query/" + queryId))
                        .header("X-Trino-User", "anyUser")
                        .DELETE()
                        .build();
                assertThat(HTTP_CLIENT.send(request, discarding()).statusCode()).isEqualTo(204);
                return;
            }
            MILLISECONDS.sleep(10);
        }
        throw new IllegalStateException("Query did not reach running state or completed before it could be cancelled");
    }
}
