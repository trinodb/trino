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
package io.trino.tests.product.utils;

import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import io.airlift.log.Logger;
import io.trino.tempto.query.QueryExecutionException;
import io.trino.tempto.query.QueryExecutor;
import io.trino.tempto.query.QueryResult;

import java.sql.Connection;
import java.time.temporal.ChronoUnit;

import static io.trino.tempto.context.ThreadLocalTestContextHolder.testContext;
import static io.trino.tests.product.utils.DeltaQueryExecutors.createDeltaQueryExecutor;
import static io.trino.tests.product.utils.HadoopTestUtils.ERROR_COMMITTING_WRITE_TO_HIVE_RETRY_POLICY;

public final class QueryExecutors
{
    private static final Logger log = Logger.get(QueryExecutors.class);

    public static QueryExecutor onTrino()
    {
        return connectToTrino("presto");
    }

    public static QueryExecutor onCompatibilityTestServer()
    {
        return connectToTrino("compatibility-test-server");
    }

    public static QueryExecutor connectToTrino(String trinoConfig)
    {
        return new QueryExecutor()
        {
            private final QueryExecutor delegate = testContext().getDependency(QueryExecutor.class, trinoConfig);

            @Override
            public QueryResult executeQuery(String sql, QueryParam... params)
                    throws QueryExecutionException
            {
                return Failsafe.with(ERROR_COMMITTING_WRITE_TO_HIVE_RETRY_POLICY)
                        .get(() -> delegate.executeQuery(sql, params));
            }

            @Override
            public Connection getConnection()
            {
                return delegate.getConnection();
            }

            @Override
            public void close()
            {
                delegate.close();
            }
        };
    }

    public static QueryExecutor onHive()
    {
        return testContext().getDependency(QueryExecutor.class, "hive");
    }

    public static QueryExecutor onSqlServer()
    {
        return testContext().getDependency(QueryExecutor.class, "sqlserver");
    }

    public static QueryExecutor onMySql()
    {
        return testContext().getDependency(QueryExecutor.class, "mysql");
    }

    public static QueryExecutor onSpark()
    {
        return new QueryExecutor() {
            private final QueryExecutor delegate = testContext().getDependency(QueryExecutor.class, "spark");

            @Override
            public QueryResult executeQuery(String sql, QueryParam... params)
                    throws QueryExecutionException
            {
                return Failsafe.with(ERROR_COMMITTING_WRITE_TO_HIVE_RETRY_POLICY)
                        .get(() -> delegate.executeQuery(sql, params));
            }

            @Override
            public Connection getConnection()
            {
                return delegate.getConnection();
            }

            @Override
            public void close()
            {
                delegate.close();
            }
        };
    }

    public static QueryExecutor onDelta()
    {
        // Databricks clusters sometimes return HTTP 502 while starting, possibly when the gateway is up,
        // but Spark is still initializing. It's also possible an OOM on Spark will restart it, and the gateway may
        // return 502 then as well. Handling this with a query retry allows us to use the cluster's autostart feature safely,
        // while keeping costs to a minimum.

        RetryPolicy<QueryResult> databricksRetryPolicy = RetryPolicy.<QueryResult>builder()
                .handleIf(throwable -> throwable.getMessage().contains("HTTP Response code: 502"))
                .withBackoff(1, 10, ChronoUnit.SECONDS)
                .withMaxRetries(60)
                .onRetry(event -> log.warn(event.getLastException(), "Query failed on attempt %d, will retry.", event.getAttemptCount()))
                .build();

        return new QueryExecutor()
        {
            private final QueryExecutor delegate = createDeltaQueryExecutor(testContext());

            @Override
            public QueryResult executeQuery(String sql, QueryParam... params)
                    throws QueryExecutionException
            {
                return Failsafe.with(databricksRetryPolicy)
                        .get(() -> delegate.executeQuery(sql, params));
            }

            @Override
            public Connection getConnection()
            {
                return delegate.getConnection();
            }

            @Override
            public void close()
            {
                delegate.close();
            }
        };
    }

    public static QueryExecutor onHudi()
    {
        return testContext().getDependency(QueryExecutor.class, "hudi");
    }

    private QueryExecutors() {}
}
