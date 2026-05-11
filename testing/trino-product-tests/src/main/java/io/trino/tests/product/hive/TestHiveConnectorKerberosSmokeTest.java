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
package io.trino.tests.product.hive;

import com.google.common.io.Closer;
import io.trino.tempto.AfterMethodWithContext;
import io.trino.tempto.BeforeMethodWithContext;
import io.trino.tempto.query.QueryResult;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.nullToEmpty;
import static io.trino.tests.product.TestGroups.HIVE_KERBEROS;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class TestHiveConnectorKerberosSmokeTest
{
    private static final String CANCELLATION_MESSAGE = "explicitly cancelled for test without failure";
    private static final String CANCELLATION_FAILURE_SUFFIX = "Query killed. Message: " + CANCELLATION_MESSAGE;

    private Closer closer;
    private ExecutorService executor;

    @BeforeMethodWithContext
    public void setUp()
    {
        closer = Closer.create();
        executor = newSingleThreadExecutor(); // single thread is enough, it schedules the query to cancel
        closer.register(executor::shutdownNow);
    }

    @AfterMethodWithContext
    public void cleanUp()
            throws IOException
    {
        executor = null;
        closer.close();
        closer = null;
    }

    @Test(groups = {HIVE_KERBEROS, PROFILE_SPECIFIC_TESTS}, timeOut = 120_000L)
    public void kerberosTicketExpiryTest()
            throws Exception
    {
        // Session properties to reduce memory usage and also make the query run longer
        onTrino().executeQuery("SET SESSION scale_writers = false");
        onTrino().executeQuery("SET SESSION task_scale_writers_enabled = false");

        String sql = "CREATE TABLE orders AS SELECT * FROM tpch.sf1000.orders";
        Future<?> queryExecution = executor.submit(() -> onTrino().executeQuery(sql));

        // 2x of ticket_lifetime as configured in hadoop-kerberos krb5.conf, sufficient to cause at-least 1 ticket expiry
        SECONDS.sleep(60L);
        failIfQueryFinishedBeforeCancellation(queryExecution, null);
        cancelQueryIfRunning(sql, queryExecution);

        try {
            queryExecution.get(30, SECONDS);
            fail("Expected query to have failed");
        }
        catch (TimeoutException e) {
            queryExecution.cancel(true);
            throw e;
        }
        catch (ExecutionException expected) {
            assertThat(expected.getCause())
                    .hasMessageEndingWith(CANCELLATION_FAILURE_SUFFIX);
        }
    }

    private void cancelQueryIfRunning(String sql, Future<?> queryExecution)
            throws Exception
    {
        QueryResult queryResult = onTrino().executeQuery("SELECT query_id FROM system.runtime.queries WHERE query = '%s' AND state = 'RUNNING' LIMIT 2".formatted(sql));
        checkState(queryResult.getRowsCount() < 2, "Found multiple queries");
        if (queryResult.getRowsCount() == 0) {
            failIfQueryFinishedBeforeCancellation(queryExecution, null);
            return;
        }

        String queryId = (String) queryResult.getOnlyValue();
        try {
            onTrino().executeQuery("CALL system.runtime.kill_query(query_id => '%s', message => '%s')".formatted(queryId, CANCELLATION_MESSAGE));
        }
        catch (RuntimeException e) {
            if (!nullToEmpty(e.getMessage()).contains("Target query is not running")) {
                throw e;
            }
            failIfQueryFinishedBeforeCancellation(queryExecution, e);
            throw e;
        }
    }

    private static void failIfQueryFinishedBeforeCancellation(Future<?> queryExecution, RuntimeException cancellationFailure)
            throws Exception
    {
        if (!queryExecution.isDone()) {
            return;
        }

        try {
            queryExecution.get();
        }
        catch (ExecutionException e) {
            AssertionError failure = new AssertionError("Query failed before it could be cancelled; Kerberos ticket refresh was not verified", e.getCause());
            if (cancellationFailure != null) {
                failure.addSuppressed(cancellationFailure);
            }
            throw failure;
        }

        AssertionError failure = new AssertionError("Query completed before it could be cancelled; increase the input size to exercise Kerberos ticket refresh");
        if (cancellationFailure != null) {
            failure.addSuppressed(cancellationFailure);
        }
        throw failure;
    }
}
