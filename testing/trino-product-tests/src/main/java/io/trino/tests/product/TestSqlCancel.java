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
package io.trino.tests.product;

import com.google.common.base.Stopwatch;
import com.google.common.io.Closer;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import io.airlift.http.client.ResponseHandler;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.trino.tempto.AfterMethodWithContext;
import io.trino.tempto.BeforeMethodWithContext;
import io.trino.tempto.ProductTest;
import io.trino.tempto.query.QueryResult;
import org.testng.annotations.Test;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.Request.Builder.prepareDelete;
import static io.airlift.http.client.ResponseHandlerUtils.propagate;
import static io.trino.tempto.assertions.QueryAssert.assertQueryFailure;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static java.lang.System.nanoTime;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;

public class TestSqlCancel
        extends ProductTest
{
    private ExecutorService executor;
    private QueryCanceller queryCanceller;
    private Closer closer;

    @Inject
    @Named("databases.trino.server_address")
    private String serverAddress;

    @BeforeMethodWithContext
    public void setUp()
    {
        closer = Closer.create();
        executor = newSingleThreadExecutor(); // single thread is enough, it schedules the query to cancel
        closer.register(executor::shutdownNow);
        queryCanceller = closer.register(new QueryCanceller(serverAddress));
    }

    @AfterMethodWithContext
    public void cleanUp()
            throws IOException
    {
        queryCanceller = null; // closed via closer
        executor = null; // closed via closer
        closer.close();
        closer = null;
    }

    @Test(timeOut = 60_000L)
    public void cancelCreateTable()
            throws Exception
    {
        String tableName = "cancel_createtable_" + nanoTime();
        String sql = format("CREATE TABLE %s AS SELECT * FROM tpch.sf1.lineitem", tableName);

        runAndCancelQuery(sql);
        assertQueryFailure(() -> onTrino().executeQuery("SELECT * from " + tableName))
                .hasMessageContaining("Table 'hive.default.%s' does not exist", tableName);
    }

    @Test(timeOut = 60_000L)
    public void cancelInsertInto()
            throws Exception
    {
        String tableName = "cancel_insertinto_" + nanoTime();
        onTrino().executeQuery(format("CREATE TABLE %s (orderkey BIGINT, partkey BIGINT, shipinstruct VARCHAR(25)) ", tableName));
        String sql = format("INSERT INTO %s SELECT orderkey, partkey, shipinstruct FROM tpch.sf1.lineitem", tableName);
        runAndCancelQuery(sql);
        assertThat(onTrino().executeQuery("SELECT * from " + tableName)).hasNoRows();
        onTrino().executeQuery("DROP TABLE " + tableName);
    }

    @Test(timeOut = 60_000L)
    public void cancelSelect()
            throws Exception
    {
        runAndCancelQuery("SELECT * FROM tpch.sf1.lineitem AS cancel_select_" + nanoTime());
    }

    private void runAndCancelQuery(String sql)
            throws Exception
    {
        Future<?> queryExecution = executor.submit(() -> onTrino().executeQuery(sql));

        cancelQuery(sql);

        try {
            queryExecution.get(30, SECONDS);
            fail("Query failure was expected");
        }
        catch (TimeoutException e) {
            queryExecution.cancel(true);
            throw e;
        }
        catch (ExecutionException expected) {
            assertThat(expected.getCause())
                    .hasMessageEndingWith("Query was canceled");
        }
    }

    private void cancelQuery(String sql)
            throws InterruptedException
    {
        Stopwatch stopwatch = Stopwatch.createStarted();
        while (stopwatch.elapsed(SECONDS) < 30) {
            String findQuerySql = "SELECT query_id from system.runtime.queries WHERE query = '%s' and state = 'RUNNING' LIMIT 2";
            QueryResult queryResult = onTrino().executeQuery(format(findQuerySql, sql));
            checkState(queryResult.getRowsCount() < 2, "Query is executed multiple times");
            if (queryResult.getRowsCount() == 1) {
                String queryId = (String) queryResult.getOnlyValue();
                Response response = queryCanceller.cancel(queryId);
                assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NO_CONTENT.code());
                return;
            }
            MILLISECONDS.sleep(100L);
        }
        throw new IllegalStateException("Query did not reach running state or maybe it was too quick.");
    }

    private static class QueryCanceller
            implements Closeable
    {
        private final HttpClient httpClient;
        private final URI uri;

        QueryCanceller(String uri)
        {
            this.httpClient = new JettyHttpClient(new HttpClientConfig());
            this.uri = URI.create(requireNonNull(uri, "uri is null"));
        }

        public Response cancel(String queryId)
        {
            requireNonNull(queryId, "queryId is null");
            URI cancelUri = uriBuilderFrom(uri).appendPath("/v1/query").appendPath(queryId).build();
            Request request = prepareDelete()
                    .setHeader("X-Trino-User", "anyUser")
                    .setUri(cancelUri)
                    .build();
            return httpClient.execute(request, new ResponseHandler<>()
            {
                @Override
                public Response handleException(Request request, Exception exception)
                {
                    throw propagate(request, exception);
                }

                @Override
                public Response handle(Request request, Response response)
                {
                    return response;
                }
            });
        }

        @Override
        public void close()
        {
            httpClient.close();
        }
    }
}
