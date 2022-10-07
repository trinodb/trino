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
package io.trino.client;

import okhttp3.ConnectionPool;
import okhttp3.Headers;
import okhttp3.HttpUrl;
import okhttp3.Interceptor;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.net.HttpHeaders.USER_AGENT;
import static io.trino.client.JsonCodec.jsonCodec;

public class MultiThreadedQuery
        implements Runnable
{
    private static final String USER_AGENT_VALUE = StatementClientV1.class.getSimpleName() +
            "/" +
            firstNonNull(StatementClientV1.class.getPackage().getImplementationVersion(), "unknown");
    private static final String HEADER_NEXTURI = "X-Trino-NextUri";
    private static final JsonCodec<QueryResults> QUERY_RESULTS_CODEC = jsonCodec(QueryResults.class);
    StatementClient client;
    ClientSession session;
    Results results;
    Results currentResults;
    Semaphore allResultsReady;
    LocalDateTime start;
    String nextUri;
    Optional<String> user;

    public MultiThreadedQuery(ClientSession session, Headers headers, StatementClient caller, Optional<String> user)
    {
        this.start = LocalDateTime.now();
        this.client = caller;
        this.session = session;
        this.user = user;
        this.nextUri = headers.get(HEADER_NEXTURI);
//        System.out.println("DEBUG: " + nextUri);
        this.allResultsReady = new Semaphore(1);
        try {
            this.allResultsReady.acquire();
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        Results newResults = new Results(0);
        newResults.nextUriReady(nextUri);
        this.results = newResults;
        this.currentResults = newResults;

        Thread thread = new Thread(this);
        thread.start();
    }

    /**
     * Starts creating a Results linked list
     */
    @Override
    public void run()
    {
        ExecutorService executorService = Executors.newFixedThreadPool(8);
        ConnectionPool pool = new ConnectionPool(10, 5, TimeUnit.SECONDS);

        String nextUri = this.nextUri;
        Results newResults = this.results;
        int counter = 1;

        while (nextUri != null) {
            try {
                counter++;

                String urlToDownload = nextUri;
                Results res = newResults;
                LocalDateTime start = LocalDateTime.now();
                executorService.submit(() -> downloadData(urlToDownload, res));
                res.waitForNextUri();
                nextUri = res.nextUri;
                if (nextUri != null) {
                    newResults = new Results(counter);
                    newResults.nextUri = nextUri;
                    results.next = newResults;
                    results = newResults;
                }
            }
            catch (Exception e) {
                e.printStackTrace();
                nextUri = null;
            }
        }

        LocalDateTime end = LocalDateTime.now();
        Duration duration = Duration.between(start, end);
        System.out.printf("NextUri trail completed in %2d.%06ds (%d result sets)\n", duration.getSeconds(), duration.getNano() / 1000, counter);

        executorService.shutdown();
        end = LocalDateTime.now();
        duration = Duration.between(start, end);
        System.out.printf("Fetch data completed in %2d.%06ds\n", duration.getSeconds(), duration.getNano() / 1000);
    }

    /**
     * Downloads a given URI, parse it and store the results
     * @param nextUri the URI to download
     * @param results the Results object where to store the results
     */
    private void downloadData(String nextUri, Results results)
    {
        try {
            HttpUrl url = HttpUrl.get(nextUri);
            OkHttpClient client = new OkHttpClient.Builder()
                    .addNetworkInterceptor(new HeadersInterceptor(results))
                    .build();

            Request.Builder builder = new Request.Builder()
                    .addHeader(USER_AGENT, USER_AGENT_VALUE);
//                    .addHeader(ACCEPT_ENCODING, "gzip");

            if (this.user.isPresent()) {
                builder = builder.addHeader("X-Trino-User", user.get());
            }

            Request request = builder
                    .url(url)
                    .build();

            results.content = JsonResponse.execute(QUERY_RESULTS_CODEC, client, request, OptionalLong.empty());
            results.dataReady();
        }
        catch (Exception e) {
            System.out.printf("ERROR FOR %s\n", results.nextUri);
            e.printStackTrace();
        }
    }

    /**
     * Called when ResultSet.next() is called
     *
     * @return the QueryResults representing the data returned by a URI
     */
    public JsonResponse<QueryResults> getNextResults()
    {
        if (currentResults == null) {
            return null;
        }
        boolean acquired = false;

        while (!acquired) {
            try {
//            System.out.printf("Check [%d] %s\n", currentResults.nb, currentResults.nextUri);
                currentResults.waitForData();
                acquired = true;
//            System.out.printf("Check [%d] %s DONE\n", currentResults.nb, currentResults.nextUri);
            }
            catch (Exception e) {
                acquired = false;
            }
        }

        JsonResponse<QueryResults> results = currentResults.content;
        if (results.getStatusCode() != 200) {
            System.out.printf("Problem [%d]: HTTP error code %d\n", currentResults.nb, results.getStatusCode());
        }
        currentResults = currentResults.next;
        return results;
    }

    /**
     * Element of a chained list which contains QueryResults data
     * (QueryResults is what is the Trino client expecting)
     */
    public static class Results
    {
        Results next;
        int nb;
        String nextUri;
        JsonResponse<QueryResults> content;
        int index;
        private final Semaphore dataLock;
        private final Semaphore nextUriLock;

        public Results(int nb)
        {
            this.nb = nb;
            this.dataLock = new Semaphore(1);
            this.nextUriLock = new Semaphore(1);
            try {
                this.dataLock.acquire();
                this.nextUriLock.acquire();
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }

        public void waitForNextUri()
                throws InterruptedException
        {
            this.nextUriLock.acquire();
        }

        public void nextUriReady(String nextUri)
        {
            this.nextUri = nextUri;
            this.nextUriLock.release();
        }

        public void waitForData()
                throws InterruptedException
        {
            this.dataLock.acquire();
        }

        public void dataReady()
        {
            this.dataLock.release();
        }
    }

    /**
     * OkHttp hook
     */
    static class HeadersInterceptor
            implements Interceptor
    {
        private final Results results;

        public HeadersInterceptor(Results results)
        {
            this.results = results;
        }

        @Override public Response intercept(Interceptor.Chain chain) throws IOException
        {
            Request request = chain.request();
            Response response = chain.proceed(request);
            String nextUri = response.header(HEADER_NEXTURI);
            results.nextUriReady(nextUri);

            return response;
        }
    }
}
