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
package io.trino.plugin.prometheus;

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

import static io.trino.plugin.prometheus.PrometheusQueryRunner.createPrometheusQueryRunner;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

/**
 * Integration tests against Prometheus container
 */
@Test(singleThreaded = true)
public class TestPrometheusIntegrationStatus
        extends AbstractTestQueryFramework
{
    private PrometheusServer server;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.server = new PrometheusServer();
        return createPrometheusQueryRunner(server, ImmutableMap.of());
    }

    @Test
    public void testConfirmMetricAvailableAndCheckUp()
            throws Exception
    {
        int maxTries = 60;
        int timeBetweenTriesMillis = 1000;
        int tries = 0;
        final OkHttpClient httpClient = new OkHttpClient.Builder()
                .connectTimeout(120, TimeUnit.SECONDS)
                .readTimeout(120, TimeUnit.SECONDS)
                .build();
        HttpUrl.Builder urlBuilder = HttpUrl.parse(server.getUri().toString()).newBuilder().encodedPath("/api/v1/query");
        urlBuilder.addQueryParameter("query", "up[1d]");
        String url = urlBuilder.build().toString();
        Request request = new Request.Builder()
                .url(url)
                .build();
        String responseBody;
        // this seems to be a reliable way to ensure Prometheus has `up` metric data
        while (tries < maxTries) {
            responseBody = httpClient.newCall(request).execute().body().string();
            if (responseBody.contains("values")) {
                Logger log = Logger.get(TestPrometheusIntegrationStatus.class);
                log.info("prometheus response: %s", responseBody);
                break;
            }
            Thread.sleep(timeBetweenTriesMillis);
            tries++;
        }
        if (tries == maxTries) {
            fail("Prometheus container not available for metrics query in " + maxTries * timeBetweenTriesMillis + " milliseconds.");
        }
        // now we're making sure the client is ready
        tries = 0;
        while (tries < maxTries) {
            if (getQueryRunner().tableExists(getSession(), "up")) {
                break;
            }
            Thread.sleep(timeBetweenTriesMillis);
            tries++;
        }
        if (tries == maxTries) {
            fail("Prometheus container, or client, not available for metrics query in " + maxTries * timeBetweenTriesMillis + " milliseconds.");
        }

        MaterializedResult results = computeActual("SELECT * FROM prometheus.default.up LIMIT 1");
        assertEquals(results.getRowCount(), 1);
        MaterializedRow row = results.getMaterializedRows().get(0);
        assertEquals(row.getField(0).toString(), "{instance=localhost:9090, __name__=up, job=prometheus}");
    }

    @Test
    public void testPushDown()
    {
        // default interval on the `up` metric that Prometheus records on itself is 15 seconds, so this should only yield one row
        MaterializedResult results = computeActual("SELECT * FROM prometheus.default.up WHERE timestamp > (NOW() - INTERVAL '15' SECOND)");
        assertEquals(results.getRowCount(), 1);
    }
}
