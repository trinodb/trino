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
package io.trino.plugin.inference;

import com.google.common.collect.ImmutableMap;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

/**
 * Tests authentication, retries, statistics, errors of requests, and an endpoint with a path, with proxies in front of OpenVINO Model Server.
 * The query runner has no workers, so the statistics of each catalog are kept by one connector instance.
 */
@Execution(SAME_THREAD)
final class TestModelServerAccess
        extends AbstractTestQueryFramework
{
    private static final String HOUSE_PRICE_QUERY =
            """
            SELECT price
            FROM TABLE(%s.system.predict(
                input => TABLE(SELECT 100e0 AS area, BIGINT '3' AS rooms),
                model_name => 'house-price'))
            """;

    private TestingModelServerProxy proxy;
    private Path tokenFile;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        TestingOpenVinoModelServer server = closeAfterClass(new TestingOpenVinoModelServer());
        proxy = closeAfterClass(new TestingModelServerProxy(server.getUri()));
        TestingModelServerProxy pathProxy = closeAfterClass(new TestingModelServerProxy(server.getUri(), "/model-server"));
        tokenFile = Files.createTempFile("inference-token", null);
        String endpoint = proxy.getUri().toString();

        QueryRunner queryRunner = DistributedQueryRunner.builder(testSessionBuilder().build())
                .setWorkerCount(0)
                .build();
        queryRunner.installPlugin(new InferencePlugin());
        queryRunner.createCatalog("no_token", "inference", ImmutableMap.of(
                "inference.endpoint", endpoint));
        queryRunner.createCatalog("bearer_token", "inference", ImmutableMap.of(
                "inference.endpoint", endpoint,
                "inference.authentication.token", "secret"));
        queryRunner.createCatalog("header_token_file", "inference", ImmutableMap.of(
                "inference.endpoint", endpoint,
                "inference.authentication.header-name", "infer-key",
                "inference.authentication.token-file", tokenFile.toString()));
        queryRunner.createCatalog("no_retries", "inference", ImmutableMap.of(
                "inference.endpoint", endpoint,
                "inference.max-retries", "0"));
        queryRunner.createCatalog("unreachable", "inference", ImmutableMap.of(
                "inference.endpoint", "http://127.0.0.1:1",
                "inference.max-retries", "1"));
        queryRunner.createCatalog("endpoint_with_path", "inference", ImmutableMap.of(
                "inference.endpoint", pathProxy.getUri().toString()));
        queryRunner.createCatalog("short_timeout", "inference", ImmutableMap.of(
                "inference.endpoint", endpoint,
                "inference.max-retries", "0",
                "inference.http-client.request-timeout", "1s"));
        return queryRunner;
    }

    @Test
    void testEndpointWithPath()
    {
        // the proxy serves the model server only under the path of the endpoint, so the requests fail unless they keep the path
        assertThat(query(HOUSE_PRICE_QUERY.formatted("endpoint_with_path"))).matches("VALUES 115000e0");
    }

    @Test
    void testBearerToken()
    {
        proxy.requireHeader("Authorization", "Bearer secret");
        try {
            assertThat(query(HOUSE_PRICE_QUERY.formatted("bearer_token"))).matches("VALUES 115000e0");
            assertThat(query(HOUSE_PRICE_QUERY.formatted("no_token")))
                    .failure().hasMessageMatching("Model server rejected the authentication of the request to .*/v2/models/house-price with HTTP status 401. Check the inference.authentication properties of the catalog");
        }
        finally {
            proxy.doNotRequireHeader();
        }
    }

    @Test
    void testCustomHeaderWithRotatedTokenFile()
            throws IOException
    {
        try {
            proxy.requireHeader("infer-key", "first");
            Files.writeString(tokenFile, "first\n", UTF_8);
            assertThat(query(HOUSE_PRICE_QUERY.formatted("header_token_file"))).matches("VALUES 115000e0");

            // the file is read for every request, so a rotated token is used without restarting Trino
            proxy.requireHeader("infer-key", "second");
            Files.writeString(tokenFile, "second\n", UTF_8);
            assertThat(query(HOUSE_PRICE_QUERY.formatted("header_token_file"))).matches("VALUES 115000e0");
        }
        finally {
            proxy.doNotRequireHeader();
        }
    }

    @Test
    void testRetries()
            throws Exception
    {
        long retries = statistic("no_token", "Retries.TotalCount");
        long rows = statistic("no_token", "InferenceRows.TotalCount");

        // the first request of the query fails, and succeeds when retried
        proxy.failNextRequests(1);
        assertThat(query(
                """
                SELECT count(*)
                FROM TABLE(no_token.system.predict(
                    input => TABLE(SELECT CAST(x AS double) AS area, BIGINT '1' AS rooms FROM UNNEST(sequence(1, 5)) t(x)),
                    model_name => 'house-price'))
                WHERE price IS NOT NULL
                """))
                .matches("VALUES BIGINT '5'");
        assertThat(statistic("no_token", "Retries.TotalCount") - retries).isEqualTo(1);
        assertThat(statistic("no_token", "InferenceRows.TotalCount") - rows).isEqualTo(5);

        proxy.failNextRequests(1);
        assertThat(query(HOUSE_PRICE_QUERY.formatted("no_retries")))
                .failure().hasMessageMatching("(?s)Request to .*/v2/models/house-price failed with HTTP status 503\\. Response: .*");
        proxy.failNextRequests(0);
    }

    @Test
    void testConnectionFailure()
            throws Exception
    {
        long retries = statistic("unreachable", "Retries.TotalCount");
        long failures = statistic("unreachable", "FailedRequests.TotalCount");
        assertThat(query(HOUSE_PRICE_QUERY.formatted("unreachable")))
                .failure().hasMessageMatching("(?s)Failed to fetch metadata of model house-price from http://127.0.0.1:1/v2/models/house-price: .+\\. Check that the model server is running, and the inference.endpoint property of the catalog");
        assertThat(statistic("unreachable", "Retries.TotalCount") - retries).isEqualTo(1);
        assertThat(statistic("unreachable", "FailedRequests.TotalCount") - failures).isEqualTo(1);
    }

    @Test
    void testRequestTooLarge()
    {
        // like the request size limit of a proxy
        proxy.limitRequestBodySize(10);
        try {
            assertThat(query(HOUSE_PRICE_QUERY.formatted("no_token")))
                    .failure().hasMessageMatching("(?s)Request to .*/v2/models/house-price/infer failed with HTTP status 413\\. Lower the inference.max-batch-size property of the catalog\\. Response: .*");
        }
        finally {
            proxy.limitRequestBodySize(Integer.MAX_VALUE);
        }
    }

    @Test
    void testTimeout()
    {
        proxy.delayInferenceRequests(Duration.ofSeconds(3));
        try {
            assertThat(query(HOUSE_PRICE_QUERY.formatted("short_timeout")))
                    .failure().hasMessageMatching("(?s)Inference request for model house-price to .*/v2/models/house-price/infer failed: .+\\. The model server did not respond in time. Increase the inference.http-client.request-timeout and inference.http-client.idle-timeout properties, or lower the inference.max-batch-size property of the catalog");
        }
        finally {
            proxy.delayInferenceRequests(Duration.ZERO);
        }
    }

    private static long statistic(String catalog, String attribute)
            throws Exception
    {
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        ObjectName name = new ObjectName("trino.plugin.inference:type=OpenInferenceClient,name=" + catalog);
        return ((Number) server.getAttribute(name, attribute)).longValue();
    }
}
