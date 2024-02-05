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
package io.trino.plugin.opensearch;

import com.amazonaws.util.Base64;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import com.google.common.net.HostAndPort;
import io.trino.sql.query.QueryAssertions;
import io.trino.testing.QueryRunner;
import org.apache.http.HttpHost;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Isolated;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.Optional;

import static com.google.common.io.Resources.getResource;
import static io.airlift.testing.Closeables.closeAll;
import static io.trino.plugin.base.ssl.SslUtils.createSSLContext;
import static io.trino.plugin.opensearch.OpenSearchQueryRunner.createOpenSearchQueryRunner;
import static io.trino.plugin.opensearch.OpenSearchServer.OPENSEARCH_IMAGE;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@Isolated
@TestInstance(PER_CLASS)
public class TestPasswordAuthentication
{
    private static final String USER = "admin";
    private static final String PASSWORD = "admin";

    private OpenSearchServer opensearch;
    private RestHighLevelClient client;
    private QueryAssertions assertions;

    @BeforeAll
    public void setUp()
            throws Exception
    {
        opensearch = new OpenSearchServer(OPENSEARCH_IMAGE, true, ImmutableMap.<String, String>builder()
                .put("opensearch.yml", loadResource("opensearch.yml"))
                .put("esnode.pem", loadResource("esnode.pem"))
                .put("esnode-key.pem", loadResource("esnode-key.pem"))
                .put("root-ca.pem", loadResource("root-ca.pem"))
                .buildOrThrow());

        HostAndPort address = opensearch.getAddress();
        client = new RestHighLevelClient(RestClient.builder(new HttpHost(address.getHost(), address.getPort(), "https"))
                .setHttpClientConfigCallback(this::setupSslContext));

        QueryRunner runner = createOpenSearchQueryRunner(
                opensearch.getAddress(),
                ImmutableList.of(),
                ImmutableMap.of(),
                ImmutableMap.<String, String>builder()
                        .put("opensearch.security", "PASSWORD")
                        .put("opensearch.auth.user", USER)
                        .put("opensearch.auth.password", PASSWORD)
                        .put("opensearch.tls.enabled", "true")
                        .put("opensearch.tls.verify-hostnames", "false")
                        .put("opensearch.tls.truststore-path", new File(getResource("truststore.jks").toURI()).getPath())
                        .put("opensearch.tls.truststore-password", "123456")
                        .buildOrThrow(),
                3);

        assertions = new QueryAssertions(runner);
    }

    private HttpAsyncClientBuilder setupSslContext(HttpAsyncClientBuilder clientBuilder)
    {
        try {
            return clientBuilder.setSSLContext(createSSLContext(
                    Optional.empty(),
                    Optional.empty(),
                    Optional.of(new File(Resources.getResource("truststore.jks").toURI())),
                    Optional.of("123456")));
        }
        catch (GeneralSecurityException | IOException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @AfterAll
    public final void destroy()
            throws IOException
    {
        closeAll(
                () -> assertions.close(),
                () -> opensearch.stop(),
                () -> client.close());

        assertions = null;
        opensearch = null;
        client = null;
    }

    @Test
    public void test()
            throws IOException
    {
        String json = new ObjectMapper().writeValueAsString(ImmutableMap.of("value", 42L));

        Request request = new Request("POST", "/test/_doc?refresh");
        request.setJsonEntity(json);
        request.setOptions(RequestOptions.DEFAULT.toBuilder()
                .addHeader("Authorization", format("Basic %s", Base64.encodeAsString(format("%s:%s", USER, PASSWORD).getBytes(StandardCharsets.UTF_8)))));
        client.getLowLevelClient().performRequest(request);

        assertThat(assertions.query("SELECT * FROM test"))
                .matches("VALUES BIGINT '42'");
    }

    private static String loadResource(String file)
            throws IOException
    {
        return Resources.toString(getResource(file), UTF_8);
    }
}
