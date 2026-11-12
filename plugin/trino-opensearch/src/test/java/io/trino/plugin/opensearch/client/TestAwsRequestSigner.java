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
package io.trino.plugin.opensearch.client;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.hc.client5.http.async.AsyncExecChain;
import org.apache.hc.core5.http.HttpRequest;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.http.message.BasicHttpRequest;
import org.apache.hc.core5.http.nio.AsyncEntityProducer;
import org.apache.hc.core5.net.URIAuthority;
import org.junit.jupiter.api.Test;
import org.opensearch.client.nio.HttpEntityAsyncEntityProducer;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

import java.net.URI;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.hash.Hashing.sha256;
import static io.trino.plugin.opensearch.AwsSecurityConfig.DeploymentType.SERVERLESS;
import static io.trino.plugin.opensearch.client.AwsRequestSigner.parseQueryParameters;
import static io.trino.plugin.opensearch.client.AwsRequestSigner.readContent;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hc.core5.http.ContentType.APPLICATION_JSON;
import static org.assertj.core.api.Assertions.assertThat;

final class TestAwsRequestSigner
{
    private static final String EMPTY_PAYLOAD_SHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

    @Test
    void testAuthorityMatchesSignedHostForDefaultPort()
            throws Exception
    {
        HttpRequest request = sign("GET", "https://example.us-east-1.aoss.amazonaws.com:443/index/_search", null);

        assertThat(request.getFirstHeader("Host").getValue()).isEqualTo("example.us-east-1.aoss.amazonaws.com");
        assertThat(request.getAuthority()).isEqualTo(new URIAuthority("example.us-east-1.aoss.amazonaws.com"));
    }

    @Test
    void testAuthorityMatchesSignedHostForCustomPort()
            throws Exception
    {
        HttpRequest request = sign("GET", "https://example.us-east-1.aoss.amazonaws.com:9200/index/_search", null);

        assertThat(request.getFirstHeader("Host").getValue()).isEqualTo("example.us-east-1.aoss.amazonaws.com:9200");
        assertThat(request.getAuthority()).isEqualTo(new URIAuthority("example.us-east-1.aoss.amazonaws.com", 9200));
    }

    @Test
    void testParseQueryParametersDecodesValues()
    {
        assertThat(parseQueryParameters(null)).isEmpty();
        assertThat(parseQueryParameters("preference=_shards%3A0&scroll=60000ms&q=a%20b%2Bc&flag"))
                .containsExactlyInAnyOrderEntriesOf(ImmutableMap.of(
                        "preference", ImmutableList.of("_shards:0"),
                        "scroll", ImmutableList.of("60000ms"),
                        "q", ImmutableList.of("a b+c"),
                        "flag", ImmutableList.of("")));
    }

    @Test
    void testSignsPayloadHash()
            throws Exception
    {
        String body = "{\"query\":{\"match_all\":{}}}";
        AsyncEntityProducer entityProducer = new HttpEntityAsyncEntityProducer(new StringEntity(body, APPLICATION_JSON));

        HttpRequest request = sign("POST", "https://example.us-east-1.aoss.amazonaws.com/index/_search", entityProducer);

        assertThat(request.getFirstHeader("x-amz-content-sha256").getValue()).isEqualTo(sha256().hashString(body, UTF_8).toString());
        // reading the body for signing must not consume it
        assertThat(new String(readContent(entityProducer), UTF_8)).isEqualTo(body);
    }

    @Test
    void testSignsEmptyPayloadHash()
            throws Exception
    {
        HttpRequest request = sign("GET", "https://example.us-east-1.aoss.amazonaws.com/index/_mapping", null);

        assertThat(request.getFirstHeader("x-amz-content-sha256").getValue()).isEqualTo(EMPTY_PAYLOAD_SHA256);
    }

    private static HttpRequest sign(String method, String uri, AsyncEntityProducer entityProducer)
            throws Exception
    {
        AwsRequestSigner signer = new AwsRequestSigner(
                "us-east-1",
                SERVERLESS,
                StaticCredentialsProvider.create(AwsBasicCredentials.create("access-key", "secret-key")));

        AtomicReference<HttpRequest> signedRequest = new AtomicReference<>();
        AtomicReference<AsyncEntityProducer> signedEntityProducer = new AtomicReference<>();
        AsyncExecChain chain = (request, producer, _, _) -> {
            signedRequest.set(request);
            signedEntityProducer.set(producer);
        };
        signer.execute(new BasicHttpRequest(method, URI.create(uri)), entityProducer, null, chain, null);

        assertThat(signedEntityProducer.get()).isSameAs(entityProducer);
        return signedRequest.get();
    }
}
