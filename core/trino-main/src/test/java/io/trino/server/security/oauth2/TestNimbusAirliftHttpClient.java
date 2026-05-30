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
package io.trino.server.security.oauth2;

import com.nimbusds.jose.util.Resource;
import io.airlift.http.client.HeaderName;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.testing.TestingHttpClient;
import io.trino.spi.NodeVersion;
import org.junit.jupiter.api.Test;

import java.net.URI;

import static com.google.common.net.HttpHeaders.USER_AGENT;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static io.airlift.http.client.testing.TestingResponse.mockResponse;
import static org.assertj.core.api.Assertions.assertThat;

public class TestNimbusAirliftHttpClient
{
    @Test
    public void testRetrieveResourceUserAgentHeader()
            throws Exception
    {
        NimbusAirliftHttpClient client = new NimbusAirliftHttpClient(
                new TestingHttpClient(request -> {
                    assertThat(request.getHeader(HeaderName.of(USER_AGENT))).isEqualTo("Trino/test-version");
                    return mockResponse(HttpStatus.OK, JSON_UTF_8, "{}");
                }),
                new NodeVersion("test-version"));

        Resource resource = client.retrieveResource(URI.create("http://example.com/jwks").toURL());

        assertThat(resource.getContent()).isEqualTo("{}");
    }
}
