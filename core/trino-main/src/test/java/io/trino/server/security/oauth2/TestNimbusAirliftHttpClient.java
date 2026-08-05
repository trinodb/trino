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
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.testing.TestingHttpClient;
import io.trino.spi.NodeVersion;
import org.junit.jupiter.api.Test;

import java.net.URI;

import static com.google.common.net.MediaType.JSON_UTF_8;
import static io.airlift.http.client.HeaderNames.USER_AGENT;
import static io.airlift.http.client.testing.TestingResponse.mockResponse;
import static org.assertj.core.api.Assertions.assertThat;

public class TestNimbusAirliftHttpClient
{
    private static final NodeVersion TEST_VERSION = new NodeVersion("test-version");

    @Test
    public void testRetrieveResourceUserAgentHeader()
            throws Exception
    {
        NimbusAirliftHttpClient client = new NimbusAirliftHttpClient(
                new TestingHttpClient(request -> {
                    assertThat(request.getHeader(USER_AGENT)).isEqualTo("Trino/" + TEST_VERSION.version());
                    return mockResponse(HttpStatus.OK, JSON_UTF_8, "{}");
                }),
                TEST_VERSION);

        Resource resource = client.retrieveResource(URI.create("http://example.com/jwks").toURL());

        assertThat(resource.getContent()).isEqualTo("{}");
    }
}
