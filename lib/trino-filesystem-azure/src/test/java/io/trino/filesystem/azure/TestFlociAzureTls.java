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
package io.trino.filesystem.azure;

import com.azure.core.http.HttpMethod;
import com.azure.core.http.HttpRequest;
import com.azure.core.http.HttpResponse;
import com.azure.core.util.Context;
import io.trino.filesystem.azure.AbstractTestAzureFileSystemFloci.FlociAzureResource;
import org.junit.jupiter.api.Test;
import reactor.netty.http.client.HttpClient;

import javax.net.ssl.SSLContext;

import static org.assertj.core.api.Assertions.assertThat;

class TestFlociAzureTls
{
    @Test
    void testClientTrustIsIsolated()
            throws Exception
    {
        SSLContext defaultContext = SSLContext.getDefault();
        String trustStore = System.getProperty("javax.net.ssl.trustStore");
        String trustStorePassword = System.getProperty("javax.net.ssl.trustStorePassword");
        String trustStoreType = System.getProperty("javax.net.ssl.trustStoreType");
        // Initialize Netty's cached default TLS provider before starting the emulator.
        assertThat(HttpClient.create().secure()).isNotNull();

        try (FlociAzureResource resource = new FlociAzureResource()) {
            HttpRequest request = new HttpRequest(HttpMethod.GET, resource.flociAzure().getEndpoint().resolve("/health").toString());
            try (HttpResponse response = resource.httpClient().sendSync(request, Context.NONE)) {
                assertThat(response.getStatusCode()).isEqualTo(200);
            }
            assertThat(SSLContext.getDefault()).isSameAs(defaultContext);
            assertThat(System.getProperty("javax.net.ssl.trustStore")).isEqualTo(trustStore);
            assertThat(System.getProperty("javax.net.ssl.trustStorePassword")).isEqualTo(trustStorePassword);
            assertThat(System.getProperty("javax.net.ssl.trustStoreType")).isEqualTo(trustStoreType);
        }

        assertThat(SSLContext.getDefault()).isSameAs(defaultContext);
        assertThat(System.getProperty("javax.net.ssl.trustStore")).isEqualTo(trustStore);
        assertThat(System.getProperty("javax.net.ssl.trustStorePassword")).isEqualTo(trustStorePassword);
        assertThat(System.getProperty("javax.net.ssl.trustStoreType")).isEqualTo(trustStoreType);
    }
}
