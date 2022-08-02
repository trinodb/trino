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
package io.trino.plugin.pinot;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.net.HttpHeaders;
import com.google.common.net.MediaType;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.testing.TestingHttpClient;
import io.airlift.http.client.testing.TestingResponse;
import io.airlift.units.Duration;
import io.trino.plugin.pinot.auth.PinotAuthenticationProvider;
import io.trino.plugin.pinot.auth.PinotBrokerAuthenticationProvider;
import io.trino.plugin.pinot.auth.PinotControllerAuthenticationProvider;
import io.trino.plugin.pinot.auth.none.PinotEmptyAuthenticationProvider;
import io.trino.plugin.pinot.auth.password.PinotPasswordAuthenticationProvider;
import io.trino.plugin.pinot.client.IdentityPinotHostMapper;
import io.trino.plugin.pinot.client.PinotClient;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.airlift.concurrent.Threads.threadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.assertj.core.api.Assertions.assertThat;

public class TestPinotClient
{
    @Test
    public void testBrokersParsed()
    {
        HttpClient httpClient = new TestingHttpClient(request -> {
            assertThat(request.getHeaders().get("k1").get(0)).isEqualTo("v1");
            assertThat(request.getHeaders().get("k2").get(0)).isEqualTo("v2");
            assertThat(request.getHeaders().get("k3").get(0)).isEqualTo("some random v3");
            return TestingResponse.mockResponse(HttpStatus.OK, MediaType.JSON_UTF_8, "{\n" +
                "  \"tableName\": \"dummy\",\n" +
                "  \"brokers\": [\n" +
                "    {\n" +
                "      \"tableType\": \"offline\",\n" +
                "      \"instances\": [\n" +
                "        \"Broker_dummy-broker-host1-datacenter1_6513\",\n" +
                "        \"Broker_dummy-broker-host2-datacenter1_6513\",\n" +
                "        \"Broker_dummy-broker-host4-datacenter1_6513\"\n" +
                "      ]\n" +
                "    },\n" +
                "    {\n" +
                "      \"tableType\": \"realtime\",\n" +
                "      \"instances\": [\n" +
                "        \"Broker_dummy-broker-host1-datacenter1_6513\",\n" +
                "        \"Broker_dummy-broker-host2-datacenter1_6513\",\n" +
                "        \"Broker_dummy-broker-host3-datacenter1_6513\"\n" +
                "      ]\n" +
                "    }\n" +
                "  ],\n" +
                "  \"server\": [\n" +
                "    {\n" +
                "      \"tableType\": \"offline\",\n" +
                "      \"instances\": [\n" +
                "        \"Server_dummy-server-host8-datacenter1_7090\",\n" +
                "        \"Server_dummy-server-host9-datacenter1_7090\"\n" +
                "      ]\n" +
                "    },\n" +
                "    {\n" +
                "      \"tableType\": \"realtime\",\n" +
                "      \"instances\": [\n" +
                "        \"Server_dummy-server-host7-datacenter1_7090\",\n" +
                "        \"Server_dummy-server-host4-datacenter1_7090\",\n" +
                "        \"Server_dummy-server-host5-datacenter1_7090\",\n" +
                "        \"Server_dummy-server-host6-datacenter1_7090\"\n" +
                "      ]\n" +
                "    }\n" +
                "  ]\n" +
                "}");
        });
        PinotConfig pinotConfig = new PinotConfig()
                .setMetadataCacheExpiry(new Duration(1, TimeUnit.MILLISECONDS))
                .setControllerUrls("localhost:7900")
                .setExtraHttpHeaders("k1:v1,k2:v2,k3:some random v3");
        PinotClient pinotClient = new PinotClient(
                pinotConfig,
                new IdentityPinotHostMapper(),
                httpClient,
                newCachedThreadPool(threadsNamed("pinot-metadata-fetcher-testing")),
                MetadataUtil.TABLES_JSON_CODEC,
                MetadataUtil.BROKERS_FOR_TABLE_JSON_CODEC,
                MetadataUtil.TIME_BOUNDARY_JSON_CODEC,
                MetadataUtil.BROKER_RESPONSE_NATIVE_JSON_CODEC,
                PinotControllerAuthenticationProvider.create(PinotEmptyAuthenticationProvider.instance()),
                PinotBrokerAuthenticationProvider.create(PinotEmptyAuthenticationProvider.instance()));
        ImmutableSet<String> brokers = ImmutableSet.copyOf(pinotClient.getAllBrokersForTable("dummy"));
        assertThat(ImmutableSet.of("dummy-broker-host1-datacenter1:6513", "dummy-broker-host2-datacenter1:6513", "dummy-broker-host3-datacenter1:6513", "dummy-broker-host4-datacenter1:6513")).isEqualTo(brokers);
    }

    @Test
    public void testBuildAdditionalHeaders()
    {
        Map<String, String> extraHttpHeaders = ImmutableMap.of("k1", "v1", "k2", "value with space");
        PinotAuthenticationProvider authProvider = new PinotPasswordAuthenticationProvider("user", "password");
        ImmutableMultimap<String, String> headers = PinotClient.buildAdditionalHeaders(authProvider, extraHttpHeaders).build();
        assertThat(headers.get("k1").iterator().next()).isEqualTo("v1");
        assertThat(headers.get("k2").iterator().next()).isEqualTo("value with space");
        assertThat(headers.get(HttpHeaders.AUTHORIZATION).iterator().next()).isEqualTo("Basic dXNlcjpwYXNzd29yZA==");
    }

    @Test(expectedExceptions = PinotException.class)
    @SuppressWarnings("CheckReturnValue")
    public void testBuildAdditionalHeadersWithAuthorizationException()
    {
        Map<String, String> extraHttpHeaders = ImmutableMap.of("k1", "v1", HttpHeaders.AUTHORIZATION, "some random auth");
        PinotAuthenticationProvider authProvider = new PinotPasswordAuthenticationProvider("user", "password");
        PinotClient.buildAdditionalHeaders(authProvider, extraHttpHeaders).build();
    }
}
