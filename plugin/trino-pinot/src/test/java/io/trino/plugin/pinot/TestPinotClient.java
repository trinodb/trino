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

import com.google.common.collect.ImmutableList;
import com.google.common.net.MediaType;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.testing.TestingHttpClient;
import io.airlift.http.client.testing.TestingResponse;
import io.airlift.units.Duration;
import io.trino.plugin.pinot.auth.PinotBrokerAuthenticationProvider;
import io.trino.plugin.pinot.auth.PinotControllerAuthenticationProvider;
import io.trino.plugin.pinot.auth.none.PinotEmptyAuthenticationProvider;
import io.trino.plugin.pinot.client.IdentityPinotHostMapper;
import io.trino.plugin.pinot.client.InstanceInfo;
import io.trino.plugin.pinot.client.PinotClient;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static io.airlift.concurrent.Threads.threadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestPinotClient
{
    @Test
    public void testBrokersParsed()
    {
        HttpClient httpClient = new TestingHttpClient(_ -> TestingResponse.mockResponse(HttpStatus.OK, MediaType.JSON_UTF_8, "{\n" +
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
                "}"));
        PinotConfig pinotConfig = new PinotConfig()
                .setMetadataCacheExpiry(new Duration(1, TimeUnit.MILLISECONDS))
                .setControllerUrls(ImmutableList.of("localhost:7900"));
        AtomicReference<PinotClient> clientReference = new AtomicReference<>();
        PinotClient pinotClient = new PinotClient(
                pinotConfig,
                new IdentityPinotHostMapper(clientReference::get),
                httpClient,
                newCachedThreadPool(threadsNamed("pinot-metadata-fetcher-testing")),
                MetadataUtil.TABLES_JSON_CODEC,
                MetadataUtil.BROKERS_FOR_TABLE_JSON_CODEC,
                MetadataUtil.TIME_BOUNDARY_JSON_CODEC,
                MetadataUtil.BROKER_RESPONSE_NATIVE_JSON_CODEC,
                MetadataUtil.INSTANCE_INFO_JSON_CODEC,
                PinotControllerAuthenticationProvider.create(PinotEmptyAuthenticationProvider.instance()),
                PinotBrokerAuthenticationProvider.create(PinotEmptyAuthenticationProvider.instance()));
        clientReference.set(pinotClient);
        assertThat(pinotClient.getAllBrokersForTable("dummy"))
                .containsExactlyInAnyOrder(
                        "dummy-broker-host1-datacenter1:6513",
                        "dummy-broker-host2-datacenter1:6513",
                        "dummy-broker-host3-datacenter1:6513",
                        "dummy-broker-host4-datacenter1:6513");
    }

    @Test
    public void testInstanceInfoParsed()
    {
        HttpClient httpClient = new TestingHttpClient(_ -> TestingResponse.mockResponse(HttpStatus.OK, MediaType.JSON_UTF_8,
                """
                {
                  "instanceName": "Server_dummy-server-host1-datacenter1_8098",
                  "hostName": "Server_dummy-server-host1-datacenter1",
                  "enabled": true,
                  "port": "8098",
                  "tags": ["DefaultTenant_OFFLINE"],
                  "grpcPort": 8091,
                  "adminPort": 8097
                }
                """));
        assertThat(createPinotClient(httpClient).getInstanceInfo("Server_dummy-server-host1-datacenter1_8098"))
                .isEqualTo(new InstanceInfo("Server_dummy-server-host1-datacenter1_8098", "Server_dummy-server-host1-datacenter1", 8098, 8091));
    }

    @Test
    public void testInstanceInfoLookupFailureIsNotWrapped()
    {
        // The cache loader throws an unchecked PinotException, which Guava wraps before it reaches the caller
        HttpClient httpClient = new TestingHttpClient(_ -> TestingResponse.mockResponse(HttpStatus.NOT_FOUND, MediaType.JSON_UTF_8, "{}"));
        assertThatThrownBy(() -> createPinotClient(httpClient).getInstanceInfo("Server_missing_8098"))
                .isInstanceOf(PinotException.class);
    }

    private static PinotClient createPinotClient(HttpClient httpClient)
    {
        PinotConfig pinotConfig = new PinotConfig()
                .setMetadataCacheExpiry(new Duration(1, TimeUnit.MILLISECONDS))
                .setControllerUrls(ImmutableList.of("localhost:7900"));
        AtomicReference<PinotClient> clientReference = new AtomicReference<>();
        PinotClient pinotClient = new PinotClient(
                pinotConfig,
                new IdentityPinotHostMapper(clientReference::get),
                httpClient,
                newCachedThreadPool(threadsNamed("pinot-metadata-fetcher-testing")),
                MetadataUtil.TABLES_JSON_CODEC,
                MetadataUtil.BROKERS_FOR_TABLE_JSON_CODEC,
                MetadataUtil.TIME_BOUNDARY_JSON_CODEC,
                MetadataUtil.BROKER_RESPONSE_NATIVE_JSON_CODEC,
                MetadataUtil.INSTANCE_INFO_JSON_CODEC,
                PinotControllerAuthenticationProvider.create(PinotEmptyAuthenticationProvider.instance()),
                PinotBrokerAuthenticationProvider.create(PinotEmptyAuthenticationProvider.instance()));
        clientReference.set(pinotClient);
        return pinotClient;
    }
}
