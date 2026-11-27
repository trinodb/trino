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

import io.trino.plugin.pinot.client.InstanceInfo;
import org.apache.pinot.core.transport.ServerInstance;
import org.junit.jupiter.api.Test;

import static io.trino.plugin.pinot.MetadataUtil.INSTANCE_INFO_JSON_CODEC;
import static org.assertj.core.api.Assertions.assertThat;

public class TestInstanceInfo
{
    @Test
    public void testAutoJoinedInstance()
    {
        // Pinot prefixes the hostname of auto joined instances with "Server_"
        ServerInstance serverInstance = toServerInstance(new InstanceInfo(
                "Server_pinot-server-0.pinot-server-headless.pinot.svc.cluster.local_8098",
                "Server_pinot-server-0.pinot-server-headless.pinot.svc.cluster.local",
                8098,
                8091));
        assertThat(serverInstance.getHostname()).isEqualTo("pinot-server-0.pinot-server-headless.pinot.svc.cluster.local");
        assertThat(serverInstance.getPort()).isEqualTo(8098);
        assertThat(serverInstance.getGrpcPort()).isEqualTo(8091);
    }

    @Test
    public void testInstanceIdIsNotAHostname()
    {
        // An operator may set pinot.server.instance.id to any string. The instance id then carries no host at
        // all, and only the instance configuration says where the server actually is.
        ServerInstance serverInstance = toServerInstance(new InstanceInfo("some-logical-server-name", "pinot-server-7.example.com", 8098, 8091));
        assertThat(serverInstance.getHostname()).isEqualTo("pinot-server-7.example.com");
        assertThat(serverInstance.getPort()).isEqualTo(8098);
    }

    @Test
    public void testHostnameContainingUnderscores()
    {
        ServerInstance serverInstance = toServerInstance(new InstanceInfo("Server_host_with_underscores_8098", "Server_host_with_underscores", 8098, 8091));
        assertThat(serverInstance.getHostname()).isEqualTo("host_with_underscores");
    }

    @Test
    public void testInstanceIdWithoutPortSuffix()
    {
        // Parsing this id for a port yields nothing, but the instance configuration reports both
        ServerInstance serverInstance = toServerInstance(new InstanceInfo("Server_localhost", "Server_localhost", 8098, 8091));
        assertThat(serverInstance.getHostname()).isEqualTo("localhost");
        assertThat(serverInstance.getPort()).isEqualTo(8098);
    }

    @Test
    public void testJsonDeserializationIgnoresUnusedControllerFields()
    {
        String controllerResponse =
                """
                {
                  "instanceName": "Server_localhost_8098",
                  "hostName": "Server_localhost",
                  "enabled": true,
                  "port": "8098",
                  "tags": ["DefaultTenant_OFFLINE", "DefaultTenant_REALTIME"],
                  "pools": null,
                  "grpcPort": 8091,
                  "adminPort": 8097,
                  "queryServicePort": 8421,
                  "queryMailboxPort": 8422,
                  "shutdownInProgress": false,
                  "systemResourceInfo": {"numCores": "8", "totalMemoryMB": "16384", "maxHeapSizeMB": "4096"}
                }
                """;
        InstanceInfo instanceInfo = INSTANCE_INFO_JSON_CODEC.fromJson(controllerResponse);
        assertThat(instanceInfo).isEqualTo(new InstanceInfo("Server_localhost_8098", "Server_localhost", 8098, 8091));
        assertThat(toServerInstance(instanceInfo).getHostname()).isEqualTo("localhost");
    }

    private static ServerInstance toServerInstance(InstanceInfo instanceInfo)
    {
        // Mirrors what IdentityPinotHostMapper does with the instance configuration it fetches
        return new ServerInstance(instanceInfo.toInstanceConfig());
    }
}
