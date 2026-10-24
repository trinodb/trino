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
package io.trino.plugin.pinot.client;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.helix.model.InstanceConfig;

import static java.util.Objects.requireNonNull;
import static org.apache.pinot.spi.utils.CommonConstants.Helix.Instance.GRPC_PORT_KEY;

/**
 * The instance config of a single Pinot instance, as returned by the controller's
 * {@code /instances/{instanceName}} endpoint.
 * <p>
 * A Pinot instance id is only a name. It defaults to {@code Server_<host>_<port>}, but an operator may set
 * {@code pinot.server.instance.id} to an arbitrary string, in which case it does not encode a resolvable host
 * at all. The {@code hostName} and {@code grpcPort} of the instance config are the authoritative network
 * coordinates, so they are what the connector must use to reach a server.
 * <p>
 * This type is immutable and thread safe.
 */
public record InstanceInfo(
        @JsonProperty("instanceName") String instanceName,
        @JsonProperty("hostName") String hostName,
        @JsonProperty("port") int port,
        @JsonProperty("grpcPort") int grpcPort)
{
    public InstanceInfo
    {
        requireNonNull(instanceName, "instanceName is null");
        requireNonNull(hostName, "hostName is null");
    }

    /**
     * Rebuilds the Helix instance config this was serialized from, so that it can be handed to ServerInstance,
     * which knows how to strip the {@code Server_} prefix that Pinot adds to auto joined hostnames.
     */
    public InstanceConfig toInstanceConfig()
    {
        InstanceConfig instanceConfig = new InstanceConfig(instanceName);
        instanceConfig.setHostName(hostName);
        instanceConfig.setPort(String.valueOf(port));
        instanceConfig.getRecord().setIntField(GRPC_PORT_KEY, grpcPort);
        return instanceConfig;
    }
}
