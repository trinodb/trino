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

import com.google.common.net.HostAndPort;
import com.google.inject.Inject;
import com.google.inject.Provider;
import org.apache.pinot.core.transport.ServerInstance;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class IdentityPinotHostMapper
        implements PinotHostMapper
{
    // A Provider breaks the dependency cycle with PinotClient, which needs a PinotHostMapper of its own to
    // resolve brokers.
    private final Provider<PinotClient> pinotClient;

    @Inject
    public IdentityPinotHostMapper(Provider<PinotClient> pinotClient)
    {
        this.pinotClient = requireNonNull(pinotClient, "pinotClient is null");
    }

    @Override
    public String getBrokerHost(String host, String port)
    {
        return format("%s:%s", host, port);
    }

    /**
     * Resolves a server instance id to the instance it names.
     * <p>
     * The instance id is only a name: it defaults to {@code Server_<host>_<port>}, but an operator may set
     * {@code pinot.server.instance.id} to an arbitrary string, in which case it does not contain a resolvable
     * host. The instance configuration held by the controller is the authoritative source for that, so the
     * host is taken from there rather than parsed out of the id.
     */
    @Override
    public ServerInstance getServerInstance(String serverInstanceId)
    {
        return new ServerInstance(pinotClient.get().getInstanceInfo(serverInstanceId).toInstanceConfig());
    }

    @Override
    public HostAndPort getServerGrpcHostAndPort(String serverInstanceId, int grpcPort)
    {
        // ServerInstance strips the "Server_" prefix that Pinot adds to the hostname of auto joined instances
        return HostAndPort.fromParts(getServerInstance(serverInstanceId).getHostname(), grpcPort);
    }
}
