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

import com.google.common.net.HostAndPort;
import io.trino.plugin.pinot.client.PinotHostMapper;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.core.transport.ServerInstance;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class TestingPinotHostMapper
        implements PinotHostMapper
{
    private final HostAndPort brokerHostAndPort;
    private final HostAndPort serverHostAndPort;
    private final HostAndPort serverGrpcHostAndPort;

    public TestingPinotHostMapper(HostAndPort brokerHostAndPort, HostAndPort serverHostAndPort, HostAndPort serverGrpcHostAndPort)
    {
        this.brokerHostAndPort = requireNonNull(brokerHostAndPort, "brokerHostAndPort is null");
        this.serverHostAndPort = requireNonNull(serverHostAndPort, "serverHostAndPort is null");
        this.serverGrpcHostAndPort = requireNonNull(serverGrpcHostAndPort, "serverGrpcHostAndPort is null");
    }

    @Override
    public String getBrokerHost(String host, String port)
    {
        return format("%s:%s", brokerHostAndPort.getHost(), brokerHostAndPort.getPort());
    }

    @Override
    public ServerInstance getServerInstance(String serverHost)
    {
        InstanceConfig instanceConfig = InstanceConfig.toInstanceConfig(serverHost);
        instanceConfig.setHostName(serverHostAndPort.getHost());
        instanceConfig.setPort(String.valueOf(serverHostAndPort.getPort()));
        return new ServerInstance(instanceConfig);
    }

    @Override
    public HostAndPort getServerGrpcHostAndPort(String serverHost, int grpcPort)
    {
        return serverGrpcHostAndPort;
    }
}
