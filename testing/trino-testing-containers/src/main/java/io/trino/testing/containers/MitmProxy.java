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
package io.trino.testing.containers;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.net.HostAndPort;
import io.airlift.log.Logger;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.OutputFrame;

import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class MitmProxy
        extends BaseTestContainer
{
    private static final Logger log = Logger.get(MitmProxy.class);

    public static final String DEFAULT_IMAGE = "mitmproxy/mitmproxy:9.0.1";
    public static final String DEFAULT_HOST_NAME = "mitmproxy";

    public static final int MITMPROXY_PORT = 6660;

    public static Builder builder()
    {
        return new Builder();
    }

    private MitmProxy(
            String image,
            String hostName,
            Set<Integer> exposePorts,
            Map<String, String> filesToMount,
            Map<String, String> envVars,
            Optional<Network> network,
            int retryLimit)
    {
        super(
                image,
                hostName,
                exposePorts,
                filesToMount,
                envVars,
                network,
                retryLimit);
    }

    @Override
    protected void setupContainer()
    {
        super.setupContainer();
        withRunCommand(
                ImmutableList.of(
                        "mitmdump",
                        "--listen-port", Integer.toString(MITMPROXY_PORT),
                        "--certs", "/tmp/cert.pem",
                        "--set", "proxy_debug=true",
                        "--set", "stream_large_bodies=0"));

        withLogConsumer(MitmProxy::printProxiedRequest);
    }

    private static void printProxiedRequest(OutputFrame outputFrame)
    {
        String line = outputFrame.getUtf8String().trim();
        if (!line.startsWith("<<")) {
            log.info("Proxied " + line);
        }
    }

    @Override
    public void start()
    {
        super.start();
        log.info("Mitm proxy container started with address: " + getProxyEndpoint());
    }

    public HostAndPort getProxyHostAndPort()
    {
        return getMappedHostAndPortForExposedPort(MITMPROXY_PORT);
    }

    public String getProxyEndpoint()
    {
        return "https://" + getProxyHostAndPort();
    }

    public static class Builder
            extends BaseTestContainer.Builder<MitmProxy.Builder, MitmProxy>
    {
        private Builder()
        {
            this.image = DEFAULT_IMAGE;
            this.hostName = DEFAULT_HOST_NAME;
            this.exposePorts = ImmutableSet.of(MITMPROXY_PORT);
            this.envVars = ImmutableMap.of();
        }

        public Builder withSSLCertificate(Path filename)
        {
            return withFilesToMount(Map.of("/tmp/cert.pem", filename.toString()));
        }

        @Override
        public MitmProxy build()
        {
            return new MitmProxy(image, hostName, exposePorts, filesToMount, envVars, network, startupRetryLimit);
        }
    }
}
