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
package io.trino.plugin.hive.containers;

import com.google.common.collect.ImmutableSet;
import com.google.common.net.HostAndPort;
import io.airlift.log.Logger;
import io.trino.testing.containers.BaseTestContainer;
import org.testcontainers.containers.Network;

import java.net.URI;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.trino.plugin.hive.containers.Hive4Metastore.HIVE4_IMAGE;

public class Hive4HiveServer
        extends BaseTestContainer
{
    public static final int HIVE_SERVER_PORT = 10000;

    private static final Logger log = Logger.get(Hive4HiveServer.class);
    private static final String HOST_NAME = "hiveserver2";

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
            extends BaseTestContainer.Builder<Hive4HiveServer.Builder, Hive4HiveServer>
    {
        private Builder()
        {
            this.image = HIVE4_IMAGE;
            this.hostName = HOST_NAME;
            this.exposePorts = ImmutableSet.of(HIVE_SERVER_PORT);
        }

        @Override
        public Hive4HiveServer build()
        {
            return new Hive4HiveServer(image, hostName, exposePorts, filesToMount, envVars, network, startupRetryLimit);
        }
    }

    private Hive4HiveServer(
            String image,
            String hostName,
            Set<Integer> ports,
            Map<String, String> filesToMount,
            Map<String, String> envVars,
            Optional<Network> network,
            int startupRetryLimit)
    {
        super(
                image,
                hostName,
                ports,
                filesToMount,
                envVars,
                network,
                startupRetryLimit);
    }

    @Override
    public void start()
    {
        super.start();
        log.info("Hive container started with addresses for hive server: %s", getHiveServerEndpoint());
    }

    public String runOnHive(String query)
    {
        return executeInContainerFailOnError("beeline", "-u", "jdbc:hive2://localhost:%s/default".formatted(HIVE_SERVER_PORT), "-n", "hive", "-e", query);
    }

    public URI getHiveServerEndpoint()
    {
        HostAndPort address = getMappedHostAndPortForExposedPort(HIVE_SERVER_PORT);
        return URI.create(address.getHost() + ":" + address.getPort());
    }
}
