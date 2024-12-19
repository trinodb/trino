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

import static io.trino.testing.TestingProperties.getDockerImagesVersion;

public class Hive4Metastore
        extends BaseTestContainer
{
    public static final String HIVE4_IMAGE = "ghcr.io/trinodb/testing/hive4.0-hive:" + getDockerImagesVersion();
    public static final int HIVE_METASTORE_PORT = 9083;

    private static final Logger log = Logger.get(HiveHadoop.class);
    private static final String HOST_NAME = "metastore";

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
            extends BaseTestContainer.Builder<Hive4Metastore.Builder, Hive4Metastore>
    {
        private Builder()
        {
            this.image = HIVE4_IMAGE;
            this.hostName = HOST_NAME;
            this.exposePorts = ImmutableSet.of(HIVE_METASTORE_PORT);
        }

        @Override
        public Hive4Metastore build()
        {
            return new Hive4Metastore(image, hostName, exposePorts, filesToMount, envVars, network, startupRetryLimit);
        }
    }

    private Hive4Metastore(
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
        log.info("Hive container started with addresses for metastore: %s", getHiveMetastoreEndpoint());
    }

    public URI getHiveMetastoreEndpoint()
    {
        HostAndPort address = getMappedHostAndPortForExposedPort(HIVE_METASTORE_PORT);
        return URI.create("thrift://" + address.getHost() + ":" + address.getPort());
    }

    public URI getInternalHiveMetastoreEndpoint()
    {
        return URI.create("thrift://" + HOST_NAME + ":" + HIVE_METASTORE_PORT);
    }
}
