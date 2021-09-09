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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.net.HostAndPort;
import io.trino.testing.containers.BaseTestContainer;
import org.testcontainers.containers.Network;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class HiveHadoop
        extends BaseTestContainer
{
    private static final String IMAGE_VERSION = "43";
    public static final String DEFAULT_IMAGE = "ghcr.io/trinodb/testing/hdp2.6-hive:" + IMAGE_VERSION;
    public static final String HIVE3_IMAGE = "ghcr.io/trinodb/testing/hdp3.1-hive:" + IMAGE_VERSION;

    public static final String HOST_NAME = "hadoop-master";

    public static final int PROXY_PORT = 1180;
    public static final int HIVE_SERVER_PORT = 10000;
    public static final int HIVE_METASTORE_PORT = 9083;

    public static Builder builder()
    {
        return new Builder();
    }

    private HiveHadoop(
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
    protected void setupContainer()
    {
        super.setupContainer();
        String runCmd = "/usr/local/hadoop-run.sh";
        copyFileToContainer("containers/hive_hadoop/hadoop-run.sh", runCmd);
        withRunCommand(
                ImmutableList.of(
                        "/bin/bash",
                        runCmd));
    }

    public HostAndPort getMappedHdfsSocksProxy()
    {
        return getMappedHostAndPortForExposedPort(PROXY_PORT);
    }

    public HostAndPort getHiveServerEndpoint()
    {
        return getMappedHostAndPortForExposedPort(HIVE_SERVER_PORT);
    }

    public HostAndPort getHiveMetastoreEndpoint()
    {
        return getMappedHostAndPortForExposedPort(HIVE_METASTORE_PORT);
    }

    public static class Builder
            extends BaseTestContainer.Builder<HiveHadoop.Builder, HiveHadoop>
    {
        private Builder()
        {
            this.image = DEFAULT_IMAGE;
            this.hostName = HOST_NAME;
            this.exposePorts =
                    ImmutableSet.of(
                            HIVE_METASTORE_PORT,
                            PROXY_PORT);
        }

        @Override
        public HiveHadoop build()
        {
            return new HiveHadoop(image, hostName, exposePorts, filesToMount, envVars, network, startupRetryLimit);
        }
    }
}
