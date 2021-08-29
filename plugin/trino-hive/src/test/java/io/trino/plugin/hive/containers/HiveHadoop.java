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
import io.airlift.log.Logger;
import io.trino.testing.TestingProperties;
import io.trino.testing.containers.BaseTestContainer;
import io.trino.testing.containers.wait.strategy.SelectedPortWaitStrategy;
import org.apache.hadoop.net.NetUtils;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.WaitStrategy;

import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class HiveHadoop
        extends BaseTestContainer
{
    private static final Logger log = Logger.get(HiveHadoop.class);

    public static final String DEFAULT_IMAGE = "ghcr.io/trinodb/testing/hdp2.6-hive:" + TestingProperties.getDockerImagesVersion();
    public static final String HIVE3_IMAGE = "ghcr.io/trinodb/testing/hdp3.1-hive:" + TestingProperties.getDockerImagesVersion();

    public static final String HOST_NAME = "hadoop-master";

    public static final int HIVE_METASTORE_PORT = 9083;
    public static final int HDFS_FS_PORT = 9000;
    public static final int SOCKS_PROXY_PORT = 1180;

    private final String hostAddress;

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
            Optional<WaitStrategy> waitStrategy,
            int startupRetryLimit)
    {
        super(
                image,
                hostName,
                ports,
                filesToMount,
                envVars,
                network,
                waitStrategy,
                startupRetryLimit);
        try {
            hostAddress = InetAddress.getLocalHost().getHostAddress();
        }
        catch (UnknownHostException e) {
            throw new UncheckedIOException(e);
        }
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

    @Override
    public void start()
    {
        super.start();

        // Even though Hadoop is accessed by proxy, Hadoop still tries to resolve hadoop-master
        // (e.g: in: NameNodeProxies.createProxy)
        // This adds a static resolution for hadoop-master to docker container internal ip
        NetUtils.addStaticResolution(HOST_NAME, getContainerIpAddress());

        log.info("Hive container started with addresses for metastore: %s", getHiveMetastoreEndpoint());
    }

    public HostAndPort getHiveMetastoreEndpoint()
    {
        return HostAndPort.fromParts(hostAddress, getMappedPortForExposedPort(HIVE_METASTORE_PORT));
    }

    public HostAndPort getSocksProxyEndpoint()
    {
        return HostAndPort.fromParts(hostAddress, getMappedPortForExposedPort(SOCKS_PROXY_PORT));
    }

    public static class Builder
            extends BaseTestContainer.Builder<HiveHadoop.Builder, HiveHadoop>
    {
        private Builder()
        {
            this.image = DEFAULT_IMAGE;
            this.hostName = HOST_NAME;
            this.exposePorts = ImmutableSet.of(HIVE_METASTORE_PORT, HDFS_FS_PORT, SOCKS_PROXY_PORT);
            this.waitStrategy = Optional.of(new SelectedPortWaitStrategy(exposePorts)); // The default wait strategy sometimes causes "Malformed reply from SOCKS server"
        }

        @Override
        public HiveHadoop build()
        {
            return new HiveHadoop(image, hostName, exposePorts, filesToMount, envVars, network, waitStrategy, startupRetryLimit);
        }
    }
}
