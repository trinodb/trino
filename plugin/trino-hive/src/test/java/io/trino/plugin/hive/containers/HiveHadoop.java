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
import org.testcontainers.containers.Network;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.lang.String.format;

public class HiveHadoop
        extends BaseTestContainer
{
    private static final Logger log = Logger.get(HiveHadoop.class);

    public static final String DEFAULT_IMAGE = "ghcr.io/trinodb/testing/hdp2.6-hive:" + TestingProperties.getDockerImagesVersion();
    public static final String HIVE3_IMAGE = "ghcr.io/trinodb/testing/hdp3.1-hive:" + TestingProperties.getDockerImagesVersion();

    public static final String HOST_NAME = "hadoop-master";

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
        copyResourceToContainer("containers/hive_hadoop/hadoop-run.sh", runCmd);
        withRunCommand(
                ImmutableList.of(
                        "/bin/bash",
                        runCmd));
        withLogConsumer(new PrintingLogConsumer(format("%-20s| ", "hadoop")));
    }

    @Override
    public void start()
    {
        super.start();
        log.info("Hive container started with addresses for metastore: %s", getHiveMetastoreEndpoint());
    }

    public String runOnHive(String query)
    {
        return executeInContainerFailOnError("beeline", "-u", "jdbc:hive2://localhost:10000/default", "-n", "hive", "-e", query);
    }

    public String runOnMetastore(String query)
    {
        return executeInContainerFailOnError("mysql", "-D", "metastore", "-uroot", "-proot", "--batch", "--column-names=false", "-e", query).replaceAll("\n$", "");
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
            this.exposePorts = ImmutableSet.of(HIVE_METASTORE_PORT);
        }

        @Override
        public HiveHadoop build()
        {
            return new HiveHadoop(image, hostName, exposePorts, filesToMount, envVars, network, startupRetryLimit);
        }
    }
}
