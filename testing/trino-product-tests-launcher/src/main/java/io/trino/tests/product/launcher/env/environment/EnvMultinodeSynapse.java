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
package io.trino.tests.product.launcher.env.environment;

import com.google.inject.Inject;
import io.trino.tests.product.launcher.docker.DockerFiles;
import io.trino.tests.product.launcher.env.DockerContainer;
import io.trino.tests.product.launcher.env.Environment;
import io.trino.tests.product.launcher.env.EnvironmentProvider;
import io.trino.tests.product.launcher.env.common.StandardMultinode;
import io.trino.tests.product.launcher.env.common.TestsEnvironment;

import static io.trino.tests.product.launcher.env.EnvironmentContainers.COORDINATOR;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.TESTS;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.WORKER;
import static io.trino.tests.product.launcher.env.common.Standard.CONTAINER_TRINO_ETC;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forHostPath;

/**
 * Adapted from SEP's Synapse environment.
 */
@TestsEnvironment
public class EnvMultinodeSynapse
        extends EnvironmentProvider
{
    private static final String SYNAPSE_DATABASE = "SQLPOOL2";
    private static final int SYNAPSE_PORT = 1433;

    private final DockerFiles.ResourceProvider configDir;

    @Inject
    protected EnvMultinodeSynapse(StandardMultinode standardMultinode, DockerFiles dockerFiles)
    {
        super(standardMultinode);
        this.configDir = dockerFiles.getDockerFilesHostDirectory("conf/environment/multinode-synapse");
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        builder.addConnector("synapse")
                .configureContainer(COORDINATOR, this::configureTrinoContainer)
                .configureContainer(WORKER, this::configureTrinoContainer)
                .configureContainer(TESTS, this::withAuthenticationEnvironmentVariables);
    }

    private DockerContainer configureTrinoContainer(DockerContainer container)
    {
        return withAuthenticationEnvironmentVariables(container)
                .withCopyFileToContainer(
                        forHostPath(configDir.getPath("synapse.properties")),
                        CONTAINER_TRINO_ETC + "/catalog/synapse.properties");
    }

    private static String getSynapseUrl()
    {
        String synapseEndpoint = requireNonNull(System.getenv("SYNAPSE_ENDPOINT"), "Expected SYNAPSE_ENDPOINT environment variable to be set");
        return String.format("jdbc:sqlserver://%s:%d;database=%s", synapseEndpoint, SYNAPSE_PORT, SYNAPSE_DATABASE);
    }

    private static String getSynapseUsername()
    {
        return requireNonNull(System.getenv("SYNAPSE_USER"), "Expected SYNAPSE_USER environment variable to be set");
    }

    private static String getSynapsePassword()
    {
        return requireNonNull(System.getenv("SYNAPSE_PASSWORD"), "Expected SYNAPSE_PASSWORD environment variable to be set");
    }

    private DockerContainer withAuthenticationEnvironmentVariables(DockerContainer container)
    {
        return container.withEnv("SYNAPSE_URL", getSynapseUrl())
                .withEnv("SYNAPSE_USER", getSynapseUsername())
                .withEnv("SYNAPSE_PASSWORD", getSynapsePassword());
    }
}
