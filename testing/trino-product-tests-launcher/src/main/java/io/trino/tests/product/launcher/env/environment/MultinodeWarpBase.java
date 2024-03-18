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

import io.trino.tests.product.launcher.docker.DockerFiles;
import io.trino.tests.product.launcher.docker.DockerFiles.ResourceProvider;
import io.trino.tests.product.launcher.env.DockerContainer;
import io.trino.tests.product.launcher.env.Environment;
import io.trino.tests.product.launcher.env.EnvironmentProvider;
import io.trino.tests.product.launcher.env.common.EnvironmentExtender;
import io.trino.tests.product.launcher.env.common.Standard;
import io.trino.tests.product.launcher.testcontainers.PortBinder;

import java.nio.file.Path;
import java.util.Map;

import static io.trino.tests.product.launcher.env.EnvironmentContainers.COORDINATOR;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.TESTS;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.WORKER;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forClasspathResource;
import static org.testcontainers.utility.MountableFile.forHostPath;

public abstract class MultinodeWarpBase
        extends EnvironmentProvider
{
    private static final String LOGS_PATH = Standard.CONTAINER_TRINO_ETC + "/log.properties";

    protected final ResourceProvider configDir;
    protected final PortBinder portBinder;

    public MultinodeWarpBase(String dockerFilesHostDirectory,
            DockerFiles dockerFiles,
            PortBinder portBinder,
            EnvironmentExtender... bases)
    {
        super(bases);
        this.portBinder = requireNonNull(portBinder, "portBinder is null");
        configDir = dockerFiles.getDockerFilesHostDirectory(dockerFilesHostDirectory);
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        builder.configureContainer(COORDINATOR, this::configureTrinoContainer);
        builder.configureContainer(COORDINATOR, this::configureWarpResource);
        builder.configureContainer(COORDINATOR, this::configureWarpStorage);

        builder.configureContainer(WORKER, this::configureTrinoContainer);
        builder.configureContainer(WORKER, this::configureWarpStorage);

        builder.configureContainer(TESTS, this::copyTestResources);
        builder.addConnector("warp_speed");
    }

    protected void copyTestResources(DockerContainer container)
    {
        container.withCopyFileToContainer(forHostPath(Path.of(".", "/docker/presto-product-tests/warp/synthetic.json"), 493),
                "/docker/synthetic.json");
    }

    protected void configureTrinoContainer(DockerContainer container)
    {
        container.withPrivilegedMode(true)
                .withEnv(Map.of("AWS_REGION", "us-east-1"))
                .withCopyFileToContainer(forClasspathResource("/docker/presto-product-tests/warp/trino/log.properties"), LOGS_PATH)
                .withCopyFileToContainer(forClasspathResource("/docker/presto-product-tests/warp/trino/jvm.config"), Standard.CONTAINER_TRINO_JVM_CONFIG)
                .withCopyFileToContainer(forHostPath(configDir.getPath("trino/etc")), Standard.CONTAINER_TRINO_ETC);
    }

    private void configureWarpResource(DockerContainer container)
    {
        portBinder.exposePort(container, 8089);
    }

    private void configureWarpStorage(DockerContainer container)
    {
        container.withCopyFileToContainer(forClasspathResource("/docker/presto-product-tests/warp/trino/setup-warp-speed.sh", 493),
                "/docker/presto-init.d/setup-warp-speed.sh");
    }

    protected static String requireEnv(String variable)
    {
        return requireNonNull(System.getenv(variable), "environment variable not set: " + variable);
    }
}
