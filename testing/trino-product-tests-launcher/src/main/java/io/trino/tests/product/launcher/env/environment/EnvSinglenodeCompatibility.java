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

import com.google.common.collect.ImmutableList;
import io.trino.tests.product.launcher.docker.DockerFiles;
import io.trino.tests.product.launcher.env.DockerContainer;
import io.trino.tests.product.launcher.env.Environment;
import io.trino.tests.product.launcher.env.EnvironmentProvider;
import io.trino.tests.product.launcher.env.common.Hadoop;
import io.trino.tests.product.launcher.env.common.Standard;
import io.trino.tests.product.launcher.env.common.TestsEnvironment;
import io.trino.tests.product.launcher.testcontainers.PortBinder;
import org.testcontainers.containers.startupcheck.IsRunningStartupCheckStrategy;
import org.testcontainers.utility.DockerImageName;

import javax.inject.Inject;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

import static io.trino.tests.product.launcher.env.EnvironmentContainers.TESTS;
import static java.lang.Integer.parseInt;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.containers.wait.strategy.Wait.forHealthcheck;
import static org.testcontainers.containers.wait.strategy.Wait.forLogMessage;
import static org.testcontainers.utility.MountableFile.forHostPath;

@TestsEnvironment
public class EnvSinglenodeCompatibility
        extends EnvironmentProvider
{
    private static final int SERVER_PORT = 8081;
    private static final String COMPATIBILTY_TEST_CONTAINER_NAME = "compatibility-test-coordinator";

    private final DockerFiles dockerFiles;
    private final DockerFiles.ResourceProvider configDir;
    private final PortBinder portBinder;

    private Config extraConfig;

    @Inject
    public EnvSinglenodeCompatibility(Standard standard, Hadoop hadoop, DockerFiles dockerFiles, PortBinder portBinder)
    {
        super(ImmutableList.of(standard, hadoop));
        this.dockerFiles = dockerFiles;
        this.configDir = dockerFiles.getDockerFilesHostDirectory("conf/environment/singlenode-compatibility");
        this.portBinder = portBinder;
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        configureCompatibilityTestContainer(builder, extraConfig);
        configureTestsContainer(builder, extraConfig);
    }

    private void configureCompatibilityTestContainer(Environment.Builder builder, Config config)
    {
        String containerConfigDir = getConfigurationDirectory(config.getCompatibilityTestDockerImage());
        DockerContainer container = new DockerContainer(config.getCompatibilityTestDockerImage(), COMPATIBILTY_TEST_CONTAINER_NAME)
                .withExposedLogPaths("/var/trino/var/log", "/var/log/container-health.log")
                .withCopyFileToContainer(forHostPath(dockerFiles.getDockerFilesHostPath("conf/presto/etc/jvm.config")), containerConfigDir + "jvm.config")
                .withCopyFileToContainer(forHostPath(configDir.getPath("config.properties")), containerConfigDir + "config.properties")
                .withCopyFileToContainer(forHostPath(configDir.getPath("hive.properties")), containerConfigDir + "catalog/hive.properties")
                .withCopyFileToContainer(forHostPath(dockerFiles.getDockerFilesHostPath()), "/docker/presto-product-tests")
                .withStartupCheckStrategy(new IsRunningStartupCheckStrategy())
                .waitingForAll(forLogMessage(".*======== SERVER STARTED ========.*", 1), forHealthcheck())
                .withStartupTimeout(Duration.ofMinutes(5));
        builder.addContainer(container);
        portBinder.exposePort(container, SERVER_PORT);
    }

    protected String getConfigurationDirectory(String dockerImageName)
    {
        try {
            int version = getVersionFromDockerImageName(dockerImageName);
            if (version <= 350) {
                return "/usr/lib/presto/default/etc/";
            }
            if (version == 351) {
                // 351 has the Trino configuration at a different location
                return "/usr/lib/trino/default/etc/";
            }
            return "/etc/trino/";
        }
        catch (NumberFormatException e) {
            throw new RuntimeException("Failed to parse version from docker image name " + dockerImageName);
        }
    }

    private void configureTestsContainer(Environment.Builder builder, Config config)
    {
        int version = getVersionFromDockerImageName(config.getCompatibilityTestDockerImage());
        String temptoConfig = version <= 350 ? "presto-tempto-configuration.yaml" : "trino-tempto-configuration.yaml";
        builder.configureContainer(TESTS, container -> container
                .withCopyFileToContainer(
                        forHostPath(configDir.getPath(temptoConfig)),
                        "/docker/presto-product-tests/conf/tempto/tempto-configuration-profile-config-file.yaml"));
    }

    protected int getVersionFromDockerImageName(String dockerImageName)
    {
        return parseInt(DockerImageName.parse(dockerImageName).getVersionPart());
    }

    @Override
    public Optional<String> getExtraOptionsPrefix()
    {
        return Optional.of("compatibility.");
    }

    @Override
    public void setExtraOptions(Map<String, String> extraOptions)
    {
        extraConfig = new Config(extraOptions);
    }

    public static class Config
    {
        private static final String TEST_DOCKER_IMAGE = "testDockerImage";
        private final String compatibilityTestDockerImage;

        public Config(Map<String, String> extraOptions)
        {
            this.compatibilityTestDockerImage = requireNonNull(extraOptions.get(TEST_DOCKER_IMAGE), "Required extra option " + TEST_DOCKER_IMAGE + " is null");
        }

        public String getCompatibilityTestDockerImage()
        {
            return compatibilityTestDockerImage;
        }
    }
}
