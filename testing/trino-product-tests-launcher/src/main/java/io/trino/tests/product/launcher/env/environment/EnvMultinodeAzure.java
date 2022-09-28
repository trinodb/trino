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
import io.trino.tests.product.launcher.env.Environment;
import io.trino.tests.product.launcher.env.EnvironmentConfig;
import io.trino.tests.product.launcher.env.EnvironmentProvider;
import io.trino.tests.product.launcher.env.common.Hadoop;
import io.trino.tests.product.launcher.env.common.StandardMultinode;
import io.trino.tests.product.launcher.env.common.TestsEnvironment;

import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;

import static io.trino.tests.product.launcher.env.EnvironmentContainers.COORDINATOR;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.HADOOP;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.TESTS;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.WORKER;
import static io.trino.tests.product.launcher.env.common.Hadoop.CONTAINER_HADOOP_INIT_D;
import static java.nio.file.attribute.PosixFilePermissions.fromString;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forHostPath;

@TestsEnvironment
public class EnvMultinodeAzure
        extends EnvironmentProvider
{
    private final DockerFiles dockerFiles;
    private final DockerFiles.ResourceProvider configDir;
    private final String hadoopBaseImage;
    private final String hadoopImagesVersion;

    @Inject
    public EnvMultinodeAzure(DockerFiles dockerFiles, StandardMultinode standardMultinode, Hadoop hadoop, EnvironmentConfig environmentConfig)
    {
        super(ImmutableList.of(standardMultinode, hadoop));
        this.dockerFiles = requireNonNull(dockerFiles, "dockerFiles is null");
        configDir = dockerFiles.getDockerFilesHostDirectory("conf/environment/multinode-azure");
        requireNonNull(environmentConfig, "environmentConfig is null");
        hadoopBaseImage = environmentConfig.getHadoopBaseImage();
        hadoopImagesVersion = environmentConfig.getHadoopImagesVersion();
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        String dockerImageName = hadoopBaseImage + ":" + hadoopImagesVersion;

        builder.configureContainer(HADOOP, container -> {
            container.setDockerImageName(dockerImageName);
            container.withCopyFileToContainer(
                    forHostPath(getCoreSiteOverrideXml()),
                    "/docker/presto-product-tests/conf/environment/multinode-azure/core-site-overrides.xml");
            container.withCopyFileToContainer(
                    forHostPath(dockerFiles.getDockerFilesHostPath("conf/environment/multinode-azure/apply-azure-config.sh")),
                    CONTAINER_HADOOP_INIT_D + "apply-azure-config.sh");
        });

        builder.configureContainer(COORDINATOR, container -> container
                .withEnv("ABFS_ACCOUNT", requireEnv("ABFS_ACCOUNT"))
                .withEnv("ABFS_ACCESS_KEY", requireEnv("ABFS_ACCESS_KEY")));

        builder.configureContainer(WORKER, container -> container
                .withEnv("ABFS_ACCOUNT", requireEnv("ABFS_ACCOUNT"))
                .withEnv("ABFS_ACCESS_KEY", requireEnv("ABFS_ACCESS_KEY")));

        builder.configureContainer(TESTS, container -> container
                .withEnv("ABFS_CONTAINER", requireEnv("ABFS_CONTAINER"))
                .withEnv("ABFS_ACCOUNT", requireEnv("ABFS_ACCOUNT")));

        builder.addConnector("hive", forHostPath(configDir.getPath("hive.properties")));
    }

    private Path getCoreSiteOverrideXml()
    {
        try {
            String coreSite = Files.readString(configDir.getPath("core-site-overrides-template.xml"))
                    .replace("%ABFS_ACCOUNT%", requireEnv("ABFS_ACCOUNT"))
                    .replace("%ABFS_ACCESS_KEY%", requireEnv("ABFS_ACCESS_KEY"));
            File coreSiteXml = Files.createTempFile("core-site", ".xml", PosixFilePermissions.asFileAttribute(fromString("rwxrwxrwx"))).toFile();
            coreSiteXml.deleteOnExit();
            Files.writeString(coreSiteXml.toPath(), coreSite);
            return coreSiteXml.toPath();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static String requireEnv(String variable)
    {
        return requireNonNull(System.getenv(variable), () -> "environment variable not set: " + variable);
    }
}
