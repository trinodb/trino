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
package io.prestosql.tests.product.launcher.env.environment;

import com.google.common.collect.ImmutableList;
import io.prestosql.tests.product.launcher.PathResolver;
import io.prestosql.tests.product.launcher.docker.DockerFiles;
import io.prestosql.tests.product.launcher.env.DockerContainer;
import io.prestosql.tests.product.launcher.env.Environment;
import io.prestosql.tests.product.launcher.env.EnvironmentOptions;
import io.prestosql.tests.product.launcher.env.common.AbstractEnvironmentProvider;
import io.prestosql.tests.product.launcher.env.common.Hadoop;
import io.prestosql.tests.product.launcher.env.common.Standard;
import io.prestosql.tests.product.launcher.env.common.TestsEnvironment;

import javax.inject.Inject;

import java.io.File;

import static io.prestosql.tests.product.launcher.env.common.Hadoop.CONTAINER_PRESTO_HIVE_PROPERTIES;
import static io.prestosql.tests.product.launcher.env.common.Hadoop.CONTAINER_PRESTO_ICEBERG_PROPERTIES;
import static io.prestosql.tests.product.launcher.env.common.Standard.CONTAINER_PRESTO_CONFIG_PROPERTIES;
import static io.prestosql.tests.product.launcher.env.common.Standard.CONTAINER_PRESTO_JVM_CONFIG;
import static io.prestosql.tests.product.launcher.env.common.Standard.createPrestoContainer;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.containers.BindMode.READ_ONLY;

@TestsEnvironment
public final class Multinode
        extends AbstractEnvironmentProvider
{
    private final PathResolver pathResolver;
    private final DockerFiles dockerFiles;

    private final String imagesVersion;
    private final File serverPackage;

    @Inject
    public Multinode(
            PathResolver pathResolver,
            DockerFiles dockerFiles,
            Standard standard,
            Hadoop hadoop,
            EnvironmentOptions environmentOptions)
    {
        super(ImmutableList.of(standard, hadoop));
        this.pathResolver = requireNonNull(pathResolver, "pathResolver is null");
        this.dockerFiles = requireNonNull(dockerFiles, "dockerFiles is null");
        imagesVersion = requireNonNull(environmentOptions.imagesVersion, "environmentOptions.imagesVersion is null");
        serverPackage = requireNonNull(environmentOptions.serverPackage, "environmentOptions.serverPackage is null");
    }

    @Override
    protected void extendEnvironment(Environment.Builder builder)
    {
        builder.configureContainer("presto-master", container -> {
            container
                    .withFileSystemBind(dockerFiles.getDockerFilesHostPath("conf/environment/multinode/multinode-master-jvm.config"), CONTAINER_PRESTO_JVM_CONFIG, READ_ONLY)
                    .withFileSystemBind(dockerFiles.getDockerFilesHostPath("conf/environment/multinode/multinode-master-config.properties"), CONTAINER_PRESTO_CONFIG_PROPERTIES, READ_ONLY);
        });

        builder.addContainer("presto-worker", createPrestoWorker());
    }

    @SuppressWarnings("resource")
    private DockerContainer createPrestoWorker()
    {
        DockerContainer container = createPrestoContainer(dockerFiles, pathResolver, serverPackage, "prestodev/centos7-oj11:" + imagesVersion)
                .withFileSystemBind(dockerFiles.getDockerFilesHostPath("conf/environment/multinode/multinode-worker-jvm.config"), CONTAINER_PRESTO_JVM_CONFIG, READ_ONLY)
                .withFileSystemBind(dockerFiles.getDockerFilesHostPath("conf/environment/multinode/multinode-worker-config.properties"), CONTAINER_PRESTO_CONFIG_PROPERTIES, READ_ONLY)
                .withFileSystemBind(dockerFiles.getDockerFilesHostPath("common/hadoop/hive.properties"), CONTAINER_PRESTO_HIVE_PROPERTIES, READ_ONLY)
                .withFileSystemBind(dockerFiles.getDockerFilesHostPath("common/hadoop/iceberg.properties"), CONTAINER_PRESTO_ICEBERG_PROPERTIES, READ_ONLY);

        return container;
    }
}
