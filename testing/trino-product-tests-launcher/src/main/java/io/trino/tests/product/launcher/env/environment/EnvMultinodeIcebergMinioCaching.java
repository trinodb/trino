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

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.trino.tests.product.launcher.docker.DockerFiles;
import io.trino.tests.product.launcher.docker.DockerFiles.ResourceProvider;
import io.trino.tests.product.launcher.env.Environment;
import io.trino.tests.product.launcher.env.EnvironmentProvider;
import io.trino.tests.product.launcher.env.common.Hadoop;
import io.trino.tests.product.launcher.env.common.Minio;
import io.trino.tests.product.launcher.env.common.StandardMultinode;
import io.trino.tests.product.launcher.env.common.TestsEnvironment;

import static org.testcontainers.utility.MountableFile.forHostPath;

@TestsEnvironment
public final class EnvMultinodeIcebergMinioCaching
        extends EnvironmentProvider
{
    private final ResourceProvider configDir;

    @Inject
    public EnvMultinodeIcebergMinioCaching(StandardMultinode standardMultinode, Hadoop hadoop, Minio minio, DockerFiles dockerFiles)
    {
        super(standardMultinode, hadoop, minio);
        this.configDir = dockerFiles.getDockerFilesHostDirectory("conf/environment/multinode-iceberg-minio-cached");
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        builder.addConnector("iceberg", forHostPath(configDir.getPath("iceberg.properties")));
        builder.configureContainers(container -> container.withTmpFs(ImmutableMap.of("/tmp/cache", "rw")));
    }
}
