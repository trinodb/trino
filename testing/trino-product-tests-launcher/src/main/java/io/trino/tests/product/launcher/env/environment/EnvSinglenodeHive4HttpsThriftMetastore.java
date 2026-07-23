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
import com.google.inject.Inject;
import io.trino.tests.product.launcher.docker.DockerFiles;
import io.trino.tests.product.launcher.env.Environment;
import io.trino.tests.product.launcher.env.EnvironmentProvider;
import io.trino.tests.product.launcher.env.common.Hive4WithMinioHttpThrift;
import io.trino.tests.product.launcher.env.common.HiveMetastoreThriftHttpsNginx;
import io.trino.tests.product.launcher.env.common.Minio;
import io.trino.tests.product.launcher.env.common.Standard;
import io.trino.tests.product.launcher.env.common.TestsEnvironment;

import java.nio.file.Path;

import static io.trino.tests.product.launcher.env.EnvironmentContainers.COORDINATOR;
import static io.trino.tests.product.launcher.env.common.HiveMetastoreThriftHttpsNginx.HMS_THRIFT_HTTPS_NGINX;
import static io.trino.tests.product.launcher.env.common.Standard.CONTAINER_TRINO_ETC;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forHostPath;

@TestsEnvironment
public class EnvSinglenodeHive4HttpsThriftMetastore
        extends EnvironmentProvider
{
    private static final String CONTAINER_TRUSTSTORE = CONTAINER_TRINO_ETC + "/hive-metastore-thrift-https-truststore.jks";

    private final DockerFiles.ResourceProvider configDir;
    private final Path hiveMetastoreThriftHttpsTruststoreHostPath;

    @Inject
    public EnvSinglenodeHive4HttpsThriftMetastore(
            Standard standard,
            Minio minio,
            Hive4WithMinioHttpThrift hive4WithMinioHttpThrift,
            HiveMetastoreThriftHttpsNginx hiveMetastoreThriftHttpsNginx,
            DockerFiles dockerFiles)
    {
        super(ImmutableList.of(standard, minio, hive4WithMinioHttpThrift, requireNonNull(hiveMetastoreThriftHttpsNginx, "hiveMetastoreThriftHttpsNginx is null")));
        this.configDir = requireNonNull(dockerFiles, "dockerFiles is null").getDockerFilesHostDirectory("conf/environment/singlenode-hive4-https-thrift-metastore");
        this.hiveMetastoreThriftHttpsTruststoreHostPath = hiveMetastoreThriftHttpsNginx.getTrustStorePath();
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        builder.addConnector("hive", forHostPath(configDir.getPath("trino/catalog/hive.properties")));

        builder.configureContainer(COORDINATOR, dockerContainer -> dockerContainer
                .withCopyFileToContainer(
                        forHostPath(hiveMetastoreThriftHttpsTruststoreHostPath),
                        CONTAINER_TRUSTSTORE)
                .withEnv(
                        "JAVA_TOOL_OPTIONS",
                        "-Djavax.net.ssl.trustStore=%s -Djavax.net.ssl.trustStorePassword=changeit".formatted(CONTAINER_TRUSTSTORE)));

        builder.containerDependsOn(COORDINATOR, HMS_THRIFT_HTTPS_NGINX);
    }
}
