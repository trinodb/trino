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
package io.trino.tests.product.launcher.env.common;

import com.google.inject.Inject;
import io.trino.tests.product.launcher.docker.DockerFiles;
import io.trino.tests.product.launcher.docker.DockerFiles.ResourceProvider;
import io.trino.tests.product.launcher.env.DockerContainer;
import io.trino.tests.product.launcher.env.Environment;
import org.testcontainers.containers.startupcheck.IsRunningStartupCheckStrategy;
import org.testcontainers.containers.wait.strategy.Wait;

import java.nio.file.Path;
import java.time.Duration;

import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forHostPath;

/**
 * TLS termination (nginx) in front of the Hive 4 standalone metastore HTTP Thrift endpoint.
 * Trino should use {@code https://}{@link #HMS_THRIFT_HTTPS_NGINX}{@code :443/metastore} with bearer authentication.
 */
public class HiveMetastoreThriftHttpsNginx
        implements EnvironmentExtender
{
    public static final String HMS_THRIFT_HTTPS_NGINX = "hms-thrift-https";
    private static final String NGINX_IMAGE = "nginx:1.25-alpine";

    private final ResourceProvider configDir;

    @Inject
    public HiveMetastoreThriftHttpsNginx(DockerFiles dockerFiles)
    {
        this.configDir = requireNonNull(dockerFiles, "dockerFiles is null")
                .getDockerFilesHostDirectory("common/hive-metastore-thrift-https-nginx");
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        DockerContainer nginx = new DockerContainer(NGINX_IMAGE, HMS_THRIFT_HTTPS_NGINX)
                .withCopyFileToContainer(
                        forHostPath(configDir.getPath("nginx.conf")),
                        "/etc/nginx/nginx.conf")
                .withCopyFileToContainer(
                        forHostPath(configDir.getPath("server.crt")),
                        "/etc/nginx/certs/server.crt")
                .withCopyFileToContainer(
                        forHostPath(configDir.getPath("server.key")),
                        "/etc/nginx/certs/server.key")
                .withStartupCheckStrategy(new IsRunningStartupCheckStrategy())
                .waitingFor(Wait.forListeningPort())
                .withStartupTimeout(Duration.ofMinutes(2));
        builder.addContainer(nginx);
        builder.containerDependsOn(HMS_THRIFT_HTTPS_NGINX, Hive4WithMinioHttpThrift.METASTORE);
    }

    public Path getTrustStorePath()
    {
        return configDir.getPath("truststore.jks");
    }
}
