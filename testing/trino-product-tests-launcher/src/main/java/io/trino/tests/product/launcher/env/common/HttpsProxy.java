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

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.tests.product.launcher.docker.DockerFiles;
import io.trino.tests.product.launcher.docker.DockerFiles.ResourceProvider;
import io.trino.tests.product.launcher.env.Environment;

import java.nio.file.Path;
import java.util.List;

import static io.trino.tests.product.launcher.env.common.HttpProxy.HTTP_PROXY_CONF_DIR;
import static io.trino.tests.product.launcher.env.common.HttpProxy.PROXY;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forHostPath;

public class HttpsProxy
        implements EnvironmentExtender
{
    private final ResourceProvider configDir;
    private final HttpProxy httpProxy;

    @Inject
    public HttpsProxy(DockerFiles dockerFiles, HttpProxy httpProxy)
    {
        this.configDir = requireNonNull(dockerFiles, "dockerFiles is null")
                .getDockerFilesHostDirectory("common/https-proxy");
        this.httpProxy = requireNonNull(httpProxy, "httpProxy is null");
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        builder.configureContainer(PROXY, proxyContainer -> proxyContainer
                .withCopyFileToContainer(
                        forHostPath(configDir.getPath("httpd.conf")),
                        HTTP_PROXY_CONF_DIR + "/httpd.conf")
                .withCopyFileToContainer(
                        forHostPath(configDir.getPath("cert")),
                        HTTP_PROXY_CONF_DIR + "/cert"));
    }

    public Path getTrustStorePath()
    {
        return configDir.getPath("cert/truststore.jks");
    }

    @Override
    public List<EnvironmentExtender> getDependencies()
    {
        return ImmutableList.of(httpProxy);
    }
}
