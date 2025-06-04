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

import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forHostPath;

public class HttpProxy
        implements EnvironmentExtender
{
    public static final String HTTP_PROXY_CONF_DIR = "/usr/local/apache2/conf";
    public static final String PROXY = "proxy";
    private static final String HTTPD_VERSION = "2.4.51";

    private final ResourceProvider configDir;

    @Inject
    public HttpProxy(DockerFiles dockerFiles)
    {
        this.configDir = requireNonNull(dockerFiles, "dockerFiles is null")
                .getDockerFilesHostDirectory("common/http-proxy");
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        DockerContainer proxy = new DockerContainer("httpd:" + HTTPD_VERSION, PROXY);
        proxy.withCopyFileToContainer(
                forHostPath(configDir.getPath("httpd.conf")),
                HTTP_PROXY_CONF_DIR + "/httpd.conf");
        builder.addContainer(proxy);
    }
}
