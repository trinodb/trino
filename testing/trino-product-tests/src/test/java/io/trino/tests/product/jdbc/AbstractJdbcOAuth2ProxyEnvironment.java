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
package io.trino.tests.product.jdbc;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.nio.file.Path;

/**
 * Shared base for OAuth2 proxy environments.
 */
public abstract class AbstractJdbcOAuth2ProxyEnvironment
        extends JdbcOAuth2BasicEnvironment
{
    private static final String HTTPD_IMAGE = "httpd:2.4.51";

    private GenericContainer<?> proxyContainer;

    @Override
    protected void startAdditionalServices(Path hydraPemPath, Path trinoPemPath)
            throws Exception
    {
        proxyContainer = new GenericContainer<>(DockerImageName.parse(HTTPD_IMAGE))
                .withNetwork(network)
                .withNetworkAliases("proxy")
                .withExposedPorts(8888)
                .withCopyFileToContainer(
                        MountableFile.forHostPath(extractClasspathResource(getProxyHttpdConfResource())),
                        "/usr/local/apache2/conf/httpd.conf");

        String htpasswdResource = getProxyHtpasswdResource();
        if (htpasswdResource != null) {
            proxyContainer.withCopyFileToContainer(
                    MountableFile.forHostPath(extractClasspathResource(htpasswdResource)),
                    "/usr/local/apache2/conf/.htpasswd");
        }

        String proxyPemResource = getProxyPemResource();
        if (proxyPemResource != null) {
            proxyContainer.withCopyFileToContainer(
                    MountableFile.forHostPath(extractClasspathResource(proxyPemResource)),
                    "/usr/local/apache2/conf/cert/proxy.pem");
        }

        proxyContainer.start();
    }

    @Override
    protected void configureTrino(GenericContainer<?> container)
    {
        container.withCopyToContainer(
                Transferable.of(getBaseConfigProperties()),
                "/etc/trino/config.properties");
        container.withCopyToContainer(
                Transferable.of("io.trino=INFO\n"),
                "/etc/trino/log.properties");

        String trustStoreResource = getProxyTrustStoreResource();
        if (trustStoreResource != null) {
            try {
                container.withCopyFileToContainer(
                        MountableFile.forHostPath(extractClasspathResource(trustStoreResource)),
                        "/etc/trino/proxy-truststore.jks");
            }
            catch (Exception e) {
                throw new RuntimeException("Failed to copy OAuth2 proxy trust store", e);
            }
        }
    }

    @Override
    protected void stopAdditionalServices()
    {
        if (proxyContainer != null) {
            proxyContainer.close();
            proxyContainer = null;
        }
    }

    protected abstract String getProxyHttpdConfResource();

    protected String getProxyHtpasswdResource()
    {
        return null;
    }

    protected String getProxyPemResource()
    {
        return null;
    }

    protected String getProxyTrustStoreResource()
    {
        return null;
    }
}
