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
package io.trino.tests.product.hive;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.trino.TrinoContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;

public class HttpsThriftHive4MetastoreEnvironment
        extends AbstractHttpThriftHive4MetastoreEnvironment
{
    public static final String HMS_THRIFT_HTTPS_NGINX = "hms-thrift-https";
    private static final String CONTAINER_TRUSTSTORE = "/etc/trino/hive-metastore-thrift-https-truststore.jks";

    private GenericContainer<?> nginx;

    @Override
    protected void configureMetastoreDependencies()
    {
        nginx = new GenericContainer<>(DockerImageName.parse("nginx:1.25-alpine"))
                .withNetwork(network)
                .withNetworkAliases(HMS_THRIFT_HTTPS_NGINX)
                .withExposedPorts(443)
                .withCopyToContainer(Transferable.of(HttpThriftHive4MetastoreResources.readTextResource("nginx/nginx.conf")), "/etc/nginx/nginx.conf")
                .withCopyToContainer(MountableFile.forClasspathResource("hive-http-thrift-metastore/nginx/server.crt"), "/etc/nginx/certs/server.crt")
                .withCopyToContainer(MountableFile.forClasspathResource("hive-http-thrift-metastore/nginx/server.key"), "/etc/nginx/certs/server.key")
                .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofMinutes(2)))
                .dependsOn(metastore);
        nginx.start();
    }

    @Override
    protected void customizeTrinoContainer(TrinoContainer container)
    {
        Path truststore = HttpThriftHive4MetastoreResources.extractBinaryResource("nginx/truststore.jks");
        container.withCopyToContainer(MountableFile.forHostPath(truststore), CONTAINER_TRUSTSTORE);
        String truststoreInitScript =
                """
                #!/bin/bash
                if ! grep -qxF '-Djavax.net.ssl.trustStore=%1$s' /etc/trino/jvm.config; then
                    echo '-Djavax.net.ssl.trustStore=%1$s' >> /etc/trino/jvm.config
                fi
                if ! grep -qxF '-Djavax.net.ssl.trustStorePassword=changeit' /etc/trino/jvm.config; then
                    echo '-Djavax.net.ssl.trustStorePassword=changeit' >> /etc/trino/jvm.config
                fi
                """.formatted(CONTAINER_TRUSTSTORE);
        container.withCopyToContainer(
                Transferable.of(truststoreInitScript, 0755),
                "/docker/trino-init.d/01-hive-metastore-thrift-https-truststore.sh");
        container.dependsOn(nginx);
    }

    @Override
    protected String getMetastoreUri()
    {
        return "https://" + HMS_THRIFT_HTTPS_NGINX + ":443/metastore";
    }

    @Override
    protected Map<String, String> additionalHiveCatalogProperties()
    {
        return Map.of("hive.metastore.http.client.bearer-token", "test-hms-bearer-token");
    }

    @Override
    protected void doClose()
    {
        if (nginx != null) {
            nginx.close();
            nginx = null;
        }
        super.doClose();
    }
}
