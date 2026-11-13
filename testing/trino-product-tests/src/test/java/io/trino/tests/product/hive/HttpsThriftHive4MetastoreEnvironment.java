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

import io.trino.testing.containers.Hive4MetastoreContainer;
import org.testcontainers.trino.TrinoContainer;
import org.testcontainers.utility.MountableFile;

import java.util.Map;

/**
 * Hive 4 metastore serving Thrift over HTTPS with its own TLS ({@code hive.metastore.use.SSL}).
 * Trino trusts the metastore certificate through the catalog trust store and verifies the host name.
 */
public class HttpsThriftHive4MetastoreEnvironment
        extends AbstractHttpThriftHive4MetastoreEnvironment
{
    private static final String CONTAINER_TRUSTSTORE = "/etc/trino/hive-metastore-truststore.jks";

    @Override
    protected String metastoreHiveSiteXml()
    {
        return super.metastoreHiveSiteXml()
                .replace("</configuration>", HttpThriftHive4MetastoreResources.readTextResource("metastore-hive-site-tls.xml") + "</configuration>");
    }

    @Override
    protected void customizeMetastore(Hive4MetastoreContainer metastore)
    {
        metastore.withCopyToContainer(
                MountableFile.forClasspathResource("hive-http-thrift-metastore/hms-keystore.jks"),
                "/opt/hive/conf/hms-keystore.jks");
    }

    @Override
    protected void customizeTrinoContainer(TrinoContainer container)
    {
        container.withCopyToContainer(
                MountableFile.forClasspathResource("hive-http-thrift-metastore/hms-truststore.jks"),
                CONTAINER_TRUSTSTORE);
    }

    @Override
    protected String getMetastoreUri()
    {
        return "https://" + Hive4MetastoreContainer.HOST_NAME + ":9083/metastore";
    }

    @Override
    protected Map<String, String> additionalHiveCatalogProperties()
    {
        return Map.of(
                "hive.metastore.thrift.client.ssl.trust-certificate", CONTAINER_TRUSTSTORE,
                "hive.metastore.thrift.client.ssl.trust-certificate-password", "changeit");
    }
}
