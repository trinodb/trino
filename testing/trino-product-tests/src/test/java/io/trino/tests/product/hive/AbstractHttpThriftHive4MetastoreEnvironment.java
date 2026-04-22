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
import io.trino.testing.containers.Minio;
import io.trino.testing.containers.TrinoProductTestContainer;
import io.trino.testing.containers.environment.ProductTestEnvironment;
import org.testcontainers.containers.Network;
import org.testcontainers.trino.TrinoContainer;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

import static io.trino.tests.product.hive.HiveCatalogPropertiesBuilder.hiveCatalog;

abstract class AbstractHttpThriftHive4MetastoreEnvironment
        extends ProductTestEnvironment
{
    private static final String BUCKET_NAME = "hive-http-thrift-metastore-bucket";

    protected Network network;
    protected Minio minio;
    protected Hive4MetastoreContainer metastore;
    private TrinoContainer trino;

    @Override
    public void start()
            throws SQLException
    {
        if (trino != null) {
            return;
        }

        network = Network.newNetwork();

        minio = Minio.builder()
                .withNetwork(network)
                .build();
        minio.start();
        minio.createBucket(BUCKET_NAME);

        metastore = new Hive4MetastoreContainer()
                .withNetwork(network)
                .withNetworkAliases(Hive4MetastoreContainer.HOST_NAME)
                .withHiveSiteXml(HttpThriftHive4MetastoreResources.readTextResource("metastore-hive-site.xml"));
        metastore.start();

        configureMetastoreDependencies();

        TrinoContainer container = TrinoProductTestContainer.builder()
                .withNetwork(network)
                .withCatalog("hive", hiveCatalogProperties())
                .build();
        customizeTrinoContainer(container);
        trino = container;
        TrinoProductTestContainer.startAndWait(trino);
    }

    protected void configureMetastoreDependencies() {}

    protected void customizeTrinoContainer(TrinoContainer container) {}

    protected abstract String getMetastoreUri();

    protected Map<String, String> additionalHiveCatalogProperties()
    {
        return Map.of();
    }

    private Map<String, String> hiveCatalogProperties()
    {
        var builder = hiveCatalog(getMetastoreUri())
                .withMinioS3()
                .withCommonProperties()
                .withHadoopFileSystemDisabled()
                .put("hive.metastore", "thrift")
                .put("hive.non-managed-table-writes-enabled", "true")
                .put("hive.metastore.http.client.authentication.type", "BEARER")
                .put("hive.metastore.http.client.additional-headers", "x-actor-username:hive");
        additionalHiveCatalogProperties().forEach(builder::put);
        return builder.build();
    }

    @Override
    public Connection createTrinoConnection()
            throws SQLException
    {
        return TrinoProductTestContainer.createConnection(trino);
    }

    @Override
    public String getTrinoJdbcUrl()
    {
        return trino.getJdbcUrl();
    }

    @Override
    public boolean isRunning()
    {
        return trino != null && trino.isRunning();
    }

    @Override
    protected void doClose()
    {
        if (trino != null) {
            trino.close();
            trino = null;
        }
        if (metastore != null) {
            metastore.close();
            metastore = null;
        }
        if (minio != null) {
            minio.close();
            minio = null;
        }
        if (network != null) {
            network.close();
            network = null;
        }
    }
}
