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

import io.trino.testing.containers.Minio;
import io.trino.testing.containers.TrinoProductTestContainer;
import io.trino.testing.containers.environment.ProductTestEnvironment;
import org.testcontainers.containers.Network;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.trino.TrinoContainer;
import org.testcontainers.utility.MountableFile;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;

import static io.trino.testing.containers.Minio.MINIO_ROOT_PASSWORD;
import static io.trino.testing.containers.Minio.MINIO_ROOT_USER;
import static io.trino.tests.product.hive.KeystoreSecretsProductTestFixture.KEYSTORE_PASSWORD;
import static io.trino.tests.product.hive.KeystoreSecretsProductTestFixture.createKeyStore;
import static io.trino.tests.product.hive.KeystoreSecretsProductTestFixture.deleteIfExists;

/**
 * Hive file metastore with MinIO storage and keystore-resolved S3 credentials.
 */
public class S3KeystoreSecretsEnvironment
        extends ProductTestEnvironment
{
    public static final String BUCKET_NAME = "orders";
    public static final String SCHEMA_LOCATION = "s3://" + BUCKET_NAME + "/keystore-pt/";
    private static final String ACCESS_ALIAS = "fs.s3a.access.key";
    private static final String SECRET_ALIAS = "fs.s3a.secret.key";
    private static final String HIVE_METASTORE_DIR = "file:///var/trino/hive-data";

    private Network network;
    private Minio minio;
    private TrinoContainer trino;
    private Path credentialStore;

    @Override
    public void start()
            throws SQLException, InterruptedException
    {
        if (isRunning()) {
            return;
        }

        try {
            startEnvironment();
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to start S3 keystore secrets environment", e);
        }
    }

    private void startEnvironment()
            throws Exception
    {
        network = Network.newNetwork();

        minio = Minio.builder()
                .withNetwork(network)
                .build();
        minio.start();
        minio.createBucket(BUCKET_NAME);

        credentialStore = createKeyStore(Map.of(
                ACCESS_ALIAS, MINIO_ROOT_USER,
                SECRET_ALIAS, MINIO_ROOT_PASSWORD));

        Map<String, String> hiveCatalog = new LinkedHashMap<>();
        hiveCatalog.put("connector.name", "hive");
        hiveCatalog.put("hive.metastore", "file");
        hiveCatalog.put("hive.metastore.catalog.dir", HIVE_METASTORE_DIR);
        hiveCatalog.put("fs.local.enabled", "true");
        hiveCatalog.put("fs.native-s3.enabled", "true");
        hiveCatalog.put("s3.region", Minio.MINIO_REGION);
        hiveCatalog.put("s3.endpoint", "http://" + Minio.DEFAULT_HOST_NAME + ":" + Minio.MINIO_API_PORT);
        hiveCatalog.put("s3.path-style-access", "true");
        hiveCatalog.put("s3.aws-access-key", "${keystore:" + ACCESS_ALIAS + "}");
        hiveCatalog.put("s3.aws-secret-key", "${keystore:" + SECRET_ALIAS + "}");
        hiveCatalog.put("hive.non-managed-table-writes-enabled", "true");

        trino = TrinoProductTestContainer.builder()
                .withNetwork(network)
                .withCatalog("hive", hiveCatalog)
                .withCatalog("tpch", Map.of("connector.name", "tpch"))
                .build();
        trino.withCopyToContainer(Transferable.of(secretsConfiguration()), "/etc/trino/secrets.toml");
        trino.withCopyFileToContainer(MountableFile.forHostPath(credentialStore), "/etc/trino/credentials.jceks");
        trino.withCopyToContainer(Transferable.of(""), "/var/trino/hive-data/.keep");
        TrinoProductTestContainer.startAndWait(trino);
    }

    @Override
    public Connection createTrinoConnection()
            throws SQLException
    {
        return createTrinoConnection("test");
    }

    @Override
    public Connection createTrinoConnection(String user)
            throws SQLException
    {
        Connection connection = TrinoProductTestContainer.createConnection(trino, user);
        connection.setCatalog("hive");
        connection.setSchema("default");
        return connection;
    }

    @Override
    public String getTrinoJdbcUrl()
    {
        return trino != null ? trino.getJdbcUrl() : null;
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
        if (minio != null) {
            minio.close();
            minio = null;
        }
        if (network != null) {
            network.close();
            network = null;
        }
        deleteIfExists(credentialStore);
        credentialStore = null;
    }

    private static String secretsConfiguration()
    {
        return """
               [env]
               secrets-provider.name = "env"

               [keystore]
               secrets-provider.name = "keystore"
               keystore-file-path = "/etc/trino/credentials.jceks"
               keystore-type = "JCEKS"
               keystore-password = "%s"
               """.formatted(KEYSTORE_PASSWORD);
    }
}
