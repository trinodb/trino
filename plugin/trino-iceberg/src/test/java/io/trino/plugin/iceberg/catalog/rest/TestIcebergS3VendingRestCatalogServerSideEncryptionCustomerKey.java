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
package io.trino.plugin.iceberg.catalog.rest;

import com.google.common.collect.ImmutableMap;
import io.airlift.http.server.testing.TestingHttpServer;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.s3.S3FileSystemConfig;
import io.trino.filesystem.s3.S3FileSystemFactory;
import io.trino.filesystem.s3.S3FileSystemStats;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.containers.MinioTls;
import io.trino.testing.containers.TlsCertificate;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.rest.RESTCatalogServlet;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.Map;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.Minio.MINIO_REGION;
import static io.trino.testing.containers.Minio.MINIO_ROOT_PASSWORD;
import static io.trino.testing.containers.Minio.MINIO_ROOT_USER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestIcebergS3VendingRestCatalogServerSideEncryptionCustomerKey
        extends AbstractTestQueryFramework
{
    private static final byte[] SSE_KEY = randomSseKey();
    private static final String BASE64_SSE_KEY = Base64.getEncoder().encodeToString(SSE_KEY);
    private static final String BASE64_SSE_KEY_MD5 = base64Md5(SSE_KEY);

    private static byte[] randomSseKey()
    {
        byte[] key = new byte[32];
        new SecureRandom().nextBytes(key);
        return key;
    }

    private static String base64Md5(byte[] bytes)
    {
        try {
            return Base64.getEncoder().encodeToString(MessageDigest.getInstance("MD5").digest(bytes));
        }
        catch (NoSuchAlgorithmException e) {
            throw new AssertionError("MD5 is always available", e);
        }
    }

    private final String bucketName = "test-sse-c-" + randomNameSuffix();
    private S3FileSystemFactory s3FileSystemFactory;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        TlsCertificate certificate = TlsCertificate.generate();
        closeAfterClass(certificate);
        certificate.installAsDefaultTrustStore();

        MinioTls minioContainer = MinioTls.builder()
                .withCertificate(certificate)
                .build();
        minioContainer.start();
        closeAfterClass(minioContainer);

        String minioAddress = minioContainer.getMinioHttpsEndpoint();

        createBucket(minioAddress, bucketName);

        s3FileSystemFactory = new S3FileSystemFactory(
                OpenTelemetry.noop(),
                new S3FileSystemConfig()
                        .setRegion(MINIO_REGION)
                        .setEndpoint(minioAddress)
                        .setPathStyleAccess(true)
                        .setAwsAccessKey(MINIO_ROOT_USER)
                        .setAwsSecretKey(MINIO_ROOT_PASSWORD),
                new S3FileSystemStats());
        closeAfterClass(s3FileSystemFactory::destroy);

        JdbcCatalog backend = closeAfterClass(createS3BackendCatalog(minioAddress));

        Map<String, String> vendedConfig = ImmutableMap.of(
                S3FileIOProperties.SSE_TYPE, S3FileIOProperties.SSE_TYPE_CUSTOM,
                S3FileIOProperties.SSE_KEY, BASE64_SSE_KEY);
        TestingHttpServer testServer = RestCatalogTestingHttpServers.create(
                new RESTCatalogServlet(new VendingRESTCatalogAdapter(backend, vendedConfig)));
        testServer.start();
        closeAfterClass(testServer::stop);

        return IcebergQueryRunner.builder()
                .setIcebergProperties(
                        ImmutableMap.<String, String>builder()
                                .put("iceberg.catalog.type", "rest")
                                .put("iceberg.rest-catalog.uri", testServer.getBaseUrl().toString())
                                .put("iceberg.rest-catalog.vended-credentials-enabled", "true")
                                .put("fs.native-s3.enabled", "true")
                                .put("s3.region", MINIO_REGION)
                                .put("s3.endpoint", minioAddress)
                                .put("s3.path-style-access", "true")
                                .put("s3.aws-access-key", MINIO_ROOT_USER)
                                .put("s3.aws-secret-key", MINIO_ROOT_PASSWORD)
                                .buildOrThrow())
                .build();
    }

    private JdbcCatalog createS3BackendCatalog(String minioAddress)
            throws IOException
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put(CatalogProperties.URI, "jdbc:h2:file:" + Files.createTempFile(null, null).toAbsolutePath())
                .put(JdbcCatalog.PROPERTY_PREFIX + "username", "user")
                .put(JdbcCatalog.PROPERTY_PREFIX + "password", "password")
                .put(JdbcCatalog.PROPERTY_PREFIX + "schema-version", "V1")
                .put(CatalogProperties.WAREHOUSE_LOCATION, "s3://" + bucketName + "/iceberg_data")
                .put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.aws.s3.S3FileIO")
                .put(S3FileIOProperties.ENDPOINT, minioAddress)
                .put(S3FileIOProperties.ACCESS_KEY_ID, MINIO_ROOT_USER)
                .put(S3FileIOProperties.SECRET_ACCESS_KEY, MINIO_ROOT_PASSWORD)
                .put(S3FileIOProperties.PATH_STYLE_ACCESS, "true")
                .put(AwsClientProperties.CLIENT_REGION, MINIO_REGION)
                .put(S3FileIOProperties.SSE_TYPE, S3FileIOProperties.SSE_TYPE_CUSTOM)
                .put(S3FileIOProperties.SSE_KEY, BASE64_SSE_KEY)
                .put(S3FileIOProperties.SSE_MD5, BASE64_SSE_KEY_MD5)
                .buildOrThrow();

        JdbcCatalog catalog = new JdbcCatalog();
        catalog.initialize("backend_jdbc", properties);
        return catalog;
    }

    private static void createBucket(String endpoint, String bucketName)
    {
        try (S3Client s3Client = S3Client.builder()
                .endpointOverride(URI.create(endpoint))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(MINIO_ROOT_USER, MINIO_ROOT_PASSWORD)))
                .region(Region.of(MINIO_REGION))
                .forcePathStyle(true)
                .build()) {
            s3Client.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
        }
    }

    @Test
    void testWriteAndReadWithVendedSseCKey()
    {
        String tableName = "test_sse_c_" + randomNameSuffix();
        try {
            assertUpdate("CREATE TABLE " + tableName + " AS SELECT 1 x, VARCHAR 'hello' y", 1);
            assertQuery("SELECT * FROM " + tableName, "VALUES (1, 'hello')");

            assertUpdate("INSERT INTO " + tableName + " VALUES (2, 'world')", 1);
            assertQuery("SELECT * FROM " + tableName + " ORDER BY x", "VALUES (1, 'hello'), (2, 'world')");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    void testDataFilesAreEncrypted()
            throws IOException
    {
        String tableName = "test_sse_c_encrypted_" + randomNameSuffix();
        try {
            assertUpdate("CREATE TABLE " + tableName + " AS SELECT 1 x", 1);
            assertQuery("SELECT * FROM " + tableName, "VALUES 1");

            String dataFilePath = (String) computeScalar(
                    "SELECT file_path FROM \"" + tableName + "$files\" LIMIT 1");
            assertThat(dataFilePath).startsWith("s3://");

            // Reading the data file without the SSE-C key must fail,
            // proving the data is actually encrypted on storage
            TrinoFileSystem plainFileSystem = s3FileSystemFactory.create(ConnectorIdentity.ofUser("test"));
            TrinoInputFile plainInputFile = plainFileSystem.newInputFile(Location.of(dataFilePath));
            assertThatThrownBy(plainInputFile::length)
                    .isInstanceOf(IOException.class);
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }
}
