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
package io.trino.filesystem.s3.ssec;

import io.airlift.units.DataSize;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.filesystem.s3.S3FileSystemConfig;
import io.trino.filesystem.s3.S3FileSystemFactory;
import io.trino.testing.containers.Minio;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.internal.http.loader.DefaultSdkHttpClientBuilder;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpConfigurationOption;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.utils.AttributeMap;

import javax.net.ssl.TrustManager;

import java.io.IOException;
import java.net.URI;

import static io.trino.filesystem.s3.S3FileSystemConfig.S3SseType.CUSTOMER;
import static io.trino.filesystem.s3.TruststoreUtil.createTruststore;
import static io.trino.testing.containers.TestContainers.getPathFromClassPathResource;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestS3FileSystemMinioWithSseCustomerKey
        extends AbstractTestS3FileSystemWithSseCustomerKey
{
    private static final TrustManager[] TRUST_MANAGERS = createTruststore(
            getPathFromClassPathResource("minio/certs/truststore.jks"),
            "123456");
    private final String bucket = "test-bucket-test-s3-file-system-minio-sse-c";

    private Minio minio;

    @Override
    protected void initEnvironment()
    {
        super.initEnvironment();
        minio = Minio.builder()
                .withTLS(
                        getPathFromClassPathResource("minio/certs/private.key"),
                        getPathFromClassPathResource("minio/certs/private.crt"))
                .withTrustStore(getPathFromClassPathResource("minio/certs/truststore.jks"), "123456")
                .build();
        minio.start();
        minio.createBucket(bucket);
    }

    @AfterAll
    void tearDown()
    {
        if (minio != null) {
            minio.close();
            minio = null;
        }
    }

    @Override
    protected String bucket()
    {
        return bucket;
    }

    @Override
    protected S3ClientBuilder createS3ClientBuilder()
    {
        AttributeMap attributeMap = AttributeMap.builder()
                .put(SdkHttpConfigurationOption.TLS_TRUST_MANAGERS_PROVIDER, () -> TRUST_MANAGERS)
                .build();
        SdkHttpClient sdkHttpClient = new DefaultSdkHttpClientBuilder().buildWithDefaults(attributeMap);

        return S3Client.builder()
                .endpointOverride(URI.create(minio.getMinioAddress()))
                .region(Region.of(Minio.MINIO_REGION))
                .forcePathStyle(true)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(Minio.MINIO_ACCESS_KEY, Minio.MINIO_SECRET_KEY)))
                .httpClient(sdkHttpClient);
    }

    @Override
    protected S3FileSystemFactory createS3FileSystemFactory()
    {
        String absolutePath = getPathFromClassPathResource("minio/certs/truststore.jks");
        return new S3FileSystemFactory(OpenTelemetry.noop(), new S3FileSystemConfig()
                .setEndpoint(minio.getMinioAddress())
                .setRegion(Minio.MINIO_REGION)
                .setPathStyleAccess(true)
                .setAwsAccessKey(Minio.MINIO_ACCESS_KEY)
                .setAwsSecretKey(Minio.MINIO_SECRET_KEY)
                .setSseType(CUSTOMER)
                .setSseCustomerKey(s3SseCustomerKey.key())
                .setStreamingPartSize(DataSize.valueOf("5.5MB"))
                .setTruststorePath(absolutePath)
                .setTruststorePassword("123456"));
    }

    @Test
    @Override
    public void testPaths()
    {
        assertThatThrownBy(super::testPaths)
                .isInstanceOf(IOException.class)
                // MinIO does not support object keys with directory navigation ("/./" or "/../") or with double slashes ("//")
                .hasMessage("S3 HEAD request failed for file: s3://" + bucket + "/test/.././/file");
    }

    @Test
    @Override
    public void testListFiles()
            throws IOException
    {
        // MinIO is not hierarchical but has hierarchical naming constraints. For example it's not possible to have two blobs "level0" and "level0/level1".
        testListFiles(true);
    }

    @Test
    @Override
    public void testDeleteDirectory()
            throws IOException
    {
        // MinIO is not hierarchical but has hierarchical naming constraints. For example it's not possible to have two blobs "level0" and "level0/level1".
        testDeleteDirectory(true);
    }

    @Test
    @Override
    public void testListDirectories()
            throws IOException
    {
        // MinIO is not hierarchical but has hierarchical naming constraints. For example it's not possible to have two blobs "level0" and "level0/level1".
        testListDirectories(true);
    }
}
