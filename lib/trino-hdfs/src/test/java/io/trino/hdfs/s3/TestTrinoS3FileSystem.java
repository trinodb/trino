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
package io.trino.hdfs.s3;

import com.amazonaws.AmazonWebServiceClient;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSSessionCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.auth.WebIdentityTokenCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3EncryptionClient;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.EncryptionMaterials;
import com.amazonaws.services.s3.model.EncryptionMaterialsProvider;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.StorageClass;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClient;
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.trino.hdfs.s3.TrinoS3FileSystem.UnrecoverableS3OperationException;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.memory.context.MemoryReservationHandler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.jupiter.api.Test;

import javax.crypto.spec.SecretKeySpec;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.SequenceInputStream;
import java.lang.reflect.Field;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.hdfs.s3.TrinoS3FileSystem.NO_SUCH_BUCKET_ERROR_CODE;
import static io.trino.hdfs.s3.TrinoS3FileSystem.NO_SUCH_KEY_ERROR_CODE;
import static io.trino.hdfs.s3.TrinoS3FileSystem.S3_ACCESS_KEY;
import static io.trino.hdfs.s3.TrinoS3FileSystem.S3_ACL_TYPE;
import static io.trino.hdfs.s3.TrinoS3FileSystem.S3_CREDENTIALS_PROVIDER;
import static io.trino.hdfs.s3.TrinoS3FileSystem.S3_ENCRYPTION_MATERIALS_PROVIDER;
import static io.trino.hdfs.s3.TrinoS3FileSystem.S3_ENDPOINT;
import static io.trino.hdfs.s3.TrinoS3FileSystem.S3_EXTERNAL_ID;
import static io.trino.hdfs.s3.TrinoS3FileSystem.S3_IAM_ROLE;
import static io.trino.hdfs.s3.TrinoS3FileSystem.S3_KMS_KEY_ID;
import static io.trino.hdfs.s3.TrinoS3FileSystem.S3_MAX_BACKOFF_TIME;
import static io.trino.hdfs.s3.TrinoS3FileSystem.S3_MAX_CLIENT_RETRIES;
import static io.trino.hdfs.s3.TrinoS3FileSystem.S3_MAX_RETRY_TIME;
import static io.trino.hdfs.s3.TrinoS3FileSystem.S3_PATH_STYLE_ACCESS;
import static io.trino.hdfs.s3.TrinoS3FileSystem.S3_PIN_CLIENT_TO_CURRENT_REGION;
import static io.trino.hdfs.s3.TrinoS3FileSystem.S3_REGION;
import static io.trino.hdfs.s3.TrinoS3FileSystem.S3_SECRET_KEY;
import static io.trino.hdfs.s3.TrinoS3FileSystem.S3_SESSION_TOKEN;
import static io.trino.hdfs.s3.TrinoS3FileSystem.S3_SKIP_GLACIER_OBJECTS;
import static io.trino.hdfs.s3.TrinoS3FileSystem.S3_STAGING_DIRECTORY;
import static io.trino.hdfs.s3.TrinoS3FileSystem.S3_STREAMING_UPLOAD_ENABLED;
import static io.trino.hdfs.s3.TrinoS3FileSystem.S3_STREAMING_UPLOAD_PART_SIZE;
import static io.trino.hdfs.s3.TrinoS3FileSystem.S3_USER_AGENT_PREFIX;
import static io.trino.hdfs.s3.TrinoS3FileSystem.S3_USE_WEB_IDENTITY_TOKEN_CREDENTIALS_PROVIDER;
import static io.trino.memory.context.AggregatedMemoryContext.newRootAggregatedMemoryContext;
import static java.net.HttpURLConnection.HTTP_FORBIDDEN;
import static java.net.HttpURLConnection.HTTP_INTERNAL_ERROR;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.nio.file.Files.createTempDirectory;
import static java.nio.file.Files.createTempFile;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.abort;

public class TestTrinoS3FileSystem
{
    private static final int HTTP_RANGE_NOT_SATISFIABLE = 416;
    private static final String S3_DIRECTORY_OBJECT_CONTENT_TYPE = "application/x-directory; charset=UTF-8";

    @Test
    public void testEmbeddedCredentials()
            throws Exception
    {
        Configuration config = new Configuration(false);
        try (TrinoS3FileSystem fs = new TrinoS3FileSystem()) {
            AWSCredentials credentials = getStaticCredentials(config, fs, "s3n://testAccess:testSecret@test-bucket/");
            assertThat(credentials.getAWSAccessKeyId()).isEqualTo("testAccess");
            assertThat(credentials.getAWSSecretKey()).isEqualTo("testSecret");
            assertThat(credentials).isNotInstanceOf(AWSSessionCredentials.class);
        }
    }

    @Test
    public void testStaticCredentials()
            throws Exception
    {
        Configuration config = new Configuration(false);
        config.set(S3_ACCESS_KEY, "test_access_key");
        config.set(S3_SECRET_KEY, "test_secret_key");

        try (TrinoS3FileSystem fs = new TrinoS3FileSystem()) {
            AWSCredentials credentials = getStaticCredentials(config, fs, "s3n://test-bucket/");
            assertThat(credentials.getAWSAccessKeyId()).isEqualTo("test_access_key");
            assertThat(credentials.getAWSSecretKey()).isEqualTo("test_secret_key");
            assertThat(credentials).isNotInstanceOf(AWSSessionCredentials.class);
        }

        config.set(S3_SESSION_TOKEN, "test_token");
        try (TrinoS3FileSystem fs = new TrinoS3FileSystem()) {
            AWSCredentials credentials = getStaticCredentials(config, fs, "s3n://test-bucket/");
            assertThat(credentials.getAWSAccessKeyId()).isEqualTo("test_access_key");
            assertThat(credentials.getAWSSecretKey()).isEqualTo("test_secret_key");
            assertThat(credentials).isInstanceOfSatisfying(AWSSessionCredentials.class, sessionCredentials ->
                    assertThat(sessionCredentials.getSessionToken()).isEqualTo("test_token"));
        }
    }

    private static AWSCredentials getStaticCredentials(Configuration config, TrinoS3FileSystem fileSystem, String uri)
            throws IOException, URISyntaxException
    {
        fileSystem.initialize(new URI(uri), config);
        AWSCredentialsProvider awsCredentialsProvider = getAwsCredentialsProvider(fileSystem);
        assertThat(awsCredentialsProvider).isInstanceOf(AWSStaticCredentialsProvider.class);
        return awsCredentialsProvider.getCredentials();
    }

    @Test
    public void testEndpointWithPinToCurrentRegionConfiguration()
            throws Exception
    {
        Configuration config = new Configuration(false);
        config.set(S3_ENDPOINT, "test.example.endpoint.com");
        config.set(S3_PIN_CLIENT_TO_CURRENT_REGION, "true");
        try (TrinoS3FileSystem fs = new TrinoS3FileSystem()) {
            assertThatThrownBy(() -> fs.initialize(new URI("s3a://test-bucket/"), config))
                    .isInstanceOf(VerifyException.class)
                    .hasMessage("Invalid configuration: either endpoint can be set or S3 client can be pinned to the current region");
        }
    }

    @Test
    public void testEndpointWithExplicitRegionConfiguration()
            throws Exception
    {
        Configuration config = new Configuration(false);

        // Only endpoint set
        config.set(S3_ENDPOINT, "test.example.endpoint.com");
        try (TrinoS3FileSystem fs = new TrinoS3FileSystem()) {
            fs.initialize(new URI("s3a://test-bucket/"), config);
            assertThat(((AmazonS3Client) fs.getS3Client()).getSignerRegionOverride()).isNull();
        }

        // Endpoint and region set
        config.set(S3_ENDPOINT, "test.example.endpoint.com");
        config.set(S3_REGION, "region1");
        try (TrinoS3FileSystem fs = new TrinoS3FileSystem()) {
            fs.initialize(new URI("s3a://test-bucket/"), config);
            assertThat(((AmazonS3Client) fs.getS3Client()).getSignerRegionOverride()).isEqualTo("region1");
        }

        // Only region set
        config.set(S3_REGION, "region1");
        try (TrinoS3FileSystem fs = new TrinoS3FileSystem()) {
            fs.initialize(new URI("s3a://test-bucket/"), config);
            assertThat(((AmazonS3Client) fs.getS3Client()).getSignerRegionOverride()).isEqualTo("region1");
        }
    }

    @Test
    public void testAssumeRoleDefaultCredentials()
            throws Exception
    {
        Configuration config = new Configuration(false);
        config.set(S3_IAM_ROLE, "test_role");

        try (TrinoS3FileSystem fs = new TrinoS3FileSystem()) {
            fs.initialize(new URI("s3n://test-bucket/"), config);
            AWSCredentialsProvider tokenService = getStsCredentialsProvider(fs, "test_role");
            assertThat(tokenService).isInstanceOf(DefaultAWSCredentialsProviderChain.class);
        }
    }

    @Test
    public void testAssumeRoleStaticCredentials()
            throws Exception
    {
        Configuration config = new Configuration(false);
        config.set(S3_ACCESS_KEY, "test_access_key");
        config.set(S3_SECRET_KEY, "test_secret_key");
        config.set(S3_IAM_ROLE, "test_role");

        try (TrinoS3FileSystem fs = new TrinoS3FileSystem()) {
            fs.initialize(new URI("s3n://test-bucket/"), config);
            AWSCredentialsProvider tokenService = getStsCredentialsProvider(fs, "test_role");
            assertThat(tokenService).isInstanceOf(AWSStaticCredentialsProvider.class);

            AWSCredentials credentials = tokenService.getCredentials();
            assertThat(credentials.getAWSAccessKeyId()).isEqualTo("test_access_key");
            assertThat(credentials.getAWSSecretKey()).isEqualTo("test_secret_key");
        }
    }

    private static AWSCredentialsProvider getStsCredentialsProvider(TrinoS3FileSystem fs, String expectedRole)
    {
        AWSCredentialsProvider awsCredentialsProvider = getAwsCredentialsProvider(fs);
        assertThat(awsCredentialsProvider).isInstanceOf(STSAssumeRoleSessionCredentialsProvider.class);

        assertThat(getFieldValue(awsCredentialsProvider, "roleArn", String.class)).isEqualTo(expectedRole);

        AWSSecurityTokenService tokenService = getFieldValue(awsCredentialsProvider, "securityTokenService", AWSSecurityTokenService.class);
        assertThat(tokenService).isInstanceOf(AWSSecurityTokenServiceClient.class);
        return getFieldValue(tokenService, "awsCredentialsProvider", AWSCredentialsProvider.class);
    }

    @Test
    public void testAssumeRoleCredentialsWithExternalId()
            throws Exception
    {
        Configuration config = new Configuration(false);
        config.set(S3_IAM_ROLE, "role");
        config.set(S3_EXTERNAL_ID, "externalId");

        try (TrinoS3FileSystem fs = new TrinoS3FileSystem()) {
            fs.initialize(new URI("s3n://test-bucket/"), config);
            AWSCredentialsProvider awsCredentialsProvider = getAwsCredentialsProvider(fs);
            assertThat(awsCredentialsProvider).isInstanceOf(STSAssumeRoleSessionCredentialsProvider.class);
            assertThat(getFieldValue(awsCredentialsProvider, "roleArn", String.class)).isEqualTo("role");
            assertThat(getFieldValue(awsCredentialsProvider, "roleExternalId", String.class)).isEqualTo("externalId");
        }
    }

    @Test
    public void testDefaultCredentials()
            throws Exception
    {
        Configuration config = new Configuration(false);

        try (TrinoS3FileSystem fs = new TrinoS3FileSystem()) {
            fs.initialize(new URI("s3n://test-bucket/"), config);
            assertThat(getAwsCredentialsProvider(fs)).isInstanceOf(DefaultAWSCredentialsProviderChain.class);
        }
    }

    @Test
    public void testPathStyleAccess()
            throws Exception
    {
        Configuration config = new Configuration(false);
        config.setBoolean(S3_PATH_STYLE_ACCESS, true);

        try (TrinoS3FileSystem fs = new TrinoS3FileSystem()) {
            fs.initialize(new URI("s3n://test-bucket/"), config);
            S3ClientOptions clientOptions = getFieldValue(fs.getS3Client(), AmazonS3Client.class, "clientOptions", S3ClientOptions.class);
            assertThat(clientOptions.isPathStyleAccess()).isTrue();
        }
    }

    @Test
    public void testUnderscoreBucket()
            throws Exception
    {
        Configuration config = new Configuration(false);
        config.setBoolean(S3_PATH_STYLE_ACCESS, true);

        try (TrinoS3FileSystem fs = new TrinoS3FileSystem()) {
            MockAmazonS3 s3 = new MockAmazonS3();
            String expectedBucketName = "test-bucket_underscore";
            URI uri = new URI("s3n://" + expectedBucketName + "/");
            assertThat(fs.getBucketName(uri)).isEqualTo(expectedBucketName);
            fs.initialize(uri, config);
            fs.setS3Client(s3);
            fs.getS3ObjectMetadata(new Path("/test/path"));
            assertThat(expectedBucketName).isEqualTo(s3.getGetObjectMetadataRequest().getBucketName());
        }
    }

    @SuppressWarnings({"ResultOfMethodCallIgnored", "OverlyStrongTypeCast", "ConstantConditions"})
    @Test
    public void testReadRetryCounters()
            throws Exception
    {
        try (TrinoS3FileSystem fs = new TrinoS3FileSystem()) {
            int maxRetries = 2;
            MockAmazonS3 s3 = new MockAmazonS3();
            s3.setGetObjectHttpErrorCode(HTTP_INTERNAL_ERROR);
            Configuration configuration = new Configuration(false);
            configuration.set(S3_MAX_BACKOFF_TIME, "1ms");
            configuration.set(S3_MAX_RETRY_TIME, "5s");
            configuration.setInt(S3_MAX_CLIENT_RETRIES, maxRetries);
            fs.initialize(new URI("s3n://test-bucket/"), configuration);
            fs.setS3Client(s3);
            try (FSDataInputStream inputStream = fs.open(new Path("s3n://test-bucket/test"))) {
                inputStream.read();
            }
            catch (Throwable expected) {
                assertThat(expected).isInstanceOf(AmazonS3Exception.class);
                assertThat(((AmazonS3Exception) expected).getStatusCode()).isEqualTo(HTTP_INTERNAL_ERROR);
                assertThat(TrinoS3FileSystem.getFileSystemStats().getReadRetries().getTotalCount()).isEqualTo(maxRetries);
                assertThat(TrinoS3FileSystem.getFileSystemStats().getGetObjectRetries().getTotalCount()).isEqualTo((maxRetries + 1L) * maxRetries);
            }
        }
    }

    @SuppressWarnings({"OverlyStrongTypeCast", "ConstantConditions"})
    @Test
    public void testGetMetadataRetryCounter()
    {
        int maxRetries = 2;
        try (TrinoS3FileSystem fs = new TrinoS3FileSystem()) {
            MockAmazonS3 s3 = new MockAmazonS3();
            s3.setGetObjectMetadataHttpCode(HTTP_INTERNAL_ERROR);
            Configuration configuration = new Configuration(false);
            configuration.set(S3_MAX_BACKOFF_TIME, "1ms");
            configuration.set(S3_MAX_RETRY_TIME, "5s");
            configuration.setInt(S3_MAX_CLIENT_RETRIES, maxRetries);
            fs.initialize(new URI("s3n://test-bucket/"), configuration);
            fs.setS3Client(s3);
            fs.getS3ObjectMetadata(new Path("s3n://test-bucket/test"));
        }
        catch (Throwable expected) {
            assertThat(expected).isInstanceOf(AmazonS3Exception.class);
            assertThat(((AmazonS3Exception) expected).getStatusCode()).isEqualTo(HTTP_INTERNAL_ERROR);
            assertThat(TrinoS3FileSystem.getFileSystemStats().getGetMetadataRetries().getTotalCount()).isEqualTo(maxRetries);
        }
    }

    @Test
    public void testReadNotFound()
            throws Exception
    {
        testReadObject(IOException.class, HTTP_NOT_FOUND, null);
    }

    @Test
    public void testNoSuchKeyFound()
            throws Exception
    {
        testReadObject(FileNotFoundException.class, HTTP_NOT_FOUND, NO_SUCH_KEY_ERROR_CODE);
    }

    @Test
    public void testNoSuchBucketFound()
            throws Exception
    {
        testReadObject(FileNotFoundException.class, HTTP_NOT_FOUND, NO_SUCH_BUCKET_ERROR_CODE);
    }

    @Test
    public void testReadForbidden()
            throws Exception
    {
        testReadObject(IOException.class, HTTP_FORBIDDEN, null);
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private static void testReadObject(Class<?> exceptionClass, int httpErrorCode, String s3ErrorCode)
            throws Exception
    {
        try (TrinoS3FileSystem fs = new TrinoS3FileSystem()) {
            MockAmazonS3 s3 = new MockAmazonS3();
            s3.setGetObjectHttpErrorCode(httpErrorCode);
            s3.setGetObjectS3ErrorCode(s3ErrorCode);
            fs.initialize(new URI("s3n://test-bucket/"), new Configuration(false));
            fs.setS3Client(s3);
            try (FSDataInputStream inputStream = fs.open(new Path("s3n://test-bucket/test"))) {
                assertThatThrownBy(inputStream::read)
                        .isInstanceOf(exceptionClass)
                        .hasMessageContaining("Failing getObject call with status code:" + httpErrorCode + "; error code:" + s3ErrorCode);
            }
        }
    }

    @Test
    public void testCreateWithNonexistentStagingDirectory()
            throws Exception
    {
        java.nio.file.Path stagingParent = createTempDirectory("test");
        java.nio.file.Path staging = Paths.get(stagingParent.toString(), "staging");
        // stagingParent = /tmp/testXXX
        // staging = /tmp/testXXX/staging

        try (TrinoS3FileSystem fs = new TrinoS3FileSystem()) {
            MockAmazonS3 s3 = new MockAmazonS3();
            Configuration conf = new Configuration(false);
            conf.set(S3_STAGING_DIRECTORY, staging.toString());
            conf.set(S3_STREAMING_UPLOAD_ENABLED, "false");
            fs.initialize(new URI("s3n://test-bucket/"), conf);
            fs.setS3Client(s3);
            FSDataOutputStream stream = fs.create(new Path("s3n://test-bucket/test"));
            stream.close();
            assertThat(Files.exists(staging)).isTrue();
        }
        finally {
            deleteRecursively(stagingParent, ALLOW_INSECURE);
        }
    }

    @Test
    public void testCreateWithStagingDirectoryFile()
            throws Exception
    {
        java.nio.file.Path staging = createTempFile("staging", null);
        // staging = /tmp/stagingXXX.tmp

        try (TrinoS3FileSystem fs = new TrinoS3FileSystem()) {
            MockAmazonS3 s3 = new MockAmazonS3();
            Configuration conf = new Configuration(false);
            conf.set(S3_STAGING_DIRECTORY, staging.toString());
            conf.set(S3_STREAMING_UPLOAD_ENABLED, "false");
            fs.initialize(new URI("s3n://test-bucket/"), conf);
            fs.setS3Client(s3);
            assertThatThrownBy(() -> fs.create(new Path("s3n://test-bucket/test")))
                    .isInstanceOf(IOException.class)
                    .hasMessageStartingWith("Configured staging path is not a directory:");
        }
        finally {
            Files.deleteIfExists(staging);
        }
    }

    @Test
    public void testCreateWithStagingDirectorySymlink()
            throws Exception
    {
        java.nio.file.Path staging = createTempDirectory("staging");
        java.nio.file.Path link = Paths.get(staging + ".symlink");
        // staging = /tmp/stagingXXX
        // link = /tmp/stagingXXX.symlink -> /tmp/stagingXXX

        try {
            try {
                Files.createSymbolicLink(link, staging);
            }
            catch (UnsupportedOperationException e) {
                abort("Filesystem does not support symlinks");
            }

            try (TrinoS3FileSystem fs = new TrinoS3FileSystem()) {
                MockAmazonS3 s3 = new MockAmazonS3();
                Configuration conf = new Configuration(false);
                conf.set(S3_STAGING_DIRECTORY, link.toString());
                fs.initialize(new URI("s3n://test-bucket/"), conf);
                fs.setS3Client(s3);
                FSDataOutputStream stream = fs.create(new Path("s3n://test-bucket/test"));
                stream.close();
                assertThat(Files.exists(link)).isTrue();
            }
        }
        finally {
            deleteRecursively(link, ALLOW_INSECURE);
            deleteRecursively(staging, ALLOW_INSECURE);
        }
    }

    @Test
    public void testReadRequestRangeNotSatisfiable()
            throws Exception
    {
        try (TrinoS3FileSystem fs = new TrinoS3FileSystem()) {
            MockAmazonS3 s3 = new MockAmazonS3();
            s3.setGetObjectHttpErrorCode(HTTP_RANGE_NOT_SATISFIABLE);
            fs.initialize(new URI("s3n://test-bucket/"), new Configuration(false));
            fs.setS3Client(s3);
            try (FSDataInputStream inputStream = fs.open(new Path("s3n://test-bucket/test"))) {
                assertThat(inputStream.read()).isEqualTo(-1);
            }
        }
    }

    @Test
    public void testGetMetadataForbidden()
            throws Exception
    {
        try (TrinoS3FileSystem fs = new TrinoS3FileSystem()) {
            MockAmazonS3 s3 = new MockAmazonS3();
            s3.setGetObjectMetadataHttpCode(HTTP_FORBIDDEN);
            fs.initialize(new URI("s3n://test-bucket/"), new Configuration(false));
            fs.setS3Client(s3);
            assertThatThrownBy(() -> fs.getS3ObjectMetadata(new Path("s3n://test-bucket/test")))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("Failing getObjectMetadata call with " + HTTP_FORBIDDEN);
        }
    }

    @Test
    public void testGetMetadataNotFound()
            throws Exception
    {
        try (TrinoS3FileSystem fs = new TrinoS3FileSystem()) {
            MockAmazonS3 s3 = new MockAmazonS3();
            s3.setGetObjectMetadataHttpCode(HTTP_NOT_FOUND);
            fs.initialize(new URI("s3n://test-bucket/"), new Configuration(false));
            fs.setS3Client(s3);
            assertThat(fs.getS3ObjectMetadata(new Path("s3n://test-bucket/test"))).isNull();
        }
    }

    @Test
    public void testEncryptionMaterialsProvider()
            throws Exception
    {
        Configuration config = new Configuration(false);
        config.set(S3_ENCRYPTION_MATERIALS_PROVIDER, TestEncryptionMaterialsProvider.class.getName());

        try (TrinoS3FileSystem fs = new TrinoS3FileSystem()) {
            fs.initialize(new URI("s3n://test-bucket/"), config);
            assertThat(fs.getS3Client()).isInstanceOf(AmazonS3EncryptionClient.class);
        }
    }

    @Test
    public void testKMSEncryptionMaterialsProvider()
            throws Exception
    {
        Configuration config = new Configuration(false);
        config.set(S3_KMS_KEY_ID, "test-key-id");

        try (TrinoS3FileSystem fs = new TrinoS3FileSystem()) {
            fs.initialize(new URI("s3n://test-bucket/"), config);
            assertThat(fs.getS3Client()).isInstanceOf(AmazonS3EncryptionClient.class);
        }
    }

    @Test
    public void testUnrecoverableS3ExceptionMessage()
    {
        assertThat(new UnrecoverableS3OperationException("my-bucket", "tmp/test/path", new IOException("test io exception")))
                .hasMessage("java.io.IOException: test io exception (Bucket: my-bucket, Key: tmp/test/path)");
    }

    @Test
    public void testWebIdentityTokenCredentialsProvider()
            throws Exception
    {
        Configuration config = new Configuration(false);
        config.setBoolean(S3_USE_WEB_IDENTITY_TOKEN_CREDENTIALS_PROVIDER, true);
        try (TrinoS3FileSystem fs = new TrinoS3FileSystem()) {
            fs.initialize(new URI("s3n://test-bucket/"), config);
            assertThat(getAwsCredentialsProvider(fs)).isInstanceOf(WebIdentityTokenCredentialsProvider.class);
        }
    }

    @Test
    public void testCustomCredentialsProvider()
            throws Exception
    {
        Configuration config = new Configuration(false);
        config.set(S3_CREDENTIALS_PROVIDER, TestCredentialsProvider.class.getName());
        try (TrinoS3FileSystem fs = new TrinoS3FileSystem()) {
            fs.initialize(new URI("s3n://test-bucket/"), config);
            assertThat(getAwsCredentialsProvider(fs)).isInstanceOf(TestCredentialsProvider.class);
        }
    }

    @Test
    public void testCustomCredentialsClassCannotBeFound()
            throws Exception
    {
        Configuration config = new Configuration(false);
        config.set(S3_CREDENTIALS_PROVIDER, "com.example.DoesNotExist");
        try (TrinoS3FileSystem fs = new TrinoS3FileSystem()) {
            assertThatThrownBy(() -> fs.initialize(new URI("s3n://test-bucket/"), config))
                    .isInstanceOf(RuntimeException.class)
                    .hasMessage("Error creating an instance of com.example.DoesNotExist for URI s3n://test-bucket/")
                    .cause()
                    .isInstanceOf(ClassNotFoundException.class)
                    .hasMessage("Class com.example.DoesNotExist not found");
        }
    }

    @Test
    public void testUserAgentPrefix()
            throws Exception
    {
        String userAgentPrefix = "agent_prefix";
        Configuration config = new Configuration(false);
        config.set(S3_USER_AGENT_PREFIX, userAgentPrefix);
        try (TrinoS3FileSystem fs = new TrinoS3FileSystem()) {
            fs.initialize(new URI("s3n://test-bucket/"), config);
            ClientConfiguration clientConfig = getFieldValue(fs.getS3Client(), AmazonWebServiceClient.class, "clientConfiguration", ClientConfiguration.class);
            assertThat(clientConfig.getUserAgentSuffix()).isEqualTo("Trino");
            assertThat(clientConfig.getUserAgentPrefix()).isEqualTo(userAgentPrefix);
        }
    }

    @Test
    public void testDefaultS3ClientConfiguration()
            throws Exception
    {
        HiveS3Config defaults = new HiveS3Config();
        try (TrinoS3FileSystem fs = new TrinoS3FileSystem()) {
            fs.initialize(new URI("s3n://test-bucket/"), new Configuration(false));
            ClientConfiguration config = getFieldValue(fs.getS3Client(), AmazonWebServiceClient.class, "clientConfiguration", ClientConfiguration.class);
            assertThat(config.getMaxErrorRetry()).isEqualTo(defaults.getS3MaxErrorRetries());
            assertThat(config.getConnectionTimeout()).isEqualTo(defaults.getS3ConnectTimeout().toMillis());
            assertThat(config.getSocketTimeout()).isEqualTo(defaults.getS3SocketTimeout().toMillis());
            assertThat(config.getMaxConnections()).isEqualTo(defaults.getS3MaxConnections());
            assertThat(config.getUserAgentSuffix()).isEqualTo("Trino");
            assertThat(config.getUserAgentPrefix()).isEqualTo("");
        }
    }

    @Test
    public void testSkipGlacierObjectsEnabled()
            throws Exception
    {
        assertSkipGlacierObjects(true);
        assertSkipGlacierObjects(false);
    }

    @Test
    public void testProxyDefaultsS3ClientConfiguration()
            throws Exception
    {
        HiveS3Config hiveS3Config = new HiveS3Config();

        TrinoS3ConfigurationInitializer configurationInitializer = new TrinoS3ConfigurationInitializer(hiveS3Config);
        Configuration trinoFsConfiguration = new Configuration(false);
        configurationInitializer.initializeConfiguration(trinoFsConfiguration);

        try (TrinoS3FileSystem fs = new TrinoS3FileSystem()) {
            fs.initialize(new URI("s3n://test-bucket/"), trinoFsConfiguration);
            ClientConfiguration config = getFieldValue(fs.getS3Client(), AmazonWebServiceClient.class, "clientConfiguration", ClientConfiguration.class);
            assertThat(config.getProxyHost()).isNull();
            assertThat(config.getProxyPort()).isEqualTo(-1);
            assertThat(config.getProxyProtocol()).isEqualTo(Protocol.HTTP);
            assertThat(config.getNonProxyHosts()).isEqualTo(System.getProperty("http.nonProxyHosts"));
            assertThat(config.getProxyUsername()).isNull();
            assertThat(config.getProxyPassword()).isNull();
            assertThat(config.isPreemptiveBasicProxyAuth()).isFalse();
        }
    }

    @Test
    public void testOnNoHostProxyDefaultsS3ClientConfiguration()
            throws Exception
    {
        HiveS3Config hiveS3Config = new HiveS3Config();
        hiveS3Config.setS3ProxyHost(null);
        hiveS3Config.setS3ProxyPort(40000);
        hiveS3Config.setS3ProxyProtocol("https");
        hiveS3Config.setS3NonProxyHosts(ImmutableList.of("firsthost.com", "secondhost.com"));
        hiveS3Config.setS3ProxyUsername("dummy_username");
        hiveS3Config.setS3ProxyPassword("dummy_password");
        hiveS3Config.setS3PreemptiveBasicProxyAuth(true);

        TrinoS3ConfigurationInitializer configurationInitializer = new TrinoS3ConfigurationInitializer(hiveS3Config);
        Configuration trinoFsConfiguration = new Configuration(false);
        configurationInitializer.initializeConfiguration(trinoFsConfiguration);

        try (TrinoS3FileSystem fs = new TrinoS3FileSystem()) {
            fs.initialize(new URI("s3n://test-bucket/"), trinoFsConfiguration);
            ClientConfiguration config = getFieldValue(fs.getS3Client(), AmazonWebServiceClient.class, "clientConfiguration", ClientConfiguration.class);
            assertThat(config.getProxyHost()).isNull();
            assertThat(config.getProxyPort()).isEqualTo(-1);
            assertThat(config.getProxyProtocol()).isEqualTo(Protocol.HTTP);
            assertThat(config.getNonProxyHosts()).isEqualTo(System.getProperty("http.nonProxyHosts"));
            assertThat(config.getProxyUsername()).isNull();
            assertThat(config.getProxyPassword()).isNull();
            assertThat(config.isPreemptiveBasicProxyAuth()).isFalse();
        }
    }

    @Test
    public void testExplicitProxyS3ClientConfiguration()
            throws Exception
    {
        HiveS3Config hiveS3Config = new HiveS3Config();
        hiveS3Config.setS3ProxyHost("dummy.com");
        hiveS3Config.setS3ProxyPort(40000);
        hiveS3Config.setS3ProxyProtocol("https");
        hiveS3Config.setS3NonProxyHosts(ImmutableList.of("firsthost.com", "secondhost.com"));
        hiveS3Config.setS3ProxyUsername("dummy_username");
        hiveS3Config.setS3ProxyPassword("dummy_password");
        hiveS3Config.setS3PreemptiveBasicProxyAuth(true);

        TrinoS3ConfigurationInitializer configurationInitializer = new TrinoS3ConfigurationInitializer(hiveS3Config);
        Configuration trinoFsConfiguration = new Configuration(false);
        configurationInitializer.initializeConfiguration(trinoFsConfiguration);

        try (TrinoS3FileSystem fs = new TrinoS3FileSystem()) {
            fs.initialize(new URI("s3n://test-bucket/"), trinoFsConfiguration);
            ClientConfiguration config = getFieldValue(fs.getS3Client(), AmazonWebServiceClient.class, "clientConfiguration", ClientConfiguration.class);
            assertThat(config.getProxyHost()).isEqualTo("dummy.com");
            assertThat(config.getProxyPort()).isEqualTo(40000);
            assertThat(config.getProxyProtocol()).isEqualTo(Protocol.HTTPS);
            assertThat(config.getNonProxyHosts()).isEqualTo("firsthost.com|secondhost.com");
            assertThat(config.getProxyUsername()).isEqualTo("dummy_username");
            assertThat(config.getProxyPassword()).isEqualTo("dummy_password");
            assertThat(config.isPreemptiveBasicProxyAuth()).isTrue();
        }
    }

    private static void assertSkipGlacierObjects(boolean skipGlacierObjects)
            throws Exception
    {
        Configuration config = new Configuration(false);
        config.set(S3_SKIP_GLACIER_OBJECTS, String.valueOf(skipGlacierObjects));

        try (TrinoS3FileSystem fs = new TrinoS3FileSystem()) {
            MockAmazonS3 s3 = new MockAmazonS3();
            s3.setHasGlacierObjects(true);
            fs.initialize(new URI("s3n://test-bucket/"), config);
            fs.setS3Client(s3);
            FileStatus[] statuses = fs.listStatus(new Path("s3n://test-bucket/test"));
            assertThat(statuses.length).isEqualTo(skipGlacierObjects ? 2 : 4);
        }
    }

    @Test
    public void testSkipHadoopFolderMarkerObjectsEnabled()
            throws Exception
    {
        Configuration config = new Configuration(false);

        try (TrinoS3FileSystem fs = new TrinoS3FileSystem()) {
            MockAmazonS3 s3 = new MockAmazonS3();
            s3.setHasHadoopFolderMarkerObjects(true);
            fs.initialize(new URI("s3n://test-bucket/"), config);
            fs.setS3Client(s3);
            FileStatus[] statuses = fs.listStatus(new Path("s3n://test-bucket/test"));
            assertThat(statuses.length).isEqualTo(2);
        }
    }

    public static AWSCredentialsProvider getAwsCredentialsProvider(TrinoS3FileSystem fs)
    {
        return getFieldValue(fs.getS3Client(), "awsCredentialsProvider", AWSCredentialsProvider.class);
    }

    private static <T> T getFieldValue(Object instance, String name, Class<T> type)
    {
        return getFieldValue(instance, instance.getClass(), name, type);
    }

    @SuppressWarnings("unchecked")
    private static <T> T getFieldValue(Object instance, Class<?> clazz, String name, Class<T> type)
    {
        try {
            Field field = clazz.getDeclaredField(name);
            checkArgument(field.getType() == type, "expected %s but found %s", type, field.getType());
            field.setAccessible(true);
            return (T) field.get(instance);
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    private static class TestEncryptionMaterialsProvider
            implements EncryptionMaterialsProvider
    {
        private final EncryptionMaterials encryptionMaterials;

        public TestEncryptionMaterialsProvider()
        {
            encryptionMaterials = new EncryptionMaterials(new SecretKeySpec(new byte[] {1, 2, 3}, "AES"));
        }

        @Override
        public void refresh() {}

        @Override
        public EncryptionMaterials getEncryptionMaterials(Map<String, String> materialsDescription)
        {
            return encryptionMaterials;
        }

        @Override
        public EncryptionMaterials getEncryptionMaterials()
        {
            return encryptionMaterials;
        }
    }

    private static class TestCredentialsProvider
            implements AWSCredentialsProvider
    {
        @SuppressWarnings("UnusedParameters")
        public TestCredentialsProvider(URI uri, Configuration conf) {}

        @Override
        public AWSCredentials getCredentials()
        {
            return null;
        }

        @Override
        public void refresh() {}
    }

    @Test
    public void testDefaultAcl()
            throws Exception
    {
        Configuration config = new Configuration(false);

        try (TrinoS3FileSystem fs = new TrinoS3FileSystem()) {
            MockAmazonS3 s3 = new MockAmazonS3();
            String expectedBucketName = "test-bucket";
            fs.initialize(new URI("s3n://" + expectedBucketName + "/"), config);
            fs.setS3Client(s3);
            try (FSDataOutputStream stream = fs.create(new Path("s3n://test-bucket/test"))) {
                // initiate an upload by creating a stream & closing it immediately
            }
            assertThat(CannedAccessControlList.Private).isEqualTo(s3.getAcl());
        }
    }

    @Test
    public void testFullBucketOwnerControlAcl()
            throws Exception
    {
        Configuration config = new Configuration(false);
        config.set(S3_ACL_TYPE, "BUCKET_OWNER_FULL_CONTROL");

        try (TrinoS3FileSystem fs = new TrinoS3FileSystem()) {
            MockAmazonS3 s3 = new MockAmazonS3();
            String expectedBucketName = "test-bucket";
            fs.initialize(new URI("s3n://" + expectedBucketName + "/"), config);
            fs.setS3Client(s3);
            try (FSDataOutputStream stream = fs.create(new Path("s3n://test-bucket/test"))) {
                // initiate an upload by creating a stream & closing it immediately
            }
            assertThat(CannedAccessControlList.BucketOwnerFullControl).isEqualTo(s3.getAcl());
        }
    }

    @Test
    public void testStreamingUpload()
            throws Exception
    {
        Configuration config = new Configuration(false);
        config.set(S3_STREAMING_UPLOAD_ENABLED, "true");

        try (TrinoS3FileSystem fs = new TrinoS3FileSystem()) {
            MockAmazonS3 s3 = new MockAmazonS3();
            String expectedBucketName = "test-bucket";
            config.set(S3_STREAMING_UPLOAD_PART_SIZE, "128");
            fs.initialize(new URI("s3n://" + expectedBucketName + "/"), config);
            fs.setS3Client(s3);
            String objectKey = "test";
            try (FSDataOutputStream stream = fs.create(new Path("s3n://test-bucket/" + objectKey))) {
                stream.write('a');
                stream.write("foo".repeat(21).getBytes(US_ASCII)); // 63 bytes = "foo" * 21
                stream.write("bar".repeat(44).getBytes(US_ASCII)); // 132 bytes = "bar" * 44
                stream.write("orange".repeat(25).getBytes(US_ASCII), 6, 132); // 132 bytes = "orange" * 22
            }

            List<UploadPartRequest> parts = s3.getUploadParts();
            assertThat(parts).size().isEqualTo(3);

            InputStream concatInputStream = parts.stream()
                    .map(UploadPartRequest::getInputStream)
                    .reduce(new ByteArrayInputStream(new byte[0]), SequenceInputStream::new);
            String data = new String(concatInputStream.readAllBytes(), US_ASCII);
            assertThat(data).isEqualTo("a" + "foo".repeat(21) + "bar".repeat(44) + "orange".repeat(22));
        }
    }

    @Test
    public void testEmptyDirectory()
            throws Exception
    {
        try (TrinoS3FileSystem fs = new TrinoS3FileSystem()) {
            MockAmazonS3 s3 = new MockAmazonS3()
            {
                @Override
                public ObjectMetadata getObjectMetadata(GetObjectMetadataRequest getObjectMetadataRequest)
                {
                    if (getObjectMetadataRequest.getKey().equals("empty-dir/")) {
                        ObjectMetadata objectMetadata = new ObjectMetadata();
                        objectMetadata.setContentType(S3_DIRECTORY_OBJECT_CONTENT_TYPE);
                        return objectMetadata;
                    }
                    return super.getObjectMetadata(getObjectMetadataRequest);
                }
            };
            fs.initialize(new URI("s3n://test-bucket/"), new Configuration(false));
            fs.setS3Client(s3);

            FileStatus fileStatus = fs.getFileStatus(new Path("s3n://test-bucket/empty-dir/"));
            assertThat(fileStatus.isDirectory()).isTrue();

            fileStatus = fs.getFileStatus(new Path("s3n://test-bucket/empty-dir"));
            assertThat(fileStatus.isDirectory()).isTrue();
        }
    }

    @Test
    public void testListPrefixModes()
            throws Exception
    {
        S3ObjectSummary rootObject = new S3ObjectSummary();
        rootObject.setStorageClass(StorageClass.Standard.toString());
        rootObject.setKey("standard-object-at-root.txt");
        rootObject.setLastModified(new Date());

        S3ObjectSummary childObject = new S3ObjectSummary();
        childObject.setStorageClass(StorageClass.Standard.toString());
        childObject.setKey("prefix/child-object.txt");
        childObject.setLastModified(new Date());

        try (TrinoS3FileSystem fs = new TrinoS3FileSystem()) {
            MockAmazonS3 s3 = new MockAmazonS3()
            {
                @Override
                public ListObjectsV2Result listObjectsV2(ListObjectsV2Request listObjectsV2Request)
                {
                    ListObjectsV2Result listing = new ListObjectsV2Result();
                    // Shallow listing
                    if ("/".equals(listObjectsV2Request.getDelimiter())) {
                        listing.getCommonPrefixes().add("prefix");
                        listing.getObjectSummaries().add(rootObject);
                        return listing;
                    }
                    // Recursive listing of object keys only
                    listing.getObjectSummaries().addAll(Arrays.asList(childObject, rootObject));
                    return listing;
                }
            };
            Path rootPath = new Path("s3n://test-bucket/");
            fs.initialize(rootPath.toUri(), new Configuration(false));
            fs.setS3Client(s3);

            List<LocatedFileStatus> shallowAll = remoteIteratorToList(fs.listLocatedStatus(rootPath));
            assertThat(shallowAll).hasSize(2);
            assertThat(shallowAll.get(0).isDirectory()).isTrue();
            assertThat(shallowAll.get(1).isDirectory()).isFalse();
            assertThat(shallowAll.get(0).getPath()).isEqualTo(new Path(rootPath, "prefix"));
            assertThat(shallowAll.get(1).getPath()).isEqualTo(new Path(rootPath, rootObject.getKey()));

            List<LocatedFileStatus> shallowFiles = remoteIteratorToList(fs.listFiles(rootPath, false));
            assertThat(shallowFiles).hasSize(1);
            assertThat(shallowFiles.get(0).isDirectory()).isFalse();
            assertThat(shallowFiles.get(0).getPath()).isEqualTo(new Path(rootPath, rootObject.getKey()));

            List<LocatedFileStatus> recursiveFiles = remoteIteratorToList(fs.listFiles(rootPath, true));
            assertThat(recursiveFiles).hasSize(2);
            assertThat(recursiveFiles.get(0).isDirectory()).isFalse();
            assertThat(recursiveFiles.get(1).isDirectory()).isFalse();
            assertThat(recursiveFiles.get(0).getPath()).isEqualTo(new Path(rootPath, childObject.getKey()));
            assertThat(recursiveFiles.get(1).getPath()).isEqualTo(new Path(rootPath, rootObject.getKey()));
        }
    }

    @Test
    public void testThatTrinoS3FileSystemReportsConsumedMemory()
            throws IOException
    {
        TestMemoryReservationHandler memoryReservationHandler = new TestMemoryReservationHandler();
        AggregatedMemoryContext memoryContext = newRootAggregatedMemoryContext(memoryReservationHandler, 1024 * 1000 * 1000);
        try (TrinoS3FileSystem fs = new TrinoS3FileSystem()) {
            MockAmazonS3 s3 = new MockAmazonS3();
            Path rootPath = new Path("s3n://test-bucket/");
            fs.initialize(rootPath.toUri(), new Configuration(false));
            fs.setS3Client(s3);
            OutputStream outputStream = fs.create(new Path("s3n://test-bucket/test1"), memoryContext);
            outputStream.write(new byte[] {1, 2, 3, 4, 5, 6}, 0, 6);
            outputStream.close();
        }
        assertThat(memoryReservationHandler.getReserved()).isEqualTo(0);
        assertThat(memoryReservationHandler.getMaxReserved()).isGreaterThan(0);
    }

    private static List<LocatedFileStatus> remoteIteratorToList(RemoteIterator<LocatedFileStatus> statuses)
            throws IOException
    {
        List<LocatedFileStatus> result = new ArrayList<>();
        while (statuses.hasNext()) {
            result.add(statuses.next());
        }
        return result;
    }

    private static class TestMemoryReservationHandler
            implements MemoryReservationHandler
    {
        private long reserved;
        private long maxReserved;

        @Override
        public ListenableFuture<Void> reserveMemory(String allocationTag, long delta)
        {
            reserved += delta;
            if (delta > maxReserved) {
                maxReserved = delta;
            }
            return null;
        }

        @Override
        public boolean tryReserveMemory(String allocationTag, long delta)
        {
            reserved += delta;
            if (delta > maxReserved) {
                maxReserved = delta;
            }
            return true;
        }

        public long getReserved()
        {
            return reserved;
        }

        public long getMaxReserved()
        {
            return maxReserved;
        }
    }
}
