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
package io.trino.plugin.hive.s3;

import com.amazonaws.AmazonWebServiceClient;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSSessionCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
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
import io.trino.plugin.hive.s3.TrinoS3FileSystem.UnrecoverableS3OperationException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.testng.SkipException;
import org.testng.annotations.Test;

import javax.crypto.spec.SecretKeySpec;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
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
import static com.google.common.base.Strings.repeat;
import static com.google.common.io.ByteStreams.toByteArray;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.airlift.testing.Assertions.assertInstanceOf;
import static io.trino.plugin.hive.s3.TrinoS3FileSystem.S3_ACCESS_KEY;
import static io.trino.plugin.hive.s3.TrinoS3FileSystem.S3_ACL_TYPE;
import static io.trino.plugin.hive.s3.TrinoS3FileSystem.S3_CREDENTIALS_PROVIDER;
import static io.trino.plugin.hive.s3.TrinoS3FileSystem.S3_ENCRYPTION_MATERIALS_PROVIDER;
import static io.trino.plugin.hive.s3.TrinoS3FileSystem.S3_ENDPOINT;
import static io.trino.plugin.hive.s3.TrinoS3FileSystem.S3_EXTERNAL_ID;
import static io.trino.plugin.hive.s3.TrinoS3FileSystem.S3_IAM_ROLE;
import static io.trino.plugin.hive.s3.TrinoS3FileSystem.S3_KMS_KEY_ID;
import static io.trino.plugin.hive.s3.TrinoS3FileSystem.S3_MAX_BACKOFF_TIME;
import static io.trino.plugin.hive.s3.TrinoS3FileSystem.S3_MAX_CLIENT_RETRIES;
import static io.trino.plugin.hive.s3.TrinoS3FileSystem.S3_MAX_RETRY_TIME;
import static io.trino.plugin.hive.s3.TrinoS3FileSystem.S3_PATH_STYLE_ACCESS;
import static io.trino.plugin.hive.s3.TrinoS3FileSystem.S3_PIN_CLIENT_TO_CURRENT_REGION;
import static io.trino.plugin.hive.s3.TrinoS3FileSystem.S3_SECRET_KEY;
import static io.trino.plugin.hive.s3.TrinoS3FileSystem.S3_SESSION_TOKEN;
import static io.trino.plugin.hive.s3.TrinoS3FileSystem.S3_SKIP_GLACIER_OBJECTS;
import static io.trino.plugin.hive.s3.TrinoS3FileSystem.S3_STAGING_DIRECTORY;
import static io.trino.plugin.hive.s3.TrinoS3FileSystem.S3_STREAMING_UPLOAD_ENABLED;
import static io.trino.plugin.hive.s3.TrinoS3FileSystem.S3_STREAMING_UPLOAD_PART_SIZE;
import static io.trino.plugin.hive.s3.TrinoS3FileSystem.S3_USER_AGENT_PREFIX;
import static java.net.HttpURLConnection.HTTP_FORBIDDEN;
import static java.net.HttpURLConnection.HTTP_INTERNAL_ERROR;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.nio.file.Files.createTempDirectory;
import static java.nio.file.Files.createTempFile;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

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
            assertEquals(credentials.getAWSAccessKeyId(), "testAccess");
            assertEquals(credentials.getAWSSecretKey(), "testSecret");
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
            assertEquals(credentials.getAWSAccessKeyId(), "test_access_key");
            assertEquals(credentials.getAWSSecretKey(), "test_secret_key");
            assertThat(credentials).isNotInstanceOf(AWSSessionCredentials.class);
        }

        config.set(S3_SESSION_TOKEN, "test_token");
        try (TrinoS3FileSystem fs = new TrinoS3FileSystem()) {
            AWSCredentials credentials = getStaticCredentials(config, fs, "s3n://test-bucket/");
            assertEquals(credentials.getAWSAccessKeyId(), "test_access_key");
            assertEquals(credentials.getAWSSecretKey(), "test_secret_key");
            assertThat(credentials).isInstanceOfSatisfying(AWSSessionCredentials.class, sessionCredentials ->
                    assertEquals(sessionCredentials.getSessionToken(), "test_token"));
        }
    }

    private static AWSCredentials getStaticCredentials(Configuration config, TrinoS3FileSystem fileSystem, String uri)
            throws IOException, URISyntaxException
    {
        fileSystem.initialize(new URI(uri), config);
        AWSCredentialsProvider awsCredentialsProvider = getAwsCredentialsProvider(fileSystem);
        assertInstanceOf(awsCredentialsProvider, AWSStaticCredentialsProvider.class);
        return awsCredentialsProvider.getCredentials();
    }

    @Test(expectedExceptions = VerifyException.class, expectedExceptionsMessageRegExp = "Invalid configuration: either endpoint can be set or S3 client can be pinned to the current region")
    public void testEndpointWithPinToCurrentRegionConfiguration()
            throws Exception
    {
        Configuration config = new Configuration(false);
        config.set(S3_ENDPOINT, "test.example.endpoint.com");
        config.set(S3_PIN_CLIENT_TO_CURRENT_REGION, "true");
        try (TrinoS3FileSystem fs = new TrinoS3FileSystem()) {
            fs.initialize(new URI("s3a://test-bucket/"), config);
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
            assertInstanceOf(tokenService, DefaultAWSCredentialsProviderChain.class);
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
            assertInstanceOf(tokenService, AWSStaticCredentialsProvider.class);

            AWSCredentials credentials = tokenService.getCredentials();
            assertEquals(credentials.getAWSAccessKeyId(), "test_access_key");
            assertEquals(credentials.getAWSSecretKey(), "test_secret_key");
        }
    }

    private static AWSCredentialsProvider getStsCredentialsProvider(TrinoS3FileSystem fs, String expectedRole)
    {
        AWSCredentialsProvider awsCredentialsProvider = getAwsCredentialsProvider(fs);
        assertInstanceOf(awsCredentialsProvider, STSAssumeRoleSessionCredentialsProvider.class);

        assertEquals(getFieldValue(awsCredentialsProvider, "roleArn", String.class), expectedRole);

        AWSSecurityTokenService tokenService = getFieldValue(awsCredentialsProvider, "securityTokenService", AWSSecurityTokenService.class);
        assertInstanceOf(tokenService, AWSSecurityTokenServiceClient.class);
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
            assertInstanceOf(awsCredentialsProvider, STSAssumeRoleSessionCredentialsProvider.class);
            assertEquals(getFieldValue(awsCredentialsProvider, "roleArn", String.class), "role");
            assertEquals(getFieldValue(awsCredentialsProvider, "roleExternalId", String.class), "externalId");
        }
    }

    @Test
    public void testDefaultCredentials()
            throws Exception
    {
        Configuration config = new Configuration(false);

        try (TrinoS3FileSystem fs = new TrinoS3FileSystem()) {
            fs.initialize(new URI("s3n://test-bucket/"), config);
            assertInstanceOf(getAwsCredentialsProvider(fs), DefaultAWSCredentialsProviderChain.class);
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
            assertTrue(clientOptions.isPathStyleAccess());
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
            assertEquals(fs.getBucketName(uri), expectedBucketName);
            fs.initialize(uri, config);
            fs.setS3Client(s3);
            fs.getS3ObjectMetadata(new Path("/test/path"));
            assertEquals(expectedBucketName, s3.getGetObjectMetadataRequest().getBucketName());
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
                assertInstanceOf(expected, AmazonS3Exception.class);
                assertEquals(((AmazonS3Exception) expected).getStatusCode(), HTTP_INTERNAL_ERROR);
                assertEquals(TrinoS3FileSystem.getFileSystemStats().getReadRetries().getTotalCount(), maxRetries);
                assertEquals(TrinoS3FileSystem.getFileSystemStats().getGetObjectRetries().getTotalCount(), (maxRetries + 1L) * maxRetries);
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
            assertInstanceOf(expected, AmazonS3Exception.class);
            assertEquals(((AmazonS3Exception) expected).getStatusCode(), HTTP_INTERNAL_ERROR);
            assertEquals(TrinoS3FileSystem.getFileSystemStats().getGetMetadataRetries().getTotalCount(), maxRetries);
        }
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Test
    public void testReadNotFound()
            throws Exception
    {
        try (TrinoS3FileSystem fs = new TrinoS3FileSystem()) {
            MockAmazonS3 s3 = new MockAmazonS3();
            s3.setGetObjectHttpErrorCode(HTTP_NOT_FOUND);
            fs.initialize(new URI("s3n://test-bucket/"), new Configuration(false));
            fs.setS3Client(s3);
            try (FSDataInputStream inputStream = fs.open(new Path("s3n://test-bucket/test"))) {
                assertThatThrownBy(() -> inputStream.read())
                        .isInstanceOf(IOException.class)
                        .hasMessageContaining("Failing getObject call with " + HTTP_NOT_FOUND);
            }
        }
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Test
    public void testReadForbidden()
            throws Exception
    {
        try (TrinoS3FileSystem fs = new TrinoS3FileSystem()) {
            MockAmazonS3 s3 = new MockAmazonS3();
            s3.setGetObjectHttpErrorCode(HTTP_FORBIDDEN);
            fs.initialize(new URI("s3n://test-bucket/"), new Configuration(false));
            fs.setS3Client(s3);
            try (FSDataInputStream inputStream = fs.open(new Path("s3n://test-bucket/test"))) {
                assertThatThrownBy(inputStream::read)
                        .isInstanceOf(IOException.class)
                        .hasMessageContaining("Failing getObject call with " + HTTP_FORBIDDEN);
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
            assertTrue(Files.exists(staging));
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
                throw new SkipException("Filesystem does not support symlinks", e);
            }

            try (TrinoS3FileSystem fs = new TrinoS3FileSystem()) {
                MockAmazonS3 s3 = new MockAmazonS3();
                Configuration conf = new Configuration(false);
                conf.set(S3_STAGING_DIRECTORY, link.toString());
                fs.initialize(new URI("s3n://test-bucket/"), conf);
                fs.setS3Client(s3);
                FSDataOutputStream stream = fs.create(new Path("s3n://test-bucket/test"));
                stream.close();
                assertTrue(Files.exists(link));
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
                assertEquals(inputStream.read(), -1);
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
            assertNull(fs.getS3ObjectMetadata(new Path("s3n://test-bucket/test")));
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
            assertInstanceOf(fs.getS3Client(), AmazonS3EncryptionClient.class);
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
            assertInstanceOf(fs.getS3Client(), AmazonS3EncryptionClient.class);
        }
    }

    @Test(expectedExceptions = UnrecoverableS3OperationException.class, expectedExceptionsMessageRegExp = ".*\\Q (Path: /tmp/test/path)\\E")
    public void testUnrecoverableS3ExceptionMessage()
            throws Exception
    {
        throw new UnrecoverableS3OperationException(new Path("/tmp/test/path"), new IOException("test io exception"));
    }

    @Test
    public void testCustomCredentialsProvider()
            throws Exception
    {
        Configuration config = new Configuration(false);
        config.set(S3_CREDENTIALS_PROVIDER, TestCredentialsProvider.class.getName());
        try (TrinoS3FileSystem fs = new TrinoS3FileSystem()) {
            fs.initialize(new URI("s3n://test-bucket/"), config);
            assertInstanceOf(getAwsCredentialsProvider(fs), TestCredentialsProvider.class);
        }
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "Error creating an instance of .*")
    public void testCustomCredentialsClassCannotBeFound()
            throws Exception
    {
        Configuration config = new Configuration(false);
        config.set(S3_CREDENTIALS_PROVIDER, "com.example.DoesNotExist");
        try (TrinoS3FileSystem fs = new TrinoS3FileSystem()) {
            fs.initialize(new URI("s3n://test-bucket/"), config);
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
            assertEquals(clientConfig.getUserAgentSuffix(), "Trino");
            assertEquals(clientConfig.getUserAgentPrefix(), userAgentPrefix);
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
            assertEquals(config.getMaxErrorRetry(), defaults.getS3MaxErrorRetries());
            assertEquals(config.getConnectionTimeout(), defaults.getS3ConnectTimeout().toMillis());
            assertEquals(config.getSocketTimeout(), defaults.getS3SocketTimeout().toMillis());
            assertEquals(config.getMaxConnections(), defaults.getS3MaxConnections());
            assertEquals(config.getUserAgentSuffix(), "Trino");
            assertEquals(config.getUserAgentPrefix(), "");
        }
    }

    @Test
    public void testSkipGlacierObjectsEnabled()
            throws Exception
    {
        assertSkipGlacierObjects(true);
        assertSkipGlacierObjects(false);
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
            assertEquals(statuses.length, skipGlacierObjects ? 2 : 4);
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
            assertEquals(statuses.length, 2);
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
        public void refresh()
        {
        }

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
            assertEquals(CannedAccessControlList.Private, s3.getAcl());
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
            assertEquals(CannedAccessControlList.BucketOwnerFullControl, s3.getAcl());
        }
    }

    @Test
    public void testStreamingUpload()
            throws Exception
    {
        Configuration config = new Configuration(false);
        config.set(S3_STREAMING_UPLOAD_ENABLED, "true");
        config.set(S3_STREAMING_UPLOAD_PART_SIZE, "10");

        try (TrinoS3FileSystem fs = new TrinoS3FileSystem()) {
            MockAmazonS3 s3 = new MockAmazonS3();
            String expectedBucketName = "test-bucket";
            fs.initialize(new URI("s3n://" + expectedBucketName + "/"), config);
            fs.setS3Client(s3);
            try (FSDataOutputStream stream = fs.create(new Path("s3n://test-bucket/test"))) {
                stream.write('a');
                stream.write(repeat("foo", 2).getBytes(US_ASCII));
                stream.write(repeat("bar", 3).getBytes(US_ASCII));
                stream.write(repeat("orange", 4).getBytes(US_ASCII), 6, 12);
            }

            List<UploadPartRequest> parts = s3.getUploadParts();
            assertThat(parts).size().isEqualTo(3);

            InputStream concatInputStream = parts.stream()
                    .map(UploadPartRequest::getInputStream)
                    .reduce(new ByteArrayInputStream(new byte[0]), SequenceInputStream::new);
            String data = new String(toByteArray(concatInputStream), US_ASCII);
            assertEquals(data, "afoofoobarbarbarorangeorange");
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
            assertTrue(fileStatus.isDirectory());

            fileStatus = fs.getFileStatus(new Path("s3n://test-bucket/empty-dir"));
            assertTrue(fileStatus.isDirectory());
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
            assertEquals(shallowAll.size(), 2);
            assertTrue(shallowAll.get(0).isDirectory());
            assertFalse(shallowAll.get(1).isDirectory());
            assertEquals(shallowAll.get(0).getPath(), new Path(rootPath, "prefix"));
            assertEquals(shallowAll.get(1).getPath(), new Path(rootPath, rootObject.getKey()));

            List<LocatedFileStatus> shallowFiles = remoteIteratorToList(fs.listFiles(rootPath, false));
            assertEquals(shallowFiles.size(), 1);
            assertFalse(shallowFiles.get(0).isDirectory());
            assertEquals(shallowFiles.get(0).getPath(), new Path(rootPath, rootObject.getKey()));

            List<LocatedFileStatus> recursiveFiles = remoteIteratorToList(fs.listFiles(rootPath, true));
            assertEquals(recursiveFiles.size(), 2);
            assertFalse(recursiveFiles.get(0).isDirectory());
            assertFalse(recursiveFiles.get(1).isDirectory());
            assertEquals(recursiveFiles.get(0).getPath(), new Path(rootPath, childObject.getKey()));
            assertEquals(recursiveFiles.get(1).getPath(), new Path(rootPath, rootObject.getKey()));
        }
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
}
