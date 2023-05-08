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
package io.trino.hdfs.cos;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.google.common.collect.ImmutableSet;
import io.airlift.testing.TempFile;
import io.trino.hdfs.DynamicConfigurationProvider;
import io.trino.hdfs.DynamicHdfsConfiguration;
import io.trino.hdfs.HdfsConfig;
import io.trino.hdfs.HdfsConfiguration;
import io.trino.hdfs.HdfsConfigurationInitializer;
import io.trino.hdfs.HdfsContext;
import io.trino.hdfs.s3.HiveS3Config;
import io.trino.hdfs.s3.TrinoS3ConfigurationInitializer;
import io.trino.spi.security.ConnectorIdentity;
import org.apache.hadoop.conf.Configuration;
import org.testng.annotations.Test;

import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.Properties;

import static io.airlift.testing.Assertions.assertInstanceOf;
import static io.trino.hdfs.s3.TestTrinoS3FileSystem.getAwsCredentialsProvider;
import static org.testng.Assert.assertEquals;

public class TestHiveCosServiceConfigurationProvider
{
    @Test
    public void testPerBucketCredentialsIntegrated()
            throws Exception
    {
        HdfsConfiguration hiveHdfsConfiguration = getCosHdfsConfiguration();

        try (TrinoCosFileSystem fs = new TrinoCosFileSystem()) {
            verifyStaticCredentials(hiveHdfsConfiguration, fs, "cos://test-bucket/", "test-bucket", "test_access_key", "test_secret_key");
            verifyStaticCredentials(hiveHdfsConfiguration, fs, "cos://test-bucket.a/", "test-bucket", "cos_a_access_key", "cos_a_secret_key");
            verifyStaticCredentials(hiveHdfsConfiguration, fs, "cos://test-bucket.b/", "test-bucket", "cos_b_access_key", "cos_b_secret_key");
            verifyStaticCredentials(hiveHdfsConfiguration, fs, "cos://a/", "a", "test_access_key", "test_secret_key");
        }
    }

    private HdfsConfiguration getCosHdfsConfiguration()
            throws IOException
    {
        HdfsConfigurationInitializer initializer = new HdfsConfigurationInitializer(new HdfsConfig(), ImmutableSet.of(
                new TrinoS3ConfigurationInitializer(new HiveS3Config()
                        .setS3AwsAccessKey("test_access_key")
                        .setS3AwsSecretKey("test_secret_key")),
                new CosConfigurationInitializer()));

        DynamicConfigurationProvider provider;
        try (TempFile cosServiceConfig = new TempFile()) {
            Properties cosServiceProperties = new Properties();
            cosServiceProperties.put("a.access-key", "cos_a_access_key");
            cosServiceProperties.put("a.secret-key", "cos_a_secret_key");
            cosServiceProperties.put("b.access-key", "cos_b_access_key");
            cosServiceProperties.put("b.secret-key", "cos_b_secret_key");
            try (FileOutputStream out = new FileOutputStream(cosServiceConfig.file())) {
                cosServiceProperties.store(out, "S3 bucket");
            }

            provider = new CosServiceConfigurationProvider(new HiveCosServiceConfig().setServiceConfig(cosServiceConfig.file()));
        }

        return new DynamicHdfsConfiguration(initializer, ImmutableSet.of(provider));
    }

    private static void verifyStaticCredentials(HdfsConfiguration hiveHdfsConfiguration,
            TrinoCosFileSystem fileSystem,
            String uri,
            String expectedBucket,
            String expectedAccessKey,
            String expectedSecretKey)
            throws IOException
    {
        HdfsContext hdfsContext = new HdfsContext(ConnectorIdentity.forUser("test").build());
        Configuration configuration = hiveHdfsConfiguration.getConfiguration(hdfsContext, URI.create(uri));
        fileSystem.initialize(URI.create(uri), configuration);
        assertEquals(fileSystem.getBucketName(URI.create(uri)), expectedBucket);
        AWSCredentialsProvider awsCredentialsProvider = getAwsCredentialsProvider(fileSystem);
        assertInstanceOf(awsCredentialsProvider, AWSStaticCredentialsProvider.class);
        assertEquals(awsCredentialsProvider.getCredentials().getAWSAccessKeyId(), expectedAccessKey);
        assertEquals(awsCredentialsProvider.getCredentials().getAWSSecretKey(), expectedSecretKey);
    }
}
