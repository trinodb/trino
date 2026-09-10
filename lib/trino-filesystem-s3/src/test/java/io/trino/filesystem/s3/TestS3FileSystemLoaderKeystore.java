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
package io.trino.filesystem.s3;

import io.opentelemetry.api.OpenTelemetry;
import io.trino.filesystem.s3.keystore.KeyStoreTestFixture;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

import static io.trino.filesystem.s3.S3FileSystemUtils.createCredentialsProvider;
import static org.assertj.core.api.Assertions.assertThat;

public class TestS3FileSystemLoaderKeystore
{
    @Test
    public void testGlobalAliasLoaderWiring()
            throws Exception
    {
        Path keystorePath = KeyStoreTestFixture.createKeyStore(
                Map.of(
                        "s3.access.key", "loader-access",
                        "s3.secret.key", "loader-secret"),
                "none");

        S3FileSystemConfig config = new S3FileSystemConfig()
                .setRegion("us-east-1")
                .setKeystorePath(keystorePath.toString())
                .setKeystorePassword("none")
                .setAwsAccessKeyAlias("s3.access.key")
                .setAwsSecretKeyAlias("s3.secret.key");

        S3FileSystemLoader loader = new S3FileSystemLoader(OpenTelemetry.noop(), config, new S3FileSystemStats());
        Optional<AwsCredentialsProvider> credentialsProvider = createCredentialsProvider(
                config,
                loader.aliasResolver(),
                Optional.empty());

        assertThat(credentialsProvider).isPresent();
        AwsBasicCredentials credentials = (AwsBasicCredentials) credentialsProvider.get().resolveCredentials();
        assertThat(credentials.accessKeyId()).isEqualTo("loader-access");
        assertThat(credentials.secretAccessKey()).isEqualTo("loader-secret");
    }

    @Test
    public void testBucketPrefixLoaderWiring()
            throws Exception
    {
        Path keystorePath = KeyStoreTestFixture.createKeyStore(
                Map.of(
                        "fs.s3a.bucket.orders.access.key", "bucket-access",
                        "fs.s3a.bucket.orders.secret.key", "bucket-secret"),
                "none");

        S3FileSystemConfig config = new S3FileSystemConfig()
                .setRegion("us-east-1")
                .setKeystorePath(keystorePath.toString())
                .setKeystorePassword("none")
                .setKeystoreBucketKeyPrefix("fs.s3a.bucket.");

        S3FileSystemLoader loader = new S3FileSystemLoader(OpenTelemetry.noop(), config, new S3FileSystemStats());
        S3BucketCredentialFileSystemLoader.BucketFileSystemFactory bucketFactory = loader.createFactoryForBucket("orders");

        Optional<AwsCredentialsProvider> credentialsProvider = createCredentialsProvider(
                config,
                loader.aliasResolver(),
                Optional.of("orders"));

        assertThat(credentialsProvider).isPresent();
        AwsBasicCredentials credentials = (AwsBasicCredentials) credentialsProvider.get().resolveCredentials();
        assertThat(credentials.accessKeyId()).isEqualTo("bucket-access");
        assertThat(credentials.secretAccessKey()).isEqualTo("bucket-secret");
        assertThat(bucketFactory).isNotNull();
    }
}
