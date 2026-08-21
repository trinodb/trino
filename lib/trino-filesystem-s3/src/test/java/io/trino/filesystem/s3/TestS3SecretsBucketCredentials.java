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

import io.airlift.secrets.keystore.KeystoreSecretProvider;
import io.airlift.secrets.keystore.KeystoreSecretProviderConfig;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.filesystem.s3.secrets.KeyStoreTestFixture;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

import static io.trino.filesystem.s3.S3FileSystemUtils.createCredentialsProvider;
import static org.assertj.core.api.Assertions.assertThat;

public class TestS3SecretsBucketCredentials
{
    @Test
    public void testBucketPrefixLoaderWiring()
            throws Exception
    {
        Path keystorePath = KeyStoreTestFixture.createHadoopKeyStore(
                Map.of(
                        "fs.s3a.bucket.orders.access.key", "bucket-access",
                        "fs.s3a.bucket.orders.secret.key", "bucket-secret"),
                "none");

        KeystoreSecretProvider keystoreProvider = new KeystoreSecretProvider(new KeystoreSecretProviderConfig()
                .setKeyStoreFilePath(keystorePath.toString())
                .setKeyStoreType("JCEKS")
                .setKeyStorePassword("none"));

        S3FileSystemConfig config = new S3FileSystemConfig()
                .setRegion("us-east-1")
                .setSecretsBucketKeyPrefix("fs.s3a.bucket.")
                .setSecretsProvider("keystore");

        S3SecretsCredentialResolver secretsCredentialResolver = new S3SecretsCredentialResolver(
                (_, key) -> keystoreProvider.resolveSecretValue(key),
                config);

        S3FileSystemLoader loader = new S3FileSystemLoader(OpenTelemetry.noop(), config, new S3FileSystemStats(), secretsCredentialResolver);
        S3BucketCredentialFileSystemLoader.BucketFileSystemFactory bucketFactory = loader.createFactoryForBucket("orders");

        Optional<AwsCredentialsProvider> credentialsProvider = createCredentialsProvider(
                config,
                Optional.of(secretsCredentialResolver),
                Optional.of("orders"));

        assertThat(credentialsProvider).isPresent();
        AwsBasicCredentials credentials = (AwsBasicCredentials) credentialsProvider.get().resolveCredentials();
        assertThat(credentials.accessKeyId()).isEqualTo("bucket-access");
        assertThat(credentials.secretAccessKey()).isEqualTo("bucket-secret");
        assertThat(bucketFactory).isNotNull();
    }
}
