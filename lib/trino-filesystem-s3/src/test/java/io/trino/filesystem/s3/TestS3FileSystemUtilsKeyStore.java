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

import io.trino.filesystem.s3.keystore.KeyStoreCredentialAliasResolver;
import io.trino.filesystem.s3.keystore.KeyStoreTestFixture;
import io.trino.spi.TrinoException;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.CONFIGURATION_INVALID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestS3FileSystemUtilsKeyStore
{
    @Test
    public void testExplicitAliasCredentials()
            throws Exception
    {
        Path keystorePath = KeyStoreTestFixture.createKeyStore(
                Map.of(
                        "s3.access.key", "access-from-alias",
                        "s3.secret.key", "secret-from-alias"),
                "none");

        S3FileSystemConfig config = new S3FileSystemConfig()
                .setKeystorePath(keystorePath.toString())
                .setKeystorePassword("none")
                .setAwsAccessKeyAlias("s3.access.key")
                .setAwsSecretKeyAlias("s3.secret.key");

        KeyStoreCredentialAliasResolver resolver = S3KeystoreCredentials.createAliasResolver(config).orElseThrow();

        Optional<AwsCredentialsProvider> credentialsProvider = S3FileSystemUtils.createCredentialsProvider(
                config,
                Optional.of(resolver),
                Optional.empty());

        assertThat(credentialsProvider).isPresent();
        AwsBasicCredentials credentials = (AwsBasicCredentials) credentialsProvider.get().resolveCredentials();
        assertThat(credentials.accessKeyId()).isEqualTo("access-from-alias");
        assertThat(credentials.secretAccessKey()).isEqualTo("secret-from-alias");
    }

    @Test
    public void testBucketPrefixCredentials()
            throws Exception
    {
        Path keystorePath = KeyStoreTestFixture.createKeyStore(
                Map.of(
                        "fs.s3a.bucket.test-bucket.access.key", "bucket-access",
                        "fs.s3a.bucket.test-bucket.secret.key", "bucket-secret"),
                "none");

        S3FileSystemConfig config = new S3FileSystemConfig()
                .setKeystorePath(keystorePath.toString())
                .setKeystorePassword("none")
                .setKeystoreBucketKeyPrefix("fs.s3a.bucket.");

        KeyStoreCredentialAliasResolver resolver = S3KeystoreCredentials.createAliasResolver(config).orElseThrow();

        Optional<AwsCredentialsProvider> credentialsProvider = S3FileSystemUtils.createCredentialsProvider(
                config,
                Optional.of(resolver),
                Optional.of("test-bucket"));

        assertThat(credentialsProvider).isPresent();
        AwsBasicCredentials credentials = (AwsBasicCredentials) credentialsProvider.get().resolveCredentials();
        assertThat(credentials.accessKeyId()).isEqualTo("bucket-access");
        assertThat(credentials.secretAccessKey()).isEqualTo("bucket-secret");
    }

    @Test
    public void testPartialPlaintextCredentialsRejected()
    {
        S3FileSystemConfig config = new S3FileSystemConfig()
                .setAwsAccessKey("only-access");

        assertThatThrownBy(() -> S3FileSystemUtils.createCredentialsProvider(
                config,
                Optional.empty(),
                Optional.empty()))
                .isInstanceOfSatisfying(TrinoException.class, e -> {
                    assertThat(e.getErrorCode()).isEqualTo(CONFIGURATION_INVALID.toErrorCode());
                    assertThat(e).hasMessageContaining("Both S3 access key and secret key must be configured");
                });
    }
}
