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
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class TestS3KeystoreCredentials
{
    @Test
    public void testEnvironmentKeystorePasswordFallback()
            throws Exception
    {
        Path keystorePath = KeyStoreTestFixture.createKeyStore(
                Map.of("s3.access.key", "env-access"),
                "env-password");

        S3FileSystemConfig config = new S3FileSystemConfig()
                .setKeystorePath(keystorePath.toString())
                .setAwsAccessKeyAlias("s3.access.key")
                .setAwsSecretKeyAlias("s3.access.key");

        S3KeystoreCredentials.PasswordResolution resolution = S3KeystoreCredentials.resolveKeystorePassword(
                config,
                name -> name.equals(S3KeystoreCredentials.HADOOP_CREDSTORE_PASSWORD_ENVIRONMENT_VARIABLE) ? "env-password" : null);
        assertThat(resolution.password()).isEqualTo("env-password");
        assertThat(resolution.source()).isEqualTo("HADOOP_CREDSTORE_PASSWORD environment variable");

        Optional<KeyStoreCredentialAliasResolver> resolver = S3KeystoreCredentials.createAliasResolver(
                config,
                name -> name.equals(S3KeystoreCredentials.HADOOP_CREDSTORE_PASSWORD_ENVIRONMENT_VARIABLE) ? "env-password" : null);
        assertThat(resolver.orElseThrow().resolveAlias("s3.access.key")).isEqualTo("env-access");
    }

    @Test
    public void testConfigPasswordTakesPrecedenceOverEnvironment()
            throws Exception
    {
        Path keystorePath = KeyStoreTestFixture.createKeyStore(
                Map.of("s3.access.key", "config-access"),
                "config-password");

        S3FileSystemConfig config = new S3FileSystemConfig()
                .setKeystorePath(keystorePath.toString())
                .setKeystorePassword("config-password")
                .setAwsAccessKeyAlias("s3.access.key")
                .setAwsSecretKeyAlias("s3.access.key");

        S3KeystoreCredentials.PasswordResolution resolution = S3KeystoreCredentials.resolveKeystorePassword(
                config,
                _ -> "env-password");
        assertThat(resolution.password()).isEqualTo("config-password");
        assertThat(resolution.source()).isEqualTo("s3.keystore.password");
    }

    @Test
    public void testDefaultPasswordWhenUnset()
    {
        S3KeystoreCredentials.PasswordResolution resolution = S3KeystoreCredentials.resolveKeystorePassword(
                new S3FileSystemConfig().setKeystorePath("/tmp/unused.jceks"),
                _ -> null);
        assertThat(resolution.password()).isEqualTo("none");
        assertThat(resolution.source()).isEqualTo("default ('none')");
    }
}
