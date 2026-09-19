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
package io.trino.filesystem.s3.keystore;

import io.trino.spi.TrinoException;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.Map;

import static io.trino.spi.StandardErrorCode.CONFIGURATION_INVALID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestKeyStoreCredentialAliasResolver
{
    @Test
    public void testResolveS3Aliases()
            throws Exception
    {
        Path keystorePath = KeyStoreTestFixture.createKeyStore(
                Map.of(
                        "fs.s3a.bucket.testbucket.access.key", "AKIATEST",
                        "fs.s3a.bucket.testbucket.secret.key", "SECRETTEST"),
                "none");

        KeyStoreCredentialAliasResolver resolver = new KeyStoreCredentialAliasResolver(
                keystorePath.toString(),
                "JCEKS",
                "none",
                "none");

        assertThat(resolver.resolveAlias("fs.s3a.bucket.testbucket.access.key")).isEqualTo("AKIATEST");
        assertThat(resolver.resolveAlias("FS.S3A.BUCKET.TESTBUCKET.SECRET.KEY")).isEqualTo("SECRETTEST");
    }

    @Test
    public void testResolveBucketCredentials()
            throws Exception
    {
        Path keystorePath = KeyStoreTestFixture.createKeyStore(
                Map.of(
                        "fs.s3a.bucket.my-bucket.access.key", "access-key",
                        "fs.s3a.bucket.my-bucket.secret.key", "secret-key"),
                "none");

        KeyStoreCredentialAliasResolver resolver = new KeyStoreCredentialAliasResolver(
                keystorePath.toString(),
                "JCEKS",
                "none",
                "none");

        KeyStoreCredentialAliasResolver.BucketCredentials credentials = resolver.resolveBucketCredentials("fs.s3a.bucket.", "my-bucket");
        assertThat(credentials.accessKey()).isEqualTo("access-key");
        assertThat(credentials.secretKey()).isEqualTo("secret-key");
    }

    @Test
    public void testUnknownAlias()
            throws Exception
    {
        Path keystorePath = KeyStoreTestFixture.createKeyStore(Map.of("known.alias", "value"), "none");

        KeyStoreCredentialAliasResolver resolver = new KeyStoreCredentialAliasResolver(
                keystorePath.toString(),
                "JCEKS",
                "none",
                "none");

        assertThatThrownBy(() -> resolver.resolveAlias("missing.alias"))
                .isInstanceOfSatisfying(TrinoException.class, e -> {
                    assertThat(e.getErrorCode()).isEqualTo(CONFIGURATION_INVALID.toErrorCode());
                    assertThat(e).hasMessageContaining("Unknown credential alias");
                });
    }

    @Test
    public void testReadHadoopCreatedKeystore()
            throws Exception
    {
        Path keystorePath = KeyStoreTestFixture.createHadoopKeyStore(
                Map.of(
                        "fs.s3a.bucket.example-bucket.access.key", "example-access",
                        "fs.s3a.bucket.example-bucket.secret.key", "example-secret"),
                "none");

        KeyStoreCredentialAliasResolver resolver = new KeyStoreCredentialAliasResolver(
                keystorePath.toString(),
                "JCEKS",
                "none",
                "none");

        assertThat(resolver.resolveAlias("fs.s3a.bucket.example-bucket.access.key")).isEqualTo("example-access");
        assertThat(resolver.resolveAlias("fs.s3a.bucket.example-bucket.secret.key")).isEqualTo("example-secret");
    }
}
