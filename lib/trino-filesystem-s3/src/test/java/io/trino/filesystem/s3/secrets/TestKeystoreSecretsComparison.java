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
package io.trino.filesystem.s3.secrets;

import io.airlift.configuration.secrets.SecretsResolver;
import io.airlift.secrets.keystore.KeystoreSecretProvider;
import io.airlift.secrets.keystore.KeystoreSecretProviderConfig;
import io.airlift.spi.secrets.SecretProvider;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Validates approach 3: enhanced {@link KeystoreSecretProvider} reads both PBE and Hadoop JCEKS formats.
 */
public class TestKeystoreSecretsComparison
{
    private static final String ACCESS_ALIAS = "fs.s3a.bucket.test-bucket.access.key";
    private static final String SECRET_ALIAS = "fs.s3a.bucket.test-bucket.secret.key";
    private static final String ACCESS_VALUE = "AKIATEST";
    private static final String SECRET_VALUE = "SECRETTEST";
    private static final String PASSWORD = "none";

    @Test
    public void testAirliftKeystoreProviderReadsPbeJceks()
            throws Exception
    {
        Path keystore = KeyStoreTestFixture.createKeyStore(
                Map.of(ACCESS_ALIAS, ACCESS_VALUE, SECRET_ALIAS, SECRET_VALUE),
                PASSWORD);

        KeystoreSecretProvider provider = new KeystoreSecretProvider(keystoreConfig(keystore));
        assertThat(provider.resolveSecretValue(ACCESS_ALIAS)).isEqualTo(ACCESS_VALUE);
        assertThat(provider.resolveSecretValue(SECRET_ALIAS)).isEqualTo(SECRET_VALUE);
    }

    @Test
    public void testSecretsResolverInterpolatesPbeJceksIntoCatalogProperties()
            throws Exception
    {
        Path keystore = KeyStoreTestFixture.createKeyStore(
                Map.of(ACCESS_ALIAS, ACCESS_VALUE, SECRET_ALIAS, SECRET_VALUE),
                PASSWORD);

        SecretProvider keystoreProvider = new KeystoreSecretProvider(keystoreConfig(keystore));
        SecretsResolver resolver = new SecretsResolver(Map.of("keystore", keystoreProvider));

        Map<String, String> resolved = resolver.getResolvedConfiguration(Map.of(
                "s3.aws-access-key", "${keystore:" + ACCESS_ALIAS + "}",
                "s3.aws-secret-key", "${keystore:" + SECRET_ALIAS + "}"));

        assertThat(resolved.get("s3.aws-access-key")).isEqualTo(ACCESS_VALUE);
        assertThat(resolved.get("s3.aws-secret-key")).isEqualTo(SECRET_VALUE);
    }

    @Test
    public void testAirliftKeystoreProviderReadsHadoopJceks()
            throws Exception
    {
        Path keystore = KeyStoreTestFixture.createHadoopKeyStore(
                Map.of(ACCESS_ALIAS, ACCESS_VALUE, SECRET_ALIAS, SECRET_VALUE),
                PASSWORD);

        KeystoreSecretProvider provider = new KeystoreSecretProvider(keystoreConfig(keystore));

        assertThat(provider.resolveSecretValue(ACCESS_ALIAS)).isEqualTo(ACCESS_VALUE);
        assertThat(provider.resolveSecretValue(SECRET_ALIAS)).isEqualTo(SECRET_VALUE);
    }

    private static KeystoreSecretProviderConfig keystoreConfig(Path keystore)
    {
        return new KeystoreSecretProviderConfig()
                .setKeyStoreFilePath(keystore.toString())
                .setKeyStoreType("JCEKS")
                .setKeyStorePassword(PASSWORD);
    }
}
