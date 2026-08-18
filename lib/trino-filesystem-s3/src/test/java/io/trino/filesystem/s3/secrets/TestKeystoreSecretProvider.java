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
 * Unit tests for {@link KeystoreSecretProvider} and {@link SecretsResolver} with
 * global {@code fs.s3a.access.key} / {@code fs.s3a.secret.key} aliases.
 * <p>
 * JCEKS format matrix:
 * <table>
 *   <caption>Keystore formats exercised by PR1</caption>
 *   <tr><th>Format</th><th>Entry type</th><th>Fixture</th><th>Where tested</th><th>Airlift version</th></tr>
 *   <tr><td>PBE</td><td>{@code PBEKeySpec} + {@code SecretKeyEntry}</td>
 *       <td>{@link KeyStoreTestFixture#createKeyStore}</td>
 *       <td>Unit + product tests</td><td>444 (bundled)</td></tr>
 *   <tr><td>Hadoop</td><td>{@code SecretKeySpec} via {@code setKeyEntry}</td>
 *       <td>{@link KeyStoreTestFixture#createHadoopKeyStore}</td>
 *       <td>Unit only ({@link #testAirliftKeystoreProviderReadsHadoopJceks})</td>
 *       <td>445-SNAPSHOT (test scope)</td></tr>
 * </table>
 * Product tests use PBE because Trino bundles Airlift 444, whose stock keystore plugin
 * reads PBE entries only. Production Hadoop-format keystores from
 * {@code CredentialProviderFactory} require enhanced reader support in
 * <a href="https://github.com/airlift/airlift/pull/2100">airlift/airlift#2100</a>
 * plus a subsequent Trino Airlift version bump — not a merge blocker for this test-only PR.
 * No negative test asserts that stock Airlift 444 fails on Hadoop JCEKS because
 * 445-SNAPSHOT on the test classpath would make such an assertion fragile and not
 * meaningful in CI.
 */
public class TestKeystoreSecretProvider
{
    // Global aliases: one static credential pair resolved at catalog load.
    private static final String ACCESS_ALIAS = "fs.s3a.access.key";
    private static final String SECRET_ALIAS = "fs.s3a.secret.key";
    private static final String ACCESS_VALUE = "AKIATEST";
    private static final String SECRET_VALUE = "SECRETTEST";
    private static final String PASSWORD = "none";

    /**
     * Airlift {@link KeystoreSecretProvider} reads PBE {@code SecretKeyEntry} aliases
     * from a JCEKS file — the format used by {@code KeystoreSecretsProductTestFixture}
     * and compatible with Trino's bundled Airlift 444.
     */
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

    /**
     * {@link SecretsResolver} interpolates {@code ${keystore:alias}} placeholders into
     * catalog properties using a PBE JCEKS keystore — the end-to-end global credential path.
     */
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

    /**
     * Hadoop-format JCEKS: {@link KeyStoreTestFixture#createHadoopKeyStore} writes entries
     * via {@code CredentialProviderFactory} / {@code setKeyEntry(SecretKeySpec)} — the
     * same layout as Hadoop-format JCEKS from {@code hadoop credential create}.
     * <p>
     * Requires {@code secrets-keystore-plugin} 445-SNAPSHOT on the test classpath (see
     * {@code lib/trino-filesystem-s3/pom.xml}); enhanced reader from
     * <a href="https://github.com/airlift/airlift/pull/2100">airlift/airlift#2100</a>.
     * Stock Airlift 444 cannot read this format — product tests intentionally use PBE instead.
     */
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
