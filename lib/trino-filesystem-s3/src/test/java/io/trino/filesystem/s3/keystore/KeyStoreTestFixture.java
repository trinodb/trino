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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.alias.CredentialProvider;
import org.apache.hadoop.security.alias.CredentialProviderFactory;

import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public final class KeyStoreTestFixture
{
    private KeyStoreTestFixture() {}

    /**
     * Create a JCEKS file using the same PBE SecretKeyEntry pattern as Hadoop LocalJavaKeyStoreProvider.
     */
    public static Path createKeyStore(Map<String, String> aliases, String password)
            throws Exception
    {
        KeyStore keyStore = KeyStore.getInstance("JCEKS");
        keyStore.load(null, password.toCharArray());

        SecretKeyFactory factory = SecretKeyFactory.getInstance("PBE");
        for (Map.Entry<String, String> entry : aliases.entrySet()) {
            String alias = entry.getKey().toLowerCase(Locale.US);
            PBEKeySpec keySpec = new PBEKeySpec(entry.getValue().toCharArray());
            SecretKey key = factory.generateSecret(keySpec);
            keyStore.setEntry(alias, new KeyStore.SecretKeyEntry(key), new KeyStore.PasswordProtection(password.toCharArray()));
        }

        return writeKeyStore(keyStore, password);
    }

    /**
     * Create a JCEKS file via Hadoop CredentialProviderFactory (SecretKeySpec entry format).
     */
    public static Path createHadoopKeyStore(Map<String, String> aliases, String password)
            throws IOException
    {
        Path keystorePath = Files.createTempFile("hadoop-credentials", ".jceks");
        keystorePath.toFile().deleteOnExit();

        String providerPath = "localjceks://file" + keystorePath.toAbsolutePath();
        Configuration configuration = new Configuration(false);
        configuration.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, providerPath);
        configuration.set("hadoop.security.credential.provider.password", password);

        List<CredentialProvider> providers = CredentialProviderFactory.getProviders(configuration);
        CredentialProvider provider = providers.getFirst();
        for (Map.Entry<String, String> entry : aliases.entrySet()) {
            provider.createCredentialEntry(entry.getKey(), entry.getValue().toCharArray());
        }
        provider.flush();

        return keystorePath;
    }

    private static Path writeKeyStore(KeyStore keyStore, String password)
            throws Exception
    {
        Path keystorePath = Files.createTempFile("credentials", ".jceks");
        keystorePath.toFile().deleteOnExit();
        try (OutputStream out = Files.newOutputStream(keystorePath)) {
            keyStore.store(out, password.toCharArray());
        }
        return keystorePath;
    }
}
