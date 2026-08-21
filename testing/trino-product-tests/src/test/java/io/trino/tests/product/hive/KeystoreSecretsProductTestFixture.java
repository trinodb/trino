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
package io.trino.tests.product.hive;

import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.Locale;
import java.util.Map;

/**
 * Creates JCEKS fixtures for keystore-backed secrets product tests.
 * Mirrors {@code io.trino.filesystem.s3.secrets.KeyStoreTestFixture} without a test-module dependency.
 */
public final class KeystoreSecretsProductTestFixture
{
    public static final String KEYSTORE_PASSWORD = "none";

    private KeystoreSecretsProductTestFixture() {}

    public static Path createKeyStore(Map<String, String> aliases)
            throws Exception
    {
        KeyStore keyStore = KeyStore.getInstance("JCEKS");
        keyStore.load(null, KEYSTORE_PASSWORD.toCharArray());

        SecretKeyFactory factory = SecretKeyFactory.getInstance("PBE");
        for (Map.Entry<String, String> entry : aliases.entrySet()) {
            String alias = entry.getKey().toLowerCase(Locale.US);
            PBEKeySpec keySpec = new PBEKeySpec(entry.getValue().toCharArray());
            SecretKey key = factory.generateSecret(keySpec);
            keyStore.setEntry(alias, new KeyStore.SecretKeyEntry(key), new KeyStore.PasswordProtection(KEYSTORE_PASSWORD.toCharArray()));
        }

        Path keystorePath = Files.createTempFile("keystore-secrets-pt-", ".jceks");
        keystorePath.toFile().deleteOnExit();
        try (OutputStream out = Files.newOutputStream(keystorePath)) {
            keyStore.store(out, KEYSTORE_PASSWORD.toCharArray());
        }
        return keystorePath;
    }

    public static void deleteIfExists(Path keystorePath)
    {
        if (keystorePath == null) {
            return;
        }
        try {
            Files.deleteIfExists(keystorePath);
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to delete temporary keystore: " + keystorePath, e);
        }
    }
}
