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

import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.security.Key;
import java.security.KeyStore;
import java.security.KeyStore.PasswordProtection;
import java.security.KeyStore.SecretKeyEntry;

import static io.trino.spi.StandardErrorCode.CONFIGURATION_INVALID;
import static java.nio.charset.StandardCharsets.UTF_8;

final class KeyStoreUtils
{
    private KeyStoreUtils() {}

    static KeyStore loadKeyStore(String keyStoreType, String keyStorePath, String keyStorePassword)
            throws IOException, GeneralSecurityException
    {
        KeyStore keyStore = KeyStore.getInstance(keyStoreType);
        try (InputStream stream = new FileInputStream(keyStorePath)) {
            keyStore.load(stream, keyStorePassword.toCharArray());
        }
        return keyStore;
    }

    static String readEntity(KeyStore keyStore, String entityAlias, String keystorePassword, String entryPassword)
            throws GeneralSecurityException
    {
        try {
            // Hadoop CredentialProvider format: UTF-8 credential bytes in a SecretKeySpec (setKeyEntry)
            if (keyStore.isKeyEntry(entityAlias)) {
                Key key = keyStore.getKey(entityAlias, keystorePassword.toCharArray());
                if (key instanceof SecretKeySpec secretKeySpec) {
                    return new String(secretKeySpec.getEncoded(), UTF_8);
                }
            }

            // JDBC KEYSTORE provider format: PBE-protected SecretKeyEntry (getEntry)
            var entry = keyStore.getEntry(entityAlias, new PasswordProtection(entryPassword.toCharArray()));
            if (!(entry instanceof SecretKeyEntry secretKeyEntry)) {
                throw new TrinoException(CONFIGURATION_INVALID, "Unsupported keystore entry format for alias: " + entityAlias);
            }
            SecretKeyFactory factory = SecretKeyFactory.getInstance(secretKeyEntry.getSecretKey().getAlgorithm());
            PBEKeySpec keySpec = (PBEKeySpec) factory.getKeySpec(secretKeyEntry.getSecretKey(), PBEKeySpec.class);
            return new String(keySpec.getPassword());
        }
        catch (ClassCastException e) {
            throw new TrinoException(CONFIGURATION_INVALID, "Unsupported keystore entry format for alias: " + entityAlias, e);
        }
    }
}
