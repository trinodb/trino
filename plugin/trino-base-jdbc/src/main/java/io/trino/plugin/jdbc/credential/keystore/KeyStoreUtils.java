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
package io.trino.plugin.jdbc.credential.keystore;

import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.KeyStore.PasswordProtection;
import java.security.KeyStore.SecretKeyEntry;

public final class KeyStoreUtils
{
    private KeyStoreUtils() {}

    public static KeyStore loadKeyStore(String keyStoreType, String keyStorePath, String keyStorePassword)
            throws IOException, GeneralSecurityException
    {
        KeyStore keyStore = KeyStore.getInstance(keyStoreType);
        try (InputStream stream = new FileInputStream(keyStorePath)) {
            keyStore.load(stream, keyStorePassword.toCharArray());
        }
        return keyStore;
    }

    public static String readEntity(KeyStore keyStore, String entityAlias, String entityPassword)
            throws GeneralSecurityException
    {
        SecretKeyEntry secretKeyEntry = (SecretKeyEntry) keyStore.getEntry(entityAlias, new PasswordProtection(entityPassword.toCharArray()));

        SecretKeyFactory factory = SecretKeyFactory.getInstance("PBE");
        PBEKeySpec keySpec = (PBEKeySpec) factory.getKeySpec(secretKeyEntry.getSecretKey(), PBEKeySpec.class);

        return new String(keySpec.getPassword());
    }
}
