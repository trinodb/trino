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

import io.airlift.security.pem.PemReader;

import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.security.KeyStore;

public final class TruststoreUtil
{
    private TruststoreUtil() {}

    public static TrustManager[] createTruststore(String truststorePath, String truststorePassword)
    {
        try {
            TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustManagerFactory.init(loadKeyStore(truststorePath, truststorePassword));
            return trustManagerFactory.getTrustManagers();
        }
        catch (IOException | GeneralSecurityException e) {
            throw new RuntimeException("Error loading trust store: " + truststorePath, e);
        }
    }

    private static KeyStore loadKeyStore(String truststorePath, String truststorePassword)
            throws GeneralSecurityException, IOException
    {
        File keyStoreFile = new File(truststorePath);
        if (PemReader.isPem(keyStoreFile)) {
            return PemReader.loadTrustStore(keyStoreFile);
        }

        KeyStore keyStore = KeyStore.getInstance("JKS");
        try (InputStream truststoreStream = new FileInputStream(truststorePath)) {
            keyStore.load(truststoreStream, truststorePassword == null ? null : truststorePassword.toCharArray());
        }
        return keyStore;
    }
}
