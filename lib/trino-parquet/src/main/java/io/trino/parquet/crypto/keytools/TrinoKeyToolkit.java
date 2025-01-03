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
package io.trino.parquet.crypto.keytools;

import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.crypto.AesGcmDecryptor;
import io.trino.parquet.crypto.AesMode;
import io.trino.parquet.crypto.ModuleCipherFactory;
import io.trino.parquet.crypto.ParquetCryptoRuntimeException;
import io.trino.parquet.crypto.TrinoCryptoConfigurationUtil;

import java.lang.reflect.InvocationTargetException;
import java.util.Base64;
import java.util.concurrent.ConcurrentMap;

public class TrinoKeyToolkit
{
    public static final long CACHE_LIFETIME_DEFAULT_SECONDS = 10 * 60; // 10 minutes

    // KMS client two level cache: token -> KMSInstanceId -> KmsClient
    static final TwoLevelCacheWithExpiration<TrinoKmsClient> KMS_CLIENT_CACHE_PER_TOKEN =
            KmsClientCache.INSTANCE.getCache();

    // KEK two level cache for unwrapping: token -> KEK_ID -> KEK bytes
    static final TwoLevelCacheWithExpiration<byte[]> KEK_READ_CACHE_PER_TOKEN =
            KEKReadCache.INSTANCE.getCache();

    private TrinoKeyToolkit()
    {
    }

    private enum KmsClientCache
    {
        INSTANCE;
        private final TwoLevelCacheWithExpiration<TrinoKmsClient> cache =
                new TwoLevelCacheWithExpiration<>();

        private TwoLevelCacheWithExpiration<TrinoKmsClient> getCache()
        {
            return cache;
        }
    }

    private enum KEKReadCache
    {
        INSTANCE;
        private final TwoLevelCacheWithExpiration<byte[]> cache =
                new TwoLevelCacheWithExpiration<>();

        private TwoLevelCacheWithExpiration<byte[]> getCache()
        {
            return cache;
        }
    }

    static String formatTokenForLog(String accessToken)
    {
        int maxTokenDisplayLength = 5;
        if (accessToken.length() <= maxTokenDisplayLength) {
            return accessToken;
        }
        return accessToken.substring(accessToken.length() - maxTokenDisplayLength);
    }

    static class KeyWithMasterID
    {
        private final byte[] keyBytes;
        private final String masterID;

        KeyWithMasterID(byte[] keyBytes, String masterID)
        {
            this.keyBytes = keyBytes;
            this.masterID = masterID;
        }

        byte[] getDataKey()
        {
            return keyBytes;
        }

        String getMasterID()
        {
            return masterID;
        }
    }

    static class KeyEncryptionKey
    {
        private final byte[] kekBytes;
        private final byte[] kekID;
        private String encodedKekID;
        private final String encodedWrappedKEK;

        KeyEncryptionKey(byte[] kekBytes, byte[] kekID, String encodedWrappedKEK)
        {
            this.kekBytes = kekBytes;
            this.kekID = kekID;
            this.encodedWrappedKEK = encodedWrappedKEK;
        }

        byte[] getBytes()
        {
            return kekBytes;
        }

        byte[] getID()
        {
            return kekID;
        }

        String getEncodedID()
        {
            if (null == encodedKekID) {
                encodedKekID = Base64.getEncoder().encodeToString(kekID);
            }
            return encodedKekID;
        }

        String getEncodedWrappedKEK()
        {
            return encodedWrappedKEK;
        }
    }

    /**
     * Decrypts encrypted key with "masterKey", using AES-GCM and the "aad"
     *
     * @param encodedEncryptedKey base64 encoded encrypted key
     * @param masterKeyBytes encryption key
     * @param aad additional authenticated data
     * @return decrypted key
     */
    public static byte[] decryptKeyLocally(String encodedEncryptedKey, byte[] masterKeyBytes, byte[] aad)
    {
        byte[] encryptedKey = Base64.getDecoder().decode(encodedEncryptedKey);

        AesGcmDecryptor keyDecryptor;

        keyDecryptor = (AesGcmDecryptor) ModuleCipherFactory.getDecryptor(AesMode.GCM, masterKeyBytes);

        return keyDecryptor.decrypt(encryptedKey, 0, encryptedKey.length, aad);
    }

    static TrinoKmsClient getKmsClient(String kmsInstanceID, String kmsInstanceURL, ParquetReaderOptions trinoParquetCryptoConfig,
                                       String accessToken, long cacheEntryLifetime)
    {
        ConcurrentMap<String, TrinoKmsClient> kmsClientPerKmsInstanceCache =
                KMS_CLIENT_CACHE_PER_TOKEN.getOrCreateInternalCache(accessToken, cacheEntryLifetime);

        TrinoKmsClient kmsClient =
                kmsClientPerKmsInstanceCache.computeIfAbsent(kmsInstanceID,
                        (k) -> createAndInitKmsClient(trinoParquetCryptoConfig, kmsInstanceID, kmsInstanceURL, accessToken));

        return kmsClient;
    }

    private static TrinoKmsClient createAndInitKmsClient(ParquetReaderOptions trinoParquetCryptoConfig, String kmsInstanceID,
                                                         String kmsInstanceURL, String accessToken)
    {
        Class<?> kmsClientClass = null;
        TrinoKmsClient kmsClient;

        try {
            kmsClientClass = TrinoCryptoConfigurationUtil.getClassFromConfig(trinoParquetCryptoConfig.getEncryptionKmsClientClass(),
                    TrinoKmsClient.class);

            if (null == kmsClientClass) {
                throw new ParquetCryptoRuntimeException("Could not find class " + trinoParquetCryptoConfig.getEncryptionKmsClientClass());
            }
            kmsClient = (TrinoKmsClient) kmsClientClass.getConstructor().newInstance();
        }
        catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
            throw new ParquetCryptoRuntimeException("Could not instantiate KmsClient class: "
                    + kmsClientClass, e);
        }

        kmsClient.initialize(trinoParquetCryptoConfig, kmsInstanceID, kmsInstanceURL, accessToken);

        return kmsClient;
    }

    static class TrinoKmsClientAndDetails
    {
        public TrinoKmsClient getKmsClient()
        {
            return kmsClient;
        }

        private TrinoKmsClient kmsClient;
        private String kmsInstanceID;
        private String kmsInstanceURL;

        public TrinoKmsClientAndDetails(TrinoKmsClient kmsClient, String kmsInstanceID, String kmsInstanceURL)
        {
            this.kmsClient = kmsClient;
            this.kmsInstanceID = kmsInstanceID;
            this.kmsInstanceURL = kmsInstanceURL;
        }

        public String getKmsInstanceID()
        {
            return kmsInstanceID;
        }

        public String getKmsInstanceURL()
        {
            return kmsInstanceURL;
        }
    }
}
