/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.parquet.crypto.keytools;

import io.trino.parquet.ParquetReaderOptions;
import org.apache.parquet.crypto.AesGcmDecryptor;
import org.apache.parquet.crypto.AesGcmEncryptor;
import org.apache.parquet.crypto.AesMode;
import org.apache.parquet.crypto.KeyAccessDeniedException;
import org.apache.parquet.crypto.ModuleCipherFactory;
import org.apache.parquet.crypto.ParquetCryptoRuntimeException;
import org.apache.parquet.crypto.TrinoCryptoConfigurationUtil;

import java.io.IOException;
import java.util.Base64;
import java.util.concurrent.ConcurrentMap;

public class TrinoKeyToolkit
{

    /**
     * Class implementing the KmsClient interface.
     * KMS stands for “key management service”.
     */
    public static final boolean DOUBLE_WRAPPING_DEFAULT = true;

    public static final long CACHE_LIFETIME_DEFAULT_SECONDS = 10 * 60; // 10 minutes
    // KMS client two level cache: token -> KMSInstanceId -> KmsClient
    static final TwoLevelCacheWithExpiration<TrinoKmsClient> KMS_CLIENT_CACHE_PER_TOKEN =
            KmsClientCache.INSTANCE.getCache();

    // KEK two level cache for wrapping: token -> MEK_ID -> KeyEncryptionKey
    static final TwoLevelCacheWithExpiration<KeyEncryptionKey> KEK_WRITE_CACHE_PER_TOKEN =
            KEKWriteCache.INSTANCE.getCache();

    // KEK two level cache for unwrapping: token -> KEK_ID -> KEK bytes
    static final TwoLevelCacheWithExpiration<byte[]> KEK_READ_CACHE_PER_TOKEN =
            KEKReadCache.INSTANCE.getCache();

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

    private enum KEKWriteCache
    {
        INSTANCE;
        private final TwoLevelCacheWithExpiration<KeyEncryptionKey> cache =
                new TwoLevelCacheWithExpiration<>();

        private TwoLevelCacheWithExpiration<KeyEncryptionKey> getCache()
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

    static String formatTokenForLog(String accessToken) {
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
     * Key rotation. In the single wrapping mode, decrypts data keys with old master keys, then encrypts
     * them with new master keys. In the double wrapping mode, decrypts KEKs (key encryption keys) with old
     * master keys, generates new KEKs and encrypts them with new master keys.
     * Works only if key material is not stored internally in file footers.
     * Not supported in local key wrapping mode.
     * Method can be run by multiple threads, but each thread must work on a different folder.
     * @param folderPath parent path of Parquet files, whose keys will be rotated
     * @param hadoopConfig Hadoop configuration
     * @throws IOException I/O problems
     * @throws ParquetCryptoRuntimeException General parquet encryption problems
     * @throws KeyAccessDeniedException No access to master keys
     * @throws UnsupportedOperationException Master key rotation not supported in the specific configuration
     */
//  public static void rotateMasterKeys(String folderPath, Configuration hadoopConfig)
//    throws IOException, ParquetCryptoRuntimeException, KeyAccessDeniedException, UnsupportedOperationException {
//
//    if (hadoopConfig.getBoolean(KEY_MATERIAL_INTERNAL_PROPERTY_NAME, false)) {
//      throw new UnsupportedOperationException("Key rotation is not supported for internal key material");
//    }
//
//    // If process wrote files with double-wrapped keys, clean KEK cache (since master keys are changing).
//    // Only once for each key rotation cycle; not for every folder
//    long currentTime = System.currentTimeMillis();
//    synchronized (lastCacheCleanForKeyRotationTimeLock) {
//      if (currentTime - lastCacheCleanForKeyRotationTime > CACHE_CLEAN_PERIOD_FOR_KEY_ROTATION) {
//        KEK_WRITE_CACHE_PER_TOKEN.clear();
//        lastCacheCleanForKeyRotationTime = currentTime;
//      }
//    }
//
//    Path parentPath = new Path(folderPath);
//
//    FileSystem hadoopFileSystem = parentPath.getFileSystem(hadoopConfig);
//    if (!hadoopFileSystem.exists(parentPath) || !hadoopFileSystem.isDirectory(parentPath)) {
//      throw new ParquetCryptoRuntimeException("Couldn't rotate keys - folder doesn't exist or is not a directory: " + folderPath);
//    }
//
//    FileStatus[] parquetFilesInFolder = hadoopFileSystem.listStatus(parentPath, HiddenFileFilter.INSTANCE);
//    if (parquetFilesInFolder.length == 0) {
//      throw new ParquetCryptoRuntimeException("Couldn't rotate keys - no parquet files in folder " + folderPath);
//    }
//
//    for (FileStatus fs : parquetFilesInFolder) {
//      Path parquetFile = fs.getPath();
//
//      FileKeyMaterialStore keyMaterialStore = new HadoopFSKeyMaterialStore(hadoopFileSystem);
//      keyMaterialStore.initialize(parquetFile, hadoopConfig, false);
//
//      FileKeyMaterialStore tempKeyMaterialStore = new HadoopFSKeyMaterialStore(hadoopFileSystem);
//      tempKeyMaterialStore.initialize(parquetFile, hadoopConfig, true);
//
//      Set<String> fileKeyIdSet = keyMaterialStore.getKeyIDSet();
//
//      // Start with footer key (to get KMS ID, URL, if needed)
//      FileKeyUnwrapper fileKeyUnwrapper = new FileKeyUnwrapper(hadoopConfig, parquetFile, keyMaterialStore);
//      String keyMaterialString = keyMaterialStore.getKeyMaterial(KeyMaterial.FOOTER_KEY_ID_IN_FILE);
//      KeyWithMasterID key = fileKeyUnwrapper.getDEKandMasterID(KeyMaterial.parse(keyMaterialString));
//      KmsClientAndDetails kmsClientAndDetails = fileKeyUnwrapper.getKmsClientAndDetails();
//
//      FileKeyWrapper fileKeyWrapper = new FileKeyWrapper(hadoopConfig, tempKeyMaterialStore, kmsClientAndDetails);
//      fileKeyWrapper.getEncryptionKeyMetadata(key.getDataKey(), key.getMasterID(), true,
//        KeyMaterial.FOOTER_KEY_ID_IN_FILE);
//
//      fileKeyIdSet.remove(KeyMaterial.FOOTER_KEY_ID_IN_FILE);
//      // Rotate column keys
//      for (String keyIdInFile : fileKeyIdSet) {
//        keyMaterialString = keyMaterialStore.getKeyMaterial(keyIdInFile);
//        key = fileKeyUnwrapper.getDEKandMasterID(KeyMaterial.parse(keyMaterialString));
//        fileKeyWrapper.getEncryptionKeyMetadata(key.getDataKey(), key.getMasterID(), false, keyIdInFile);
//      }
//
//      tempKeyMaterialStore.saveMaterial();
//
//      keyMaterialStore.removeMaterial();
//
//      tempKeyMaterialStore.moveMaterialTo(keyMaterialStore);
//    }
//  }
//

    /**
     * Flush any caches that are tied to the (compromised) accessToken
     *
     * @param accessToken access token
     */
    public static void removeCacheEntriesForToken(String accessToken)
    {
        KMS_CLIENT_CACHE_PER_TOKEN.removeCacheEntriesForToken(accessToken);
        KEK_WRITE_CACHE_PER_TOKEN.removeCacheEntriesForToken(accessToken);
        KEK_READ_CACHE_PER_TOKEN.removeCacheEntriesForToken(accessToken);
    }

    public static void removeCacheEntriesForAllTokens()
    {
        KMS_CLIENT_CACHE_PER_TOKEN.clear();
        KEK_WRITE_CACHE_PER_TOKEN.clear();
        KEK_READ_CACHE_PER_TOKEN.clear();
    }

    /**
     * Encrypts "key" with "masterKey", using AES-GCM and the "AAD"
     *
     * @param keyBytes the key to encrypt
     * @param masterKeyBytes encryption key
     * @param AAD additional authenticated data
     * @return base64 encoded encrypted key
     */
    public static String encryptKeyLocally(byte[] keyBytes, byte[] masterKeyBytes, byte[] AAD)
    {
        AesGcmEncryptor keyEncryptor;

        keyEncryptor = (AesGcmEncryptor) ModuleCipherFactory.getEncryptor(AesMode.GCM, masterKeyBytes);

        byte[] encryptedKey = keyEncryptor.encrypt(false, keyBytes, AAD);

        return Base64.getEncoder().encodeToString(encryptedKey);
    }

    /**
     * Decrypts encrypted key with "masterKey", using AES-GCM and the "AAD"
     *
     * @param encodedEncryptedKey base64 encoded encrypted key
     * @param masterKeyBytes encryption key
     * @param AAD additional authenticated data
     * @return decrypted key
     */
    public static byte[] decryptKeyLocally(String encodedEncryptedKey, byte[] masterKeyBytes, byte[] AAD)
    {
        byte[] encryptedKey = Base64.getDecoder().decode(encodedEncryptedKey);

        AesGcmDecryptor keyDecryptor;

        keyDecryptor = (AesGcmDecryptor) ModuleCipherFactory.getDecryptor(AesMode.GCM, masterKeyBytes);

        return keyDecryptor.decrypt(encryptedKey, 0, encryptedKey.length, AAD);
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
        TrinoKmsClient kmsClient = null;

        try {
            kmsClientClass = TrinoCryptoConfigurationUtil.getClassFromConfig(trinoParquetCryptoConfig.getEncryptionKmsClientClass(),
                    TrinoKmsClient.class);

            if (null == kmsClientClass) {
                throw new ParquetCryptoRuntimeException("Could not find class " + trinoParquetCryptoConfig.getEncryptionKmsClientClass());
            }
            kmsClient = (TrinoKmsClient) kmsClientClass.newInstance();
        }
        catch (InstantiationException | IllegalAccessException e) {
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
