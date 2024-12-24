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
package io.trino.parquet.crypto;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.json.ObjectMapperProvider;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.crypto.keytools.TrinoKmsClient;

import java.io.IOException;
import java.io.StringReader;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public abstract class TrinoVersionedLocalWrap
        implements TrinoKmsClient
{
    protected String kmsInstanceID;
    protected String kmsInstanceURL;
    protected String kmsToken;
    protected ParquetReaderOptions trinoParquetCryptoConfig;
    protected ConcurrentMap<String, byte[]> masterKeyCache;

    public TrinoVersionedLocalWrap()
    {
    }

    @Override
    public void initialize(ParquetReaderOptions trinoParquetCryptoConfig, String kmsInstanceID, String kmsInstanceURL, String accessToken)
            throws KeyAccessDeniedException
    {
        this.kmsInstanceID = kmsInstanceID;
        this.kmsInstanceURL = kmsInstanceURL;
        this.masterKeyCache = new ConcurrentHashMap();
        this.trinoParquetCryptoConfig = trinoParquetCryptoConfig;
        this.kmsToken = accessToken;
        this.initializeInternal();
    }

    @Override
    public String wrapKey(byte[] keyBytes, String masterKeyIdentifier)
            throws KeyAccessDeniedException
    {
        return null;
    }

    @Override
    public byte[] unwrapKey(String wrappedKey, String masterKeyIdentifier)
            throws KeyAccessDeniedException
    {
        LocalKeyWrap keyWrap = LocalKeyWrap.parse(wrappedKey);
        String masterKeyVersionedID = masterKeyIdentifier + ":" + keyWrap.getMasterKeyVersion();
        String encryptedEncodedKey = keyWrap.getEncryptedKey();
        byte[] masterKey = this.masterKeyCache.computeIfAbsent(masterKeyVersionedID, (k) -> this.getMasterKeyForVersion(masterKeyIdentifier, keyWrap.getMasterKeyVersion()));
        return decryptKeyLocally(encryptedEncodedKey, masterKey, null);
    }

    public static byte[] decryptKeyLocally(String encodedEncryptedKey, byte[] masterKeyBytes, byte[] aad)
    {
        byte[] encryptedKey = Base64.getDecoder().decode(encodedEncryptedKey);
        AesGcmDecryptor keyDecryptor = (AesGcmDecryptor) ModuleCipherFactory.getDecryptor(AesMode.GCM, masterKeyBytes);
        return keyDecryptor.decrypt(encryptedKey, 0, encryptedKey.length, aad);
    }

    private byte[] getMasterKeyForVersion(String keyIdentifier, String keyVersion)
    {
        this.kmsToken = trinoParquetCryptoConfig.getEncryptionKeyAccessToken();
        byte[] key = this.getMasterKey(keyIdentifier, keyVersion);
        this.checkMasterKeyLength(key.length, keyIdentifier, keyVersion);
        return key;
    }

    private void checkMasterKeyLength(int keyLength, String keyID, String keyVersion)
    {
        if (16 != keyLength && 24 != keyLength && 32 != keyLength) {
            throw new ParquetCryptoRuntimeException("Wrong length: " + keyLength + " of master key: " + keyID + ", version: " + keyVersion);
        }
    }

    protected abstract MasterKeyWithVersion getMasterKey(String var1)
            throws KeyAccessDeniedException;

    protected abstract byte[] getMasterKey(String var1, String var2)
            throws KeyAccessDeniedException;

    protected abstract void initializeInternal()
            throws KeyAccessDeniedException;

    static class LocalKeyWrap
    {
        private static final ObjectMapper OBJECT_MAPPER = new ObjectMapperProvider().get()
                .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS);
        private final String encryptedEncodedKey;
        private final String masterKeyVersion;

        private LocalKeyWrap(String masterKeyVersion, String encryptedEncodedKey)
        {
            this.masterKeyVersion = masterKeyVersion;
            this.encryptedEncodedKey = encryptedEncodedKey;
        }

        private static String createSerialized(String encryptedEncodedKey, String masterKeyVersion)
        {
            Map<String, String> keyWrapMap = new HashMap(3);
            keyWrapMap.put("localWrappingType", "LKW1");
            keyWrapMap.put("masterKeyVersion", masterKeyVersion);
            keyWrapMap.put("encryptedKey", encryptedEncodedKey);

            try {
                return OBJECT_MAPPER.writeValueAsString(keyWrapMap);
            }
            catch (IOException var4) {
                throw new ParquetCryptoRuntimeException("Failed to serialize local key wrap map", var4);
            }
        }

        static LocalKeyWrap parse(String wrappedKey)
        {
            Map keyWrapMap;
            try {
                keyWrapMap = (Map) OBJECT_MAPPER.readValue(new StringReader(wrappedKey), new TypeReference<Map<String, String>>() {
                });
            }
            catch (IOException var5) {
                throw new ParquetCryptoRuntimeException("Failed to parse local key wrap json " + wrappedKey, var5);
            }

            String localWrappingType = (String) keyWrapMap.get("localWrappingType");
            String masterKeyVersion = (String) keyWrapMap.get("masterKeyVersion");
            if (null == localWrappingType) {
                if (!"NO_VERSION".equals(masterKeyVersion)) {
                    throw new ParquetCryptoRuntimeException("No localWrappingType defined for key version: " + masterKeyVersion);
                }
            }
            else if (!"LKW1".equals(localWrappingType)) {
                throw new ParquetCryptoRuntimeException("Unsupported localWrappingType: " + localWrappingType);
            }

            String encryptedEncodedKey = (String) keyWrapMap.get("encryptedKey");
            return new LocalKeyWrap(masterKeyVersion, encryptedEncodedKey);
        }

        String getMasterKeyVersion()
        {
            return this.masterKeyVersion;
        }

        private String getEncryptedKey()
        {
            return this.encryptedEncodedKey;
        }
    }

    public static class MasterKeyWithVersion
    {
        private final byte[] masterKey;
        private final String masterKeyVersion;

        public MasterKeyWithVersion(byte[] masterKey, String masterKeyVersion)
        {
            this.masterKey = masterKey;
            this.masterKeyVersion = masterKeyVersion;
        }

        private byte[] getKey()
        {
            return this.masterKey;
        }

        private String getVersion()
        {
            return this.masterKeyVersion;
        }
    }
}
