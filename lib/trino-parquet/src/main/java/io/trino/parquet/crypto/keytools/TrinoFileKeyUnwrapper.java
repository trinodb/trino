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

import com.google.common.base.Strings;
import io.airlift.log.Logger;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.crypto.DecryptionKeyRetriever;
import io.trino.parquet.crypto.ParquetCryptoRuntimeException;
import io.trino.parquet.crypto.keytools.TrinoKeyToolkit.KeyWithMasterID;

import java.util.Base64;
import java.util.concurrent.ConcurrentMap;

import static io.trino.parquet.crypto.keytools.TrinoKeyToolkit.KEK_READ_CACHE_PER_TOKEN;
import static io.trino.parquet.crypto.keytools.TrinoKeyToolkit.KMS_CLIENT_CACHE_PER_TOKEN;

public class TrinoFileKeyUnwrapper
        implements DecryptionKeyRetriever
{
    private static final Logger LOG = Logger.get(TrinoFileKeyUnwrapper.class);

    //A map of KEK_ID -> KEK bytes, for the current token
    private final ConcurrentMap<String, byte[]> kekPerKekID;
    private final Location parquetFilePath;
    // TODO(wyu): shall we get it from Location or File
    private final TrinoFileSystem trinoFileSystem;
    private final String accessToken;
    private final long cacheEntryLifetime;
    private final ParquetReaderOptions parquetReaderOptions;
    private TrinoKeyToolkit.TrinoKmsClientAndDetails kmsClientAndDetails;
    private TrinoHadoopFSKeyMaterialStore keyMaterialStore;
    private boolean checkedKeyMaterialInternalStorage;

    TrinoFileKeyUnwrapper(ParquetReaderOptions conf, Location filePath, TrinoFileSystem trinoFileSystem)
    {
        this.trinoFileSystem = trinoFileSystem;
        this.parquetReaderOptions = conf;
        this.parquetFilePath = filePath;
        this.cacheEntryLifetime = 1000L * conf.getEncryptionCacheLifetimeSeconds();
        this.accessToken = conf.getEncryptionKeyAccessToken();
        this.kmsClientAndDetails = null;
        this.keyMaterialStore = null;
        this.checkedKeyMaterialInternalStorage = false;

        // Check cache upon each file reading (clean once in cacheEntryLifetime)
        KMS_CLIENT_CACHE_PER_TOKEN.checkCacheForExpiredTokens(cacheEntryLifetime);
        KEK_READ_CACHE_PER_TOKEN.checkCacheForExpiredTokens(cacheEntryLifetime);
        kekPerKekID = KEK_READ_CACHE_PER_TOKEN.getOrCreateInternalCache(accessToken, cacheEntryLifetime);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Creating file key unwrapper. KeyMaterialStore: {}; token snippet: {}",
                    keyMaterialStore, TrinoKeyToolkit.formatTokenForLog(accessToken));
        }
    }

    @Override
    public byte[] getKey(byte[] keyMetadataBytes)
    {
        KeyMetadata keyMetadata = KeyMetadata.parse(keyMetadataBytes);

        if (!checkedKeyMaterialInternalStorage) {
            if (!keyMetadata.keyMaterialStoredInternally()) {
                keyMaterialStore = new TrinoHadoopFSKeyMaterialStore(trinoFileSystem, parquetFilePath, false);
            }
            checkedKeyMaterialInternalStorage = true;
        }

        KeyMaterial keyMaterial;
        if (keyMetadata.keyMaterialStoredInternally()) {
            // Internal key material storage: key material is inside key metadata
            keyMaterial = keyMetadata.getKeyMaterial();
        }
        else {
            // External key material storage: key metadata contains a reference to a key in the material store
            String keyIDinFile = keyMetadata.getKeyReference();
            String keyMaterialString = keyMaterialStore.getKeyMaterial(keyIDinFile);
            if (null == keyMaterialString) {
                throw new ParquetCryptoRuntimeException("Null key material for keyIDinFile: " + keyIDinFile);
            }
            keyMaterial = KeyMaterial.parse(keyMaterialString);
        }

        return getDEKandMasterID(keyMaterial).getDataKey();
    }

    KeyWithMasterID getDEKandMasterID(KeyMaterial keyMaterial)
    {
        if (null == kmsClientAndDetails) {
            kmsClientAndDetails = getKmsClientFromConfigOrKeyMaterial(keyMaterial);
        }

        boolean doubleWrapping = keyMaterial.isDoubleWrapped();
        String masterKeyID = keyMaterial.getMasterKeyID();
        String encodedWrappedDEK = keyMaterial.getWrappedDEK();

        byte[] dataKey;
        TrinoKmsClient kmsClient = kmsClientAndDetails.getKmsClient();
        if (!doubleWrapping) {
            dataKey = kmsClient.unwrapKey(encodedWrappedDEK, masterKeyID);
        }
        else {
            // Get KEK
            String encodedKekID = keyMaterial.getKekID();
            String encodedWrappedKEK = keyMaterial.getWrappedKEK();

            byte[] kekBytes = kekPerKekID.computeIfAbsent(encodedKekID,
                    (k) -> kmsClient.unwrapKey(encodedWrappedKEK, masterKeyID));

            if (null == kekBytes) {
                throw new ParquetCryptoRuntimeException("Null KEK, after unwrapping in KMS with master key " + masterKeyID);
            }

            // Decrypt the data key
            byte[] aad = Base64.getDecoder().decode(encodedKekID);
            dataKey = TrinoKeyToolkit.decryptKeyLocally(encodedWrappedDEK, kekBytes, aad);
        }

        return new KeyWithMasterID(dataKey, masterKeyID);
    }

    TrinoKeyToolkit.TrinoKmsClientAndDetails getKmsClientFromConfigOrKeyMaterial(KeyMaterial keyMaterial)
    {
        String kmsInstanceID = this.parquetReaderOptions.getEncryptionKmsInstanceId();
        if (Strings.isNullOrEmpty(kmsInstanceID)) {
            kmsInstanceID = keyMaterial.getKmsInstanceID();
            if (null == kmsInstanceID) {
                throw new ParquetCryptoRuntimeException("KMS instance ID is missing both in properties and file key material");
            }
        }

        String kmsInstanceURL = this.parquetReaderOptions.getEncryptionKmsInstanceUrl();
        if (Strings.isNullOrEmpty(kmsInstanceURL)) {
            kmsInstanceURL = keyMaterial.getKmsInstanceURL();
            if (null == kmsInstanceURL) {
                throw new ParquetCryptoRuntimeException("KMS instance URL is missing both in properties and file key material");
            }
        }

        TrinoKmsClient kmsClient = TrinoKeyToolkit.getKmsClient(kmsInstanceID, kmsInstanceURL, this.parquetReaderOptions, accessToken, cacheEntryLifetime);
        if (null == kmsClient) {
            throw new ParquetCryptoRuntimeException("KMSClient was not successfully created for reading encrypted data.");
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("File unwrapper - KmsClient: {}; InstanceId: {}; InstanceURL: {}", kmsClient, kmsInstanceID, kmsInstanceURL);
        }
        return new TrinoKeyToolkit.TrinoKmsClientAndDetails(kmsClient, kmsInstanceID, kmsInstanceURL);
    }
}
