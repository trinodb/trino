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

//import java.io.IOException;
import java.util.Base64;
import java.util.concurrent.ConcurrentMap;

import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.Path;
import io.trino.parquet.ParquetReaderOptions;
import org.apache.parquet.crypto.DecryptionKeyRetriever;
import org.apache.parquet.crypto.ParquetCryptoRuntimeException;
import org.apache.parquet.crypto.TrinoParquetCryptoConfig;
import org.apache.parquet.crypto.keytools.KeyToolkit.KeyWithMasterID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.parquet.crypto.keytools.KeyToolkit.stringIsEmpty;
import static org.apache.parquet.crypto.keytools.KeyToolkit.KMS_CLIENT_CACHE_PER_TOKEN;
import static org.apache.parquet.crypto.keytools.KeyToolkit.KEK_READ_CACHE_PER_TOKEN;

public class TrinoFileKeyUnwrapper
        implements DecryptionKeyRetriever
{
    private static final Logger LOG = LoggerFactory.getLogger(FileKeyUnwrapper.class);

    //A map of KEK_ID -> KEK bytes, for the current token
    private final ConcurrentMap<String, byte[]> kekPerKekID;

    private TrinoKeyToolkit.TrinoKmsClientAndDetails kmsClientAndDetails = null;
    private TrinoHadoopFSKeyMaterialStore keyMaterialStore = null;
    private boolean checkedKeyMaterialInternalStorage = false;
    private final Location parquetFilePath;
    // TODO(wyu): shall we get it from Location or File?
    private final TrinoFileSystem trinoFileSystem;
    private final String accessToken;
    private final long cacheEntryLifetime;
    private final TrinoParquetCryptoConfig conf;

    // TODO(wyu): replace filePath with
    TrinoFileKeyUnwrapper(TrinoParquetCryptoConfig conf, Location filePath, TrinoFileSystem trinoFileSystem)
    {
        this.trinoFileSystem = trinoFileSystem;
        this.conf = conf;
        this.parquetFilePath = filePath;

//    cacheEntryLifetime = 1000L * hadoopConfiguration.getLong(KeyToolkit.CACHE_LIFETIME_PROPERTY_NAME,
//        KeyToolkit.CACHE_LIFETIME_DEFAULT_SECONDS);
        cacheEntryLifetime = 1000L *
                KeyToolkit.CACHE_LIFETIME_DEFAULT_SECONDS;

//    accessToken = hadoopConfiguration.getTrimmed(KeyToolkit.KEY_ACCESS_TOKEN_PROPERTY_NAME,
//        KmsClient.KEY_ACCESS_TOKEN_DEFAULT);
        accessToken =
                KmsClient.KEY_ACCESS_TOKEN_DEFAULT;

        // Check cache upon each file reading (clean once in cacheEntryLifetime)
        KMS_CLIENT_CACHE_PER_TOKEN.checkCacheForExpiredTokens(cacheEntryLifetime);
        KEK_READ_CACHE_PER_TOKEN.checkCacheForExpiredTokens(cacheEntryLifetime);
        kekPerKekID = KEK_READ_CACHE_PER_TOKEN.getOrCreateInternalCache(accessToken, cacheEntryLifetime);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Creating file key unwrapper. KeyMaterialStore: {}; token snippet: {}",
                    keyMaterialStore, KeyToolkit.formatTokenForLog(accessToken));
        }
    }

    @Override
    public byte[] getKey(byte[] keyMetadataBytes)
    {
        KeyMetadata keyMetadata = KeyMetadata.parse(keyMetadataBytes);

        if (!checkedKeyMaterialInternalStorage) {
            if (!keyMetadata.keyMaterialStoredInternally()) {
//                try{
                    keyMaterialStore = new TrinoHadoopFSKeyMaterialStore(trinoFileSystem);
                    keyMaterialStore.initialize(parquetFilePath, false);
//                }
//                catch (IOException e) {
//                    throw new ParquetCryptoRuntimeException("Failed to open key material store", e);
//                }
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
            byte[] AAD = Base64.getDecoder().decode(encodedKekID);
            dataKey = KeyToolkit.decryptKeyLocally(encodedWrappedDEK, kekBytes, AAD);
        }

        return new KeyWithMasterID(dataKey, masterKeyID);
    }

    TrinoKeyToolkit.TrinoKmsClientAndDetails getKmsClientFromConfigOrKeyMaterial(KeyMaterial keyMaterial)
    {
        String kmsInstanceID = this.conf.get(KeyToolkit.KMS_INSTANCE_ID_PROPERTY_NAME);
        if (stringIsEmpty(kmsInstanceID)) {
            kmsInstanceID = keyMaterial.getKmsInstanceID();
            if (null == kmsInstanceID) {
                throw new ParquetCryptoRuntimeException("KMS instance ID is missing both in properties and file key material");
            }
        }

        String kmsInstanceURL = this.conf.get(KeyToolkit.KMS_INSTANCE_URL_PROPERTY_NAME);
        if (stringIsEmpty(kmsInstanceURL)) {
            kmsInstanceURL = keyMaterial.getKmsInstanceURL();
            if (null == kmsInstanceURL) {
                throw new ParquetCryptoRuntimeException("KMS instance URL is missing both in properties and file key material");
            }
        }

        TrinoKmsClient kmsClient = TrinoKeyToolkit.getKmsClient(kmsInstanceID, kmsInstanceURL, this.conf, accessToken, cacheEntryLifetime);
        if (null == kmsClient) {
            throw new ParquetCryptoRuntimeException("KMSClient was not successfully created for reading encrypted data.");
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("File unwrapper - KmsClient: {}; InstanceId: {}; InstanceURL: {}", kmsClient, kmsInstanceID, kmsInstanceURL);
        }
        return new TrinoKeyToolkit.TrinoKmsClientAndDetails(kmsClient, kmsInstanceID, kmsInstanceURL);
    }

    TrinoKeyToolkit.TrinoKmsClientAndDetails getKmsClientAndDetails()
    {
        return kmsClientAndDetails;
    }
}
