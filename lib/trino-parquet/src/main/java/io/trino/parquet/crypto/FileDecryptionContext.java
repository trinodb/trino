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

import io.airlift.log.Logger;
import io.trino.parquet.ParquetDataSourceId;
import org.apache.parquet.format.BlockCipher.Decryptor;
import org.apache.parquet.format.EncryptionAlgorithm;
import org.apache.parquet.hadoop.metadata.ColumnPath;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.primitives.Bytes.concat;
import static io.trino.parquet.ParquetValidationUtils.validateParquetCrypto;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class FileDecryptionContext
{
    private static final Logger log = Logger.get(FileDecryptionContext.class);

    private final Map<ColumnPath, Optional<ColumnDecryptionContext>> columnDecryptionContext = new HashMap<>();

    private final ParquetDataSourceId dataSourceId;
    private final DecryptionKeyRetriever keyRetriever;
    private final EncryptionAlgorithm algorithm;
    private final Optional<byte[]> footerKey;
    private final byte[] fileAad;

    private AesGcmDecryptor aesGcmDecryptorWithFooterKey;
    private AesCtrDecryptor aesCtrDecryptorWithFooterKey;

    public FileDecryptionContext(ParquetDataSourceId dataSourceId, FileDecryptionProperties fileDecryptionProperties, EncryptionAlgorithm algorithm, Optional<byte[]> footerKeyMetadata)
    {
        log.debug("File Decryptor. Algo: %s", algorithm);

        requireNonNull(fileDecryptionProperties, "fileDecryptionProperties is null");
        this.dataSourceId = requireNonNull(dataSourceId, "dataSourceId is null");
        this.keyRetriever = fileDecryptionProperties.getKeyRetriever();
        this.algorithm = requireNonNull(algorithm, "algorithm is null");

        byte[] aadFileUnique;
        boolean mustSupplyAadPrefix;
        byte[] aadPrefixInFile = null;

        // Process encryption algorithm metadata
        if (algorithm.isSetAES_GCM_V1()) {
            if (algorithm.getAES_GCM_V1().isSetAad_prefix()) {
                aadPrefixInFile = algorithm.getAES_GCM_V1().getAad_prefix();
            }
            mustSupplyAadPrefix = algorithm.getAES_GCM_V1().isSupply_aad_prefix();
            aadFileUnique = algorithm.getAES_GCM_V1().getAad_file_unique();
        }
        else if (algorithm.isSetAES_GCM_CTR_V1()) {
            if (algorithm.getAES_GCM_CTR_V1().isSetAad_prefix()) {
                aadPrefixInFile = algorithm.getAES_GCM_CTR_V1().getAad_prefix();
            }
            mustSupplyAadPrefix = algorithm.getAES_GCM_CTR_V1().isSupply_aad_prefix();
            aadFileUnique = algorithm.getAES_GCM_CTR_V1().getAad_file_unique();
        }
        else {
            throw new UnsupportedOperationException(format("Unsupported algorithm: %s", algorithm));
        }

        // Determine AAD prefix, in-file AAD prefix takes precedence
        Optional<byte[]> aadPrefix = Optional.ofNullable(aadPrefixInFile).or(fileDecryptionProperties::getAadPrefix);
        validateParquetCrypto(!mustSupplyAadPrefix || aadPrefix.isPresent(), dataSourceId, "AAD prefix must be supplied");
        fileAad = aadPrefix.map(bytes -> concat(bytes, aadFileUnique)).orElse(aadFileUnique);
        footerKey = fileDecryptionProperties.getKeyRetriever().getFooterKey(footerKeyMetadata);
    }

    public Optional<ColumnDecryptionContext> getColumnDecryptionContext(ColumnPath path)
    {
        Optional<ColumnDecryptionContext> context = columnDecryptionContext.get(path);
        checkArgument(context != null, "Column %s not found in decryption context", path);
        return context;
    }

    public Decryptor getFooterDecryptor()
    {
        validateParquetCrypto(footerKey.isPresent(), dataSourceId, "User does not have access to footer or footer key does not exists");
        return getThriftModuleDecryptor(Optional.empty());
    }

    public AesGcmEncryptor getFooterEncryptor()
    {
        validateParquetCrypto(footerKey.isPresent(), dataSourceId, "User does not have access to footer or footer key does not exists");
        return new AesGcmEncryptor(footerKey.get());
    }

    public byte[] getFileAad()
    {
        return this.fileAad;
    }

    public void initPlaintextColumn(ColumnPath path)
    {
        log.debug("Column decryption (plaintext): %s", path);
        setColumnDecryptionContext(path, Optional.empty());
    }

    public Optional<ColumnDecryptionContext> initializeColumnCryptoMetadata(ColumnPath path, boolean encryptedWithFooterKey, Optional<byte[]> columnKeyMetadata)
    {
        log.debug("Column decryption (footer key): %s", path);

        Optional<ColumnDecryptionContext> context;
        if (encryptedWithFooterKey) {
            if (footerKey.isEmpty()) {
                // User does not have access to the footer key. Column is considered hidden.
                setColumnDecryptionContext(path, Optional.empty());
                return Optional.empty();
            }
            context = Optional.of(new ColumnDecryptionContext(getDataModuleDecryptor(Optional.empty()), getThriftModuleDecryptor(Optional.empty()), fileAad));
        }
        else {
            // Column is encrypted with column-specific key
            Optional<byte[]> columnKeyBytes = requireNonNull(keyRetriever.getColumnKey(path, columnKeyMetadata), format("Column key for %s not found", path));
            if (columnKeyBytes.isEmpty()) {
                // User does not have access to the column key. Column is considered hidden.
                setColumnDecryptionContext(path, Optional.empty());
                return Optional.empty();
            }
            context = Optional.of(new ColumnDecryptionContext(getDataModuleDecryptor(columnKeyBytes), getThriftModuleDecryptor(columnKeyBytes), fileAad));
        }

        setColumnDecryptionContext(path, context);
        return context;
    }

    private void setColumnDecryptionContext(ColumnPath path, Optional<ColumnDecryptionContext> context)
    {
        checkArgument(!columnDecryptionContext.containsKey(path) || columnDecryptionContext.get(path).equals(context), "Mismatching column %s encryption context already exists in decryption context", path);
        columnDecryptionContext.put(path, context);
    }

    private Decryptor getDataModuleDecryptor(Optional<byte[]> columnKey)
    {
        if (algorithm.isSetAES_GCM_V1()) {
            return getThriftModuleDecryptor(columnKey);
        }

        // AES_GCM_CTR_V1
        if (columnKey.isEmpty()) {
            // Decryptor with footer key
            if (aesCtrDecryptorWithFooterKey == null) {
                aesCtrDecryptorWithFooterKey = new AesCtrDecryptor(footerKey.get());
            }
            return aesCtrDecryptorWithFooterKey;
        }
        else {
            // Decryptor with column key
            return new AesCtrDecryptor(columnKey.orElseThrow());
        }
    }

    private Decryptor getThriftModuleDecryptor(Optional<byte[]> columnKey)
    {
        if (columnKey.isEmpty()) {
            // Decryptor with footer key
            if (aesGcmDecryptorWithFooterKey == null) {
                aesGcmDecryptorWithFooterKey = new AesGcmDecryptor(footerKey.get());
            }
            return aesGcmDecryptorWithFooterKey;
        }

        // Decryptor with column key
        return new AesGcmDecryptor(columnKey.get());
    }
}
