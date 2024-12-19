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
package io.trino.parquet;

import io.trino.parquet.crypto.keytools.TrinoKeyToolkit;
import io.trino.parquet.crypto.keytools.TrinoKmsClient;

public class ParquetReaderEncryptionOptions
{
    final String cryptoFactoryClass;
    final String encryptionKmsClientClass;
    final String encryptionKmsInstanceId;
    final String encryptionKmsInstanceUrl;
    final String encryptionKeyAccessToken;
    final long encryptionCacheLifetimeSeconds;
    final boolean uniformEncryption;
    boolean encryptionParameterChecked;
    final String failsafeEncryptionKeyId;
    final String columnKeys;
    final String footerKeyId;
    final String[] versionedKeyList;
    final String keyFile;
    final String[] keyList;
    final boolean isEncryptionEnvironmentKeys;

    public ParquetReaderEncryptionOptions()
    {
        this.cryptoFactoryClass = null;
        this.encryptionKmsClientClass = null;
        this.encryptionKmsInstanceId = null;
        this.encryptionKmsInstanceUrl = null;
        this.encryptionKeyAccessToken = TrinoKmsClient.KEY_ACCESS_TOKEN_DEFAULT;
        this.encryptionCacheLifetimeSeconds = TrinoKeyToolkit.CACHE_LIFETIME_DEFAULT_SECONDS;
        this.uniformEncryption = false;
        this.encryptionParameterChecked = false;
        this.failsafeEncryptionKeyId = null;
        this.footerKeyId = null;
        this.columnKeys = null;
        this.versionedKeyList = null;
        this.keyFile = null;
        this.keyList = null;
        this.isEncryptionEnvironmentKeys = false;
    }

    public ParquetReaderEncryptionOptions(String cryptoFactoryClass,
                                          String encryptionKmsClientClass,
                                          String encryptionKmsInstanceId,
                                          String encryptionKmsInstanceUrl,
                                          String encryptionKeyAccessToken,
                                          long encryptionCacheLifetimeSeconds,
                                          boolean uniformEncryption,
                                          boolean encryptionParameterChecked,
                                          String failsafeEncryptionKeyId,
                                          String footerKeyId,
                                          String columnKeys,
                                          String[] versionedKeyList,
                                          String keyFile,
                                          String[] keyList,
                                          boolean isEncryptionEnvironmentKeys)
    {
        this.cryptoFactoryClass = cryptoFactoryClass;
        this.encryptionKmsClientClass = encryptionKmsClientClass;
        this.encryptionKmsInstanceId = encryptionKmsInstanceId;
        this.encryptionKmsInstanceUrl = encryptionKmsInstanceUrl;
        this.encryptionKeyAccessToken = encryptionKeyAccessToken;
        this.encryptionCacheLifetimeSeconds = encryptionCacheLifetimeSeconds;
        this.uniformEncryption = uniformEncryption;
        this.encryptionParameterChecked = encryptionParameterChecked;
        this.failsafeEncryptionKeyId = failsafeEncryptionKeyId;
        this.footerKeyId = footerKeyId;
        this.columnKeys = columnKeys;
        this.versionedKeyList = versionedKeyList;
        this.keyFile = keyFile;
        this.keyList = keyList;
        this.isEncryptionEnvironmentKeys = isEncryptionEnvironmentKeys;
    }

    public ParquetReaderEncryptionOptions withEncryptionKmsClientClass(String encryptionKmsClientClass)
    {
        return new ParquetReaderEncryptionOptions(this.cryptoFactoryClass,
                encryptionKmsClientClass,
                this.encryptionKmsInstanceId,
                this.encryptionKmsInstanceUrl,
                this.encryptionKeyAccessToken,
                this.encryptionCacheLifetimeSeconds,
                this.uniformEncryption,
                this.encryptionParameterChecked,
                this.failsafeEncryptionKeyId,
                this.footerKeyId,
                this.columnKeys,
                this.versionedKeyList,
                this.keyFile,
                this.keyList,
                this.isEncryptionEnvironmentKeys);
    }

    public ParquetReaderEncryptionOptions withCryptoFactoryClass(String cryptoFactoryClass)
    {
        return new ParquetReaderEncryptionOptions(cryptoFactoryClass,
                this.encryptionKmsClientClass,
                this.encryptionKmsInstanceId,
                this.encryptionKmsInstanceUrl,
                this.encryptionKeyAccessToken,
                this.encryptionCacheLifetimeSeconds,
                this.uniformEncryption,
                this.encryptionParameterChecked,
                this.failsafeEncryptionKeyId,
                this.footerKeyId,
                this.columnKeys,
                this.versionedKeyList,
                this.keyFile,
                this.keyList,
                this.isEncryptionEnvironmentKeys);
    }

    public ParquetReaderEncryptionOptions withEncryptionKmsInstanceId(String encryptionKmsInstanceId)
    {
        return new ParquetReaderEncryptionOptions(this.cryptoFactoryClass,
                this.encryptionKmsClientClass,
                encryptionKmsInstanceId,
                this.encryptionKmsInstanceUrl,
                this.encryptionKeyAccessToken,
                this.encryptionCacheLifetimeSeconds,
                this.uniformEncryption,
                this.encryptionParameterChecked,
                this.failsafeEncryptionKeyId,
                this.footerKeyId,
                this.columnKeys,
                this.versionedKeyList,
                this.keyFile,
                this.keyList,
                this.isEncryptionEnvironmentKeys);
    }

    public ParquetReaderEncryptionOptions withEncryptionKmsInstanceUrl(String encryptionKmsInstanceUrl)
    {
        return new ParquetReaderEncryptionOptions(this.cryptoFactoryClass,
                this.encryptionKmsClientClass,
                this.encryptionKmsInstanceId,
                encryptionKmsInstanceUrl,
                this.encryptionKeyAccessToken,
                this.encryptionCacheLifetimeSeconds,
                this.uniformEncryption,
                this.encryptionParameterChecked,
                this.failsafeEncryptionKeyId,
                this.footerKeyId,
                this.columnKeys,
                this.versionedKeyList,
                this.keyFile,
                this.keyList,
                this.isEncryptionEnvironmentKeys);
    }

    public ParquetReaderEncryptionOptions withEncryptionKeyAccessToken(String encryptionKeyAccessToken)
    {
        return new ParquetReaderEncryptionOptions(this.cryptoFactoryClass,
                this.encryptionKmsClientClass,
                this.encryptionKmsInstanceId,
                this.encryptionKmsInstanceUrl,
                encryptionKeyAccessToken,
                this.encryptionCacheLifetimeSeconds,
                this.uniformEncryption,
                this.encryptionParameterChecked,
                this.failsafeEncryptionKeyId,
                this.footerKeyId,
                this.columnKeys,
                this.versionedKeyList,
                this.keyFile,
                this.keyList,
                this.isEncryptionEnvironmentKeys);
    }

    public ParquetReaderEncryptionOptions withEncryptionCacheLifetimeSeconds(Long encryptionCacheLifetimeSeconds)
    {
        return new ParquetReaderEncryptionOptions(this.cryptoFactoryClass,
                this.encryptionKmsClientClass,
                this.encryptionKmsInstanceId,
                this.encryptionKmsInstanceUrl,
                this.encryptionKeyAccessToken,
                encryptionCacheLifetimeSeconds,
                this.uniformEncryption,
                this.encryptionParameterChecked,
                this.failsafeEncryptionKeyId,
                this.footerKeyId,
                this.columnKeys,
                this.versionedKeyList,
                this.keyFile,
                this.keyList,
                this.isEncryptionEnvironmentKeys);
    }

    public ParquetReaderEncryptionOptions withEncryptionKeyFile(String keyFile)
    {
        return new ParquetReaderEncryptionOptions(this.cryptoFactoryClass,
                this.encryptionKmsClientClass,
                this.encryptionKmsInstanceId,
                this.encryptionKmsInstanceUrl,
                this.encryptionKeyAccessToken,
                this.encryptionCacheLifetimeSeconds,
                this.uniformEncryption,
                this.encryptionParameterChecked,
                this.failsafeEncryptionKeyId,
                this.footerKeyId,
                this.columnKeys,
                this.versionedKeyList,
                keyFile,
                this.keyList,
                this.isEncryptionEnvironmentKeys);
    }
}
