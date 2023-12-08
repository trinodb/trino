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

import io.airlift.log.Logger;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.TrinoHadoopPath;
import org.apache.parquet.crypto.DecryptionPropertiesFactory;
import org.apache.parquet.crypto.FileDecryptionProperties;
import org.apache.parquet.crypto.InternalFileDecryptor;
import org.apache.parquet.crypto.TrinoDecryptionPropertiesFactory;
import org.apache.parquet.crypto.TrinoParquetCryptoConfig;
import org.apache.parquet.hadoop.BadConfigurationException;
import org.apache.parquet.hadoop.util.TrinoCryptoConfigurationUtil;

import java.util.Optional;

import static org.apache.parquet.crypto.TrinoDecryptionPropertiesFactory.CRYPTO_FACTORY_CLASS_PROPERTY_NAME;

public class EncryptionUtils
{
    public static final Logger LOG = Logger.get(EncryptionUtils.class);
    private static final String PARQUET_CRYPTO_FACTORY_CLASS = "parquet.crypto.factory.class";
    private static final String PARQUET_ENCRYPTION_KMS_CLIENT_CLASS = "parquet.encryption.kms.client.class";

    private EncryptionUtils() {}

    public static Optional<InternalFileDecryptor> createDecryptor(ParquetReaderOptions parquetReaderOptions, Location filePath, TrinoFileSystem trinoFileSystem)
    {
        // TODO(wyu): not a fan of recreting configuration with the options, but not sure if I should add this dep to something like trino-hive, hudi, etc.
        // maybe add it to the trino hadoop-apache dep?
        Configuration configuration = new Configuration();
        configuration.set(PARQUET_CRYPTO_FACTORY_CLASS, parquetReaderOptions.getCryptoFactoryClass());
        configuration.set(PARQUET_ENCRYPTION_KMS_CLIENT_CLASS, parquetReaderOptions.getEncryptionKmsClientClass());
        // TODO(wyu)
        TrinoDecryptionPropertiesFactory cryptoFactory = loadDecryptionPropertiesFactory(
//                parquetReaderOptions.getCryptoFactoryClass()
                null
        );

        //TODO(WYU)
        TrinoParquetCryptoConfig trinoParquetCryptoConfig = null;
        FileDecryptionProperties fileDecryptionProperties = (cryptoFactory == null) ? null : cryptoFactory.getFileDecryptionProperties(trinoParquetCryptoConfig, filePath, trinoFileSystem);
        return (fileDecryptionProperties == null) ? Optional.empty() : Optional.of(new InternalFileDecryptor(fileDecryptionProperties));
    }

    // TODO(wyu): should output null instead of crashing
    private static TrinoDecryptionPropertiesFactory loadDecryptionPropertiesFactory(TrinoParquetCryptoConfig trinoParquetCryptoConfig) {
        final Class<?> foundClass = TrinoCryptoConfigurationUtil.getClassFromConfig(
                trinoParquetCryptoConfig, CRYPTO_FACTORY_CLASS_PROPERTY_NAME, DecryptionPropertiesFactory.class);

        try {
            return (TrinoDecryptionPropertiesFactory) foundClass.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new BadConfigurationException(
                    "could not instantiate decryptionPropertiesFactoryClass class: " + foundClass,
                    e);
        }
    }
}
