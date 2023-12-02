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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.crypto.DecryptionPropertiesFactory;
import org.apache.parquet.crypto.FileDecryptionProperties;
import org.apache.parquet.crypto.InternalFileDecryptor;

import java.util.Optional;

import static org.apache.parquet.crypto.DecryptionPropertiesFactory.loadFactory;

public class EncryptionUtils
{
    private static final String PARQUET_CRYPTO_FACTORY_CLASS = "parquet.crypto.factory.class";
    private static final String PARQUET_ENCRYPTION_KMS_CLIENT_CLASS = "parquet.encryption.kms.client.class";

    private EncryptionUtils() {}

    public static Optional<InternalFileDecryptor> createDecryptor(ParquetReaderOptions parquetReaderOptions, String path)
    {
        // TODO(wyu): not a fan of recreting configuration with the options, but not sure if I should add this dep to something like trino-hive, hudi, etc.
        // maybe add it to the trino hadoop-apache dep?
        Configuration configuration = new Configuration();
        configuration.set(PARQUET_CRYPTO_FACTORY_CLASS, parquetReaderOptions.getCryptoFactoryClass());
        configuration.set(PARQUET_ENCRYPTION_KMS_CLIENT_CLASS, parquetReaderOptions.getEncryptionKmsClientClass());
        DecryptionPropertiesFactory cryptoFactory = loadFactory(configuration);
        FileDecryptionProperties fileDecryptionProperties = (cryptoFactory == null) ? null : cryptoFactory.getFileDecryptionProperties(configuration, new Path(path));
        return (fileDecryptionProperties == null) ? Optional.empty() : Optional.of(new InternalFileDecryptor(fileDecryptionProperties));
    }
}
