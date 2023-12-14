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
import org.apache.parquet.crypto.FileDecryptionProperties;
import org.apache.parquet.crypto.InternalFileDecryptor;
import org.apache.parquet.crypto.TrinoCryptoConfigurationUtil;
import org.apache.parquet.crypto.TrinoDecryptionPropertiesFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.Optional;

public class EncryptionUtils
{
    public static final Logger LOG = Logger.get(EncryptionUtils.class);

    private EncryptionUtils() {}

    public static Optional<InternalFileDecryptor> createDecryptor(ParquetReaderOptions parquetReaderOptions, Location filePath, TrinoFileSystem trinoFileSystem)
    {
        TrinoDecryptionPropertiesFactory cryptoFactory = loadDecryptionPropertiesFactory(parquetReaderOptions);
        FileDecryptionProperties fileDecryptionProperties = (cryptoFactory == null) ? null : cryptoFactory.getFileDecryptionProperties(parquetReaderOptions, filePath, trinoFileSystem);
        return (fileDecryptionProperties == null) ? Optional.empty() : Optional.of(new InternalFileDecryptor(fileDecryptionProperties));
    }

    private static TrinoDecryptionPropertiesFactory loadDecryptionPropertiesFactory(ParquetReaderOptions trinoParquetCryptoConfig)
    {
        final Class<?> foundClass = TrinoCryptoConfigurationUtil.getClassFromConfig(
                trinoParquetCryptoConfig.getCryptoFactoryClass(), TrinoDecryptionPropertiesFactory.class);

        try {
            return (TrinoDecryptionPropertiesFactory) foundClass.getConstructor().newInstance();
        }
        catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
            LOG.warn("could not instantiate decryptionPropertiesFactoryClass class: " + foundClass, e);
            return null;
        }
    }
}
