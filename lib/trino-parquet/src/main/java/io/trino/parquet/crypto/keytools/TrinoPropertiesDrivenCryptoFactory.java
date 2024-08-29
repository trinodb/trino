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

import io.airlift.log.Logger;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.crypto.DecryptionKeyRetriever;
import io.trino.parquet.crypto.FileDecryptionProperties;
import io.trino.parquet.crypto.ParquetCryptoRuntimeException;
import io.trino.parquet.crypto.TrinoDecryptionPropertiesFactory;

public class TrinoPropertiesDrivenCryptoFactory
        implements TrinoDecryptionPropertiesFactory
{
    private static final Logger LOG = Logger.get(TrinoPropertiesDrivenCryptoFactory.class);

    @Override
    public FileDecryptionProperties getFileDecryptionProperties(ParquetReaderOptions parquetReaderOptions, Location filePath, TrinoFileSystem trinoFileSystem)
            throws ParquetCryptoRuntimeException
    {
        DecryptionKeyRetriever keyRetriever = new TrinoFileKeyUnwrapper(parquetReaderOptions, filePath, trinoFileSystem);

        if (LOG.isDebugEnabled()) {
            LOG.debug("File decryption properties for {}", filePath);
        }

        return FileDecryptionProperties.builder()
                .withKeyRetriever(keyRetriever)
                .withPlaintextFilesAllowed()
                .build();
    }
}
