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

package org.apache.parquet.crypto.keytools;

import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import org.apache.parquet.crypto.DecryptionKeyRetriever;
import org.apache.parquet.crypto.FileDecryptionProperties;
import org.apache.parquet.crypto.ParquetCryptoRuntimeException;
import org.apache.parquet.crypto.TrinoDecryptionPropertiesFactory;
import io.trino.parquet.ParquetReaderOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TrinoParquetPropertiesDrivenCryptoFactory
        implements TrinoDecryptionPropertiesFactory
{
    private static final Logger LOG = LoggerFactory.getLogger(PropertiesDrivenCryptoFactory.class);

    @Override
    public FileDecryptionProperties getFileDecryptionProperties(ParquetReaderOptions conf, Location filePath, TrinoFileSystem trinoFileSystem)
            throws ParquetCryptoRuntimeException
    {

        DecryptionKeyRetriever keyRetriever = new TrinoParquetFileKeyUnwrapper(conf, filePath, trinoFileSystem);

        if (LOG.isDebugEnabled()) {
            LOG.debug("File decryption properties for {}", filePath);
        }

        return FileDecryptionProperties.builder()
                .withKeyRetriever(keyRetriever)
                .withPlaintextFilesAllowed()
                .build();
    }
}
