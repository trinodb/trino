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

import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;

public interface TrinoDecryptionPropertiesFactory
{
    // TODO(wyu): maybe create a dedicate config class in org.apache.parquet and convert ParquetReaderOptions to this class?
    FileDecryptionProperties getFileDecryptionProperties(io.trino.parquet.ParquetReaderOptions parquetReaderOptions, Location filePath, TrinoFileSystem trinoFileSystem)
            throws ParquetCryptoRuntimeException;
}
