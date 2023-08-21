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
package io.trino.plugin.hive.line;

import io.trino.filesystem.Location;
import io.trino.plugin.hive.FileWriter;
import io.trino.plugin.hive.HiveCompressionCodec;
import io.trino.plugin.hive.HiveFileWriterFactory;
import io.trino.plugin.hive.WriterKind;
import io.trino.plugin.hive.acid.AcidTransaction;
import io.trino.plugin.hive.metastore.StorageFormat;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;

import static io.trino.plugin.hive.HiveErrorCode.HIVE_WRITER_OPEN_ERROR;
import static io.trino.plugin.hive.util.HiveClassNames.REGEX_SERDE_CLASS;

public class RegexFileWriterFactory
        implements HiveFileWriterFactory
{
    @Override
    public Optional<FileWriter> createFileWriter(
            Location location,
            List<String> inputColumnNames,
            StorageFormat storageFormat,
            HiveCompressionCodec compressionCodec,
            Properties schema,
            ConnectorSession session,
            OptionalInt bucketNumber,
            AcidTransaction transaction,
            boolean useAcidSchema,
            WriterKind writerKind)
    {
        if (REGEX_SERDE_CLASS.equals(storageFormat.getSerde())) {
            throw new TrinoException(HIVE_WRITER_OPEN_ERROR, "REGEX format is read-only");
        }
        return Optional.empty();
    }
}
