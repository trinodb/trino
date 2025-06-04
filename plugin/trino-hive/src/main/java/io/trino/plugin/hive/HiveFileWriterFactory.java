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
package io.trino.plugin.hive;

import io.trino.filesystem.Location;
import io.trino.metastore.StorageFormat;
import io.trino.plugin.hive.acid.AcidTransaction;
import io.trino.spi.connector.ConnectorSession;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

public interface HiveFileWriterFactory
{
    Optional<FileWriter> createFileWriter(
            Location location,
            List<String> inputColumnNames,
            StorageFormat storageFormat,
            HiveCompressionCodec compressionCodec,
            Map<String, String> schema,
            ConnectorSession session,
            OptionalInt bucketNumber,
            AcidTransaction transaction,
            boolean useAcidSchema,
            WriterKind writerKind);
}
