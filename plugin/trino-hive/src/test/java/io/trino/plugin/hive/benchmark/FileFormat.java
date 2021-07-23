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
package io.trino.plugin.hive.benchmark;

import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.HiveCompressionCodec;
import io.trino.plugin.hive.HivePageSourceFactory;
import io.trino.plugin.hive.HiveRecordCursorProvider;
import io.trino.plugin.hive.HiveStorageFormat;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.Type;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

public interface FileFormat
{
    HiveStorageFormat getFormat();

    FormatWriter createFileFormatWriter(
            ConnectorSession session,
            File targetFile,
            List<String> columnNames,
            List<Type> columnTypes,
            HiveCompressionCodec compressionCodec)
            throws IOException;

    boolean supportsDate();

    Optional<HivePageSourceFactory> getHivePageSourceFactory(HdfsEnvironment environment);

    Optional<HiveRecordCursorProvider> getHiveRecordCursorProvider(HdfsEnvironment environment);

    ConnectorPageSource createFileFormatReader(
            ConnectorSession session,
            HdfsEnvironment hdfsEnvironment,
            File targetFile,
            List<String> columnNames,
            List<Type> columnTypes);

    ConnectorPageSource createGenericReader(
            ConnectorSession session,
            HdfsEnvironment hdfsEnvironment,
            File targetFile,
            List<ColumnHandle> readColumns,
            List<String> schemaColumnNames,
            List<Type> schemaColumnTypes);

    boolean supports(TestData testData);
}
