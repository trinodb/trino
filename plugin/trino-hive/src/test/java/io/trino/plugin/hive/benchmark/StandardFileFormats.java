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

import com.google.common.collect.ImmutableMap;
import io.trino.filesystem.hdfs.HdfsFileSystemFactory;
import io.trino.filesystem.local.LocalOutputFile;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.hive.formats.encodings.ColumnEncodingFactory;
import io.trino.hive.formats.encodings.binary.BinaryColumnEncodingFactory;
import io.trino.hive.formats.encodings.text.TextColumnEncodingFactory;
import io.trino.hive.formats.rcfile.RcFileWriter;
import io.trino.orc.OrcReaderOptions;
import io.trino.orc.OrcWriter;
import io.trino.orc.OrcWriterOptions;
import io.trino.orc.OrcWriterStats;
import io.trino.orc.OutputStreamOrcDataSink;
import io.trino.orc.metadata.OrcType;
import io.trino.parquet.writer.ParquetSchemaConverter;
import io.trino.parquet.writer.ParquetWriter;
import io.trino.parquet.writer.ParquetWriterOptions;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.hive.HiveCompressionCodec;
import io.trino.plugin.hive.HiveConfig;
import io.trino.plugin.hive.HivePageSourceFactory;
import io.trino.plugin.hive.HiveStorageFormat;
import io.trino.plugin.hive.orc.OrcPageSourceFactory;
import io.trino.plugin.hive.parquet.ParquetPageSourceFactory;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.hive.rcfile.RcFilePageSourceFactory;
import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.Type;
import org.joda.time.DateTimeZone;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static io.trino.orc.OrcWriteValidation.OrcWriteValidationMode.BOTH;
import static io.trino.parquet.writer.ParquetSchemaConverter.HIVE_PARQUET_USE_INT96_TIMESTAMP_ENCODING;
import static io.trino.parquet.writer.ParquetSchemaConverter.HIVE_PARQUET_USE_LEGACY_DECIMAL_ENCODING;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_FILE_SYSTEM_FACTORY;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_FILE_SYSTEM_STATS;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.joda.time.DateTimeZone.UTC;

public final class StandardFileFormats
{
    private StandardFileFormats() {}

    public static final FileFormat TRINO_RCBINARY = new AbstractFileFormat()
    {
        @Override
        public HiveStorageFormat getFormat()
        {
            return HiveStorageFormat.RCBINARY;
        }

        @Override
        public Optional<HivePageSourceFactory> getHivePageSourceFactory(HdfsEnvironment hdfsEnvironment)
        {
            return Optional.of(new RcFilePageSourceFactory(TESTING_TYPE_MANAGER, HDFS_FILE_SYSTEM_FACTORY, new FileFormatDataSourceStats(), new HiveConfig().setRcfileTimeZone("UTC")));
        }

        @Override
        public FormatWriter createFileFormatWriter(
                ConnectorSession session,
                File targetFile,
                List<String> columnNames,
                List<Type> columnTypes,
                HiveCompressionCodec compressionCodec)
                throws IOException
        {
            return new PrestoRcFileFormatWriter(
                    targetFile,
                    columnTypes,
                    new BinaryColumnEncodingFactory(UTC),
                    compressionCodec);
        }
    };

    public static final FileFormat TRINO_RCTEXT = new AbstractFileFormat()
    {
        @Override
        public HiveStorageFormat getFormat()
        {
            return HiveStorageFormat.RCTEXT;
        }

        @Override
        public Optional<HivePageSourceFactory> getHivePageSourceFactory(HdfsEnvironment hdfsEnvironment)
        {
            return Optional.of(new RcFilePageSourceFactory(TESTING_TYPE_MANAGER, HDFS_FILE_SYSTEM_FACTORY, new FileFormatDataSourceStats(), new HiveConfig().setRcfileTimeZone("UTC")));
        }

        @Override
        public FormatWriter createFileFormatWriter(
                ConnectorSession session,
                File targetFile,
                List<String> columnNames,
                List<Type> columnTypes,
                HiveCompressionCodec compressionCodec)
                throws IOException
        {
            return new PrestoRcFileFormatWriter(
                    targetFile,
                    columnTypes,
                    new TextColumnEncodingFactory(),
                    compressionCodec);
        }
    };

    public static final FileFormat TRINO_ORC = new AbstractFileFormat()
    {
        @Override
        public HiveStorageFormat getFormat()
        {
            return HiveStorageFormat.ORC;
        }

        @Override
        public Optional<HivePageSourceFactory> getHivePageSourceFactory(HdfsEnvironment hdfsEnvironment)
        {
            return Optional.of(new OrcPageSourceFactory(new OrcReaderOptions(), HDFS_FILE_SYSTEM_FACTORY, new FileFormatDataSourceStats(), UTC));
        }

        @Override
        public FormatWriter createFileFormatWriter(
                ConnectorSession session,
                File targetFile,
                List<String> columnNames,
                List<Type> columnTypes,
                HiveCompressionCodec compressionCodec)
                throws IOException
        {
            return new PrestoOrcFormatWriter(
                    targetFile,
                    columnNames,
                    columnTypes,
                    compressionCodec);
        }
    };

    public static final FileFormat TRINO_PARQUET = new AbstractFileFormat()
    {
        @Override
        public HiveStorageFormat getFormat()
        {
            return HiveStorageFormat.PARQUET;
        }

        @Override
        public Optional<HivePageSourceFactory> getHivePageSourceFactory(HdfsEnvironment hdfsEnvironment)
        {
            return Optional.of(new ParquetPageSourceFactory(
                    new HdfsFileSystemFactory(hdfsEnvironment, HDFS_FILE_SYSTEM_STATS),
                    new FileFormatDataSourceStats(),
                    new ParquetReaderConfig(),
                    new HiveConfig()));
        }

        @Override
        public FormatWriter createFileFormatWriter(
                ConnectorSession session,
                File targetFile,
                List<String> columnNames,
                List<Type> columnTypes,
                HiveCompressionCodec compressionCodec)
                throws IOException
        {
            return new PrestoParquetFormatWriter(targetFile, columnNames, columnTypes, compressionCodec);
        }
    };

    private static class PrestoParquetFormatWriter
            implements FormatWriter
    {
        private final ParquetWriter writer;

        public PrestoParquetFormatWriter(File targetFile, List<String> columnNames, List<Type> types, HiveCompressionCodec compressionCodec)
                throws IOException
        {
            ParquetSchemaConverter schemaConverter = new ParquetSchemaConverter(
                    types,
                    columnNames,
                    HIVE_PARQUET_USE_LEGACY_DECIMAL_ENCODING,
                    HIVE_PARQUET_USE_INT96_TIMESTAMP_ENCODING);

            writer = new ParquetWriter(
                    new FileOutputStream(targetFile),
                    schemaConverter.getMessageType(),
                    schemaConverter.getPrimitiveTypes(),
                    ParquetWriterOptions.builder().build(),
                    compressionCodec.getParquetCompressionCodec(),
                    "test-version",
                    false,
                    Optional.of(DateTimeZone.getDefault()),
                    Optional.empty());
        }

        @Override
        public void writePage(Page page)
                throws IOException
        {
            writer.write(page);
        }

        @Override
        public void close()
                throws IOException
        {
            writer.close();
        }
    }

    private static class PrestoOrcFormatWriter
            implements FormatWriter
    {
        private final OrcWriter writer;

        public PrestoOrcFormatWriter(File targetFile, List<String> columnNames, List<Type> types, HiveCompressionCodec compressionCodec)
                throws IOException
        {
            writer = new OrcWriter(
                    OutputStreamOrcDataSink.create(new LocalOutputFile(targetFile)),
                    columnNames,
                    types,
                    OrcType.createRootOrcType(columnNames, types),
                    compressionCodec.getOrcCompressionKind(),
                    new OrcWriterOptions(),
                    ImmutableMap.of(),
                    false,
                    BOTH,
                    new OrcWriterStats());
        }

        @Override
        public void writePage(Page page)
                throws IOException
        {
            writer.write(page);
        }

        @Override
        public void close()
                throws IOException
        {
            writer.close();
        }
    }

    private static class PrestoRcFileFormatWriter
            implements FormatWriter
    {
        private final RcFileWriter writer;

        public PrestoRcFileFormatWriter(File targetFile, List<Type> types, ColumnEncodingFactory encoding, HiveCompressionCodec compressionCodec)
                throws IOException
        {
            writer = new RcFileWriter(
                    new FileOutputStream(targetFile),
                    types,
                    encoding,
                    compressionCodec.getHiveCompressionKind(),
                    ImmutableMap.of(),
                    true);
        }

        @Override
        public void writePage(Page page)
                throws IOException
        {
            writer.write(page);
        }

        @Override
        public void close()
                throws IOException
        {
            writer.close();
        }
    }
}
