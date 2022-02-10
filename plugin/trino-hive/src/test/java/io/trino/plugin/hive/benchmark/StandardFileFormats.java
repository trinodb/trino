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
import io.airlift.slice.OutputStreamSliceOutput;
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
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.HiveCompressionCodec;
import io.trino.plugin.hive.HiveConfig;
import io.trino.plugin.hive.HivePageSourceFactory;
import io.trino.plugin.hive.HiveRecordCursorProvider;
import io.trino.plugin.hive.HiveStorageFormat;
import io.trino.plugin.hive.RecordFileWriter;
import io.trino.plugin.hive.orc.OrcPageSourceFactory;
import io.trino.plugin.hive.parquet.ParquetPageSourceFactory;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.hive.rcfile.RcFilePageSourceFactory;
import io.trino.rcfile.AircompressorCodecFactory;
import io.trino.rcfile.HadoopCodecFactory;
import io.trino.rcfile.RcFileEncoding;
import io.trino.rcfile.RcFileWriter;
import io.trino.rcfile.binary.BinaryRcFileEncoding;
import io.trino.rcfile.text.TextRcFileEncoding;
import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.Type;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static io.trino.orc.OrcWriteValidation.OrcWriteValidationMode.BOTH;
import static io.trino.plugin.hive.HiveTestUtils.createGenericHiveRecordCursorProvider;
import static io.trino.plugin.hive.benchmark.AbstractFileFormat.createSchema;
import static io.trino.plugin.hive.metastore.StorageFormat.fromHiveStorageFormat;
import static io.trino.plugin.hive.util.CompressionConfigUtil.configureCompression;
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
            return Optional.of(new RcFilePageSourceFactory(TESTING_TYPE_MANAGER, hdfsEnvironment, new FileFormatDataSourceStats(), new HiveConfig().setRcfileTimeZone("UTC")));
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
                    new BinaryRcFileEncoding(UTC),
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
            return Optional.of(new RcFilePageSourceFactory(TESTING_TYPE_MANAGER, hdfsEnvironment, new FileFormatDataSourceStats(), new HiveConfig().setRcfileTimeZone("UTC")));
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
                    new TextRcFileEncoding(),
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
            return Optional.of(new OrcPageSourceFactory(new OrcReaderOptions(), hdfsEnvironment, new FileFormatDataSourceStats(), UTC));
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
            return Optional.of(new ParquetPageSourceFactory(hdfsEnvironment, new FileFormatDataSourceStats(), new ParquetReaderConfig(), new HiveConfig().setParquetTimeZone("UTC")));
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

    public static final FileFormat HIVE_RCBINARY = new AbstractFileFormat()
    {
        @Override
        public HiveStorageFormat getFormat()
        {
            return HiveStorageFormat.RCBINARY;
        }

        @Override
        public Optional<HiveRecordCursorProvider> getHiveRecordCursorProvider(HdfsEnvironment hdfsEnvironment)
        {
            return Optional.of(createGenericHiveRecordCursorProvider(hdfsEnvironment));
        }

        @Override
        public FormatWriter createFileFormatWriter(
                ConnectorSession session,
                File targetFile,
                List<String> columnNames,
                List<Type> columnTypes,
                HiveCompressionCodec compressionCodec)
        {
            return new RecordFormatWriter(targetFile, columnNames, columnTypes, compressionCodec, HiveStorageFormat.RCBINARY, session);
        }
    };

    public static final FileFormat HIVE_RCTEXT = new AbstractFileFormat()
    {
        @Override
        public HiveStorageFormat getFormat()
        {
            return HiveStorageFormat.RCTEXT;
        }

        @Override
        public Optional<HiveRecordCursorProvider> getHiveRecordCursorProvider(HdfsEnvironment hdfsEnvironment)
        {
            return Optional.of(createGenericHiveRecordCursorProvider(hdfsEnvironment));
        }

        @Override
        public FormatWriter createFileFormatWriter(
                ConnectorSession session,
                File targetFile,
                List<String> columnNames,
                List<Type> columnTypes,
                HiveCompressionCodec compressionCodec)
        {
            return new RecordFormatWriter(targetFile, columnNames, columnTypes, compressionCodec, HiveStorageFormat.RCTEXT, session);
        }
    };

    public static final FileFormat HIVE_ORC = new AbstractFileFormat()
    {
        @Override
        public HiveStorageFormat getFormat()
        {
            return HiveStorageFormat.ORC;
        }

        @Override
        public Optional<HiveRecordCursorProvider> getHiveRecordCursorProvider(HdfsEnvironment hdfsEnvironment)
        {
            return Optional.of(createGenericHiveRecordCursorProvider(hdfsEnvironment));
        }

        @Override
        public FormatWriter createFileFormatWriter(
                ConnectorSession session,
                File targetFile,
                List<String> columnNames,
                List<Type> columnTypes,
                HiveCompressionCodec compressionCodec)
        {
            return new RecordFormatWriter(targetFile, columnNames, columnTypes, compressionCodec, HiveStorageFormat.ORC, session);
        }
    };

    public static final FileFormat HIVE_PARQUET = new AbstractFileFormat()
    {
        @Override
        public HiveStorageFormat getFormat()
        {
            return HiveStorageFormat.PARQUET;
        }

        @Override
        public Optional<HiveRecordCursorProvider> getHiveRecordCursorProvider(HdfsEnvironment hdfsEnvironment)
        {
            return Optional.of(createGenericHiveRecordCursorProvider(hdfsEnvironment));
        }

        @Override
        public FormatWriter createFileFormatWriter(
                ConnectorSession session,
                File targetFile,
                List<String> columnNames,
                List<Type> columnTypes,
                HiveCompressionCodec compressionCodec)
        {
            return new RecordFormatWriter(targetFile, columnNames, columnTypes, compressionCodec, HiveStorageFormat.PARQUET, session);
        }
    };

    private static class PrestoParquetFormatWriter
            implements FormatWriter
    {
        private final ParquetWriter writer;

        public PrestoParquetFormatWriter(File targetFile, List<String> columnNames, List<Type> types, HiveCompressionCodec compressionCodec)
                throws IOException
        {
            ParquetSchemaConverter schemaConverter = new ParquetSchemaConverter(types, columnNames);

            writer = new ParquetWriter(
                    new FileOutputStream(targetFile),
                    schemaConverter.getMessageType(),
                    schemaConverter.getPrimitiveTypes(),
                    ParquetWriterOptions.builder().build(),
                    compressionCodec.getParquetCompressionCodec(),
                    "test-version");
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
                    new OutputStreamOrcDataSink(new FileOutputStream(targetFile)),
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

        public PrestoRcFileFormatWriter(File targetFile, List<Type> types, RcFileEncoding encoding, HiveCompressionCodec compressionCodec)
                throws IOException
        {
            writer = new RcFileWriter(
                    new OutputStreamSliceOutput(new FileOutputStream(targetFile)),
                    types,
                    encoding,
                    compressionCodec.getCodec().map(Class::getName),
                    new AircompressorCodecFactory(new HadoopCodecFactory(getClass().getClassLoader())),
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

    private static class RecordFormatWriter
            implements FormatWriter
    {
        private final RecordFileWriter recordWriter;

        public RecordFormatWriter(
                File targetFile,
                List<String> columnNames,
                List<Type> columnTypes,
                HiveCompressionCodec compressionCodec,
                HiveStorageFormat format,
                ConnectorSession session)
        {
            JobConf config = new JobConf(AbstractFileFormat.conf);
            configureCompression(config, compressionCodec);

            recordWriter = new RecordFileWriter(
                    new Path(targetFile.toURI()),
                    columnNames,
                    fromHiveStorageFormat(format),
                    createSchema(format, columnNames, columnTypes),
                    format.getEstimatedWriterMemoryUsage(),
                    config,
                    TESTING_TYPE_MANAGER,
                    UTC,
                    session);
        }

        @Override
        public void writePage(Page page)
        {
            for (int position = 0; position < page.getPositionCount(); position++) {
                recordWriter.appendRow(page, position);
            }
        }

        @Override
        public void close()
        {
            recordWriter.commit();
        }
    }
}
