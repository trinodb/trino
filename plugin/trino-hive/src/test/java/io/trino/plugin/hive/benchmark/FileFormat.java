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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
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
import io.trino.plugin.hive.GenericHiveRecordCursorProvider;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HiveCompressionCodec;
import io.trino.plugin.hive.HiveConfig;
import io.trino.plugin.hive.HivePageSourceFactory;
import io.trino.plugin.hive.HivePageSourceProvider;
import io.trino.plugin.hive.HiveRecordCursorProvider;
import io.trino.plugin.hive.HiveRecordCursorProvider.ReaderRecordCursorWithProjections;
import io.trino.plugin.hive.HiveSplit;
import io.trino.plugin.hive.HiveStorageFormat;
import io.trino.plugin.hive.HiveTableHandle;
import io.trino.plugin.hive.HiveType;
import io.trino.plugin.hive.HiveTypeName;
import io.trino.plugin.hive.ReaderPageSource;
import io.trino.plugin.hive.RecordFileWriter;
import io.trino.plugin.hive.TableToPartitionMapping;
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
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.RecordPageSource;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import io.trino.sql.planner.TestingConnectorTransactionHandle;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.orc.OrcWriteValidation.OrcWriteValidationMode.BOTH;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.trino.plugin.hive.HiveColumnHandle.createBaseColumn;
import static io.trino.plugin.hive.HiveTestUtils.TYPE_MANAGER;
import static io.trino.plugin.hive.HiveTestUtils.createGenericHiveRecordCursorProvider;
import static io.trino.plugin.hive.HiveType.toHiveType;
import static io.trino.plugin.hive.acid.AcidTransaction.NO_ACID_TRANSACTION;
import static io.trino.plugin.hive.metastore.StorageFormat.fromHiveStorageFormat;
import static io.trino.plugin.hive.util.CompressionConfigUtil.configureCompression;
import static java.lang.String.join;
import static java.util.stream.Collectors.joining;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.FILE_INPUT_FORMAT;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_COLUMNS;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_COLUMN_TYPES;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_LIB;
import static org.joda.time.DateTimeZone.UTC;

public enum FileFormat
{
    TRINO_RCBINARY {
        @Override
        public HiveStorageFormat getFormat()
        {
            return HiveStorageFormat.RCBINARY;
        }

        @Override
        public Optional<HivePageSourceFactory> getHivePageSourceFactory(HdfsEnvironment hdfsEnvironment)
        {
            return Optional.of(new RcFilePageSourceFactory(TYPE_MANAGER, hdfsEnvironment, new FileFormatDataSourceStats(), new HiveConfig().setRcfileTimeZone("UTC")));
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
    },

    TRINO_RCTEXT {
        @Override
        public HiveStorageFormat getFormat()
        {
            return HiveStorageFormat.RCTEXT;
        }

        @Override
        public Optional<HivePageSourceFactory> getHivePageSourceFactory(HdfsEnvironment hdfsEnvironment)
        {
            return Optional.of(new RcFilePageSourceFactory(TYPE_MANAGER, hdfsEnvironment, new FileFormatDataSourceStats(), new HiveConfig().setRcfileTimeZone("UTC")));
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
    },

    TRINO_ORC {
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
    },

    TRINO_PARQUET {
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
    },

    HIVE_RCBINARY {
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
    },

    HIVE_RCTEXT {
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
    },

    HIVE_ORC {
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
    },

    HIVE_PARQUET {
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

    public boolean supportsDate()
    {
        return true;
    }

    public abstract HiveStorageFormat getFormat();

    public abstract FormatWriter createFileFormatWriter(
            ConnectorSession session,
            File targetFile,
            List<String> columnNames,
            List<Type> columnTypes,
            HiveCompressionCodec compressionCodec)
            throws IOException;

    public Optional<HivePageSourceFactory> getHivePageSourceFactory(HdfsEnvironment environment)
    {
        return Optional.empty();
    }

    public Optional<HiveRecordCursorProvider> getHiveRecordCursorProvider(HdfsEnvironment environment)
    {
        return Optional.empty();
    }

    public final ConnectorPageSource createFileFormatReader(
            ConnectorSession session,
            HdfsEnvironment hdfsEnvironment,
            File targetFile,
            List<String> columnNames,
            List<Type> columnTypes)
    {
        Optional<HivePageSourceFactory> pageSourceFactory = getHivePageSourceFactory(hdfsEnvironment);
        Optional<HiveRecordCursorProvider> recordCursorProvider = getHiveRecordCursorProvider(hdfsEnvironment);

        checkArgument(pageSourceFactory.isPresent() ^ recordCursorProvider.isPresent());

        if (pageSourceFactory.isPresent()) {
            return createPageSource(pageSourceFactory.get(), session, targetFile, columnNames, columnTypes, getFormat());
        }

        return createPageSource(recordCursorProvider.get(), session, targetFile, columnNames, columnTypes, getFormat());
    }

    public final ConnectorPageSource createGenericReader(
            ConnectorSession session,
            HdfsEnvironment hdfsEnvironment,
            File targetFile,
            List<ColumnHandle> readColumns,
            List<String> schemaColumnNames,
            List<Type> schemaColumnTypes)
    {
        HivePageSourceProvider factory = new HivePageSourceProvider(
                TYPE_MANAGER,
                hdfsEnvironment,
                new HiveConfig(),
                getHivePageSourceFactory(hdfsEnvironment).map(ImmutableSet::of).orElse(ImmutableSet.of()),
                getHiveRecordCursorProvider(hdfsEnvironment).map(ImmutableSet::of).orElse(ImmutableSet.of()),
                new GenericHiveRecordCursorProvider(hdfsEnvironment, new HiveConfig()),
                Optional.empty());

        Properties schema = createSchema(getFormat(), schemaColumnNames, schemaColumnTypes);

        HiveSplit split = new HiveSplit(
                "schema_name",
                "table_name",
                "",
                targetFile.getPath(),
                0,
                targetFile.length(),
                targetFile.length(),
                targetFile.lastModified(),
                schema,
                ImmutableList.of(),
                ImmutableList.of(),
                OptionalInt.empty(),
                0,
                false,
                TableToPartitionMapping.empty(),
                Optional.empty(),
                Optional.empty(),
                false,
                Optional.empty());

        ConnectorPageSource hivePageSource = factory.createPageSource(
                TestingConnectorTransactionHandle.INSTANCE,
                session, split,
                new HiveTableHandle("schema_name", "table_name", ImmutableMap.of(), ImmutableList.of(), ImmutableList.of(), Optional.empty()),
                readColumns,
                DynamicFilter.EMPTY);

        return hivePageSource;
    }

    private static final JobConf conf;

    static {
        conf = new JobConf(new Configuration(false));
        conf.set("fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem");
    }

    public boolean supports(TestData testData)
    {
        return true;
    }

    private static ConnectorPageSource createPageSource(
            HiveRecordCursorProvider cursorProvider,
            ConnectorSession session,
            File targetFile,
            List<String> columnNames,
            List<Type> columnTypes,
            HiveStorageFormat format)
    {
        checkArgument(columnNames.size() == columnTypes.size(), "columnNames and columnTypes should have the same size");

        List<HiveColumnHandle> readColumns = getBaseColumns(columnNames, columnTypes);

        Optional<ReaderRecordCursorWithProjections> recordCursorWithProjections = cursorProvider.createRecordCursor(
                conf,
                session,
                new Path(targetFile.getAbsolutePath()),
                0,
                targetFile.length(),
                targetFile.length(),
                createSchema(format, columnNames, columnTypes),
                readColumns,
                TupleDomain.all(),
                TYPE_MANAGER,
                false);

        checkState(recordCursorWithProjections.isPresent(), "readerPageSourceWithProjections is not present");
        checkState(recordCursorWithProjections.get().getProjectedReaderColumns().isEmpty(), "projection should not be required");
        return new RecordPageSource(columnTypes, recordCursorWithProjections.get().getRecordCursor());
    }

    private static ConnectorPageSource createPageSource(
            HivePageSourceFactory pageSourceFactory,
            ConnectorSession session,
            File targetFile,
            List<String> columnNames,
            List<Type> columnTypes,
            HiveStorageFormat format)
    {
        checkArgument(columnNames.size() == columnTypes.size(), "columnNames and columnTypes should have the same size");

        List<HiveColumnHandle> readColumns = getBaseColumns(columnNames, columnTypes);

        Properties schema = createSchema(format, columnNames, columnTypes);
        Optional<ReaderPageSource> readerPageSourceWithProjections = pageSourceFactory
                .createPageSource(
                        conf,
                        session,
                        new Path(targetFile.getAbsolutePath()),
                        0,
                        targetFile.length(),
                        targetFile.length(),
                        schema,
                        readColumns,
                        TupleDomain.all(),
                        Optional.empty(),
                        OptionalInt.empty(),
                        false,
                        NO_ACID_TRANSACTION);

        checkState(readerPageSourceWithProjections.isPresent(), "readerPageSourceWithProjections is not present");
        checkState(readerPageSourceWithProjections.get().getReaderColumns().isEmpty(), "projection should not be required");
        return readerPageSourceWithProjections.get().get();
    }

    private static List<HiveColumnHandle> getBaseColumns(List<String> columnNames, List<Type> columnTypes)
    {
        return IntStream.range(0, columnNames.size())
                .boxed()
                .map(index -> createBaseColumn(
                        columnNames.get(index),
                        index,
                        toHiveType(columnTypes.get(index)),
                        columnTypes.get(index),
                        REGULAR,
                        Optional.empty()))
                .collect(toImmutableList());
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
            JobConf config = new JobConf(conf);
            configureCompression(config, compressionCodec);

            recordWriter = new RecordFileWriter(
                    new Path(targetFile.toURI()),
                    columnNames,
                    fromHiveStorageFormat(format),
                    createSchema(format, columnNames, columnTypes),
                    format.getEstimatedWriterSystemMemoryUsage(),
                    config,
                    TYPE_MANAGER,
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

    private static Properties createSchema(HiveStorageFormat format, List<String> columnNames, List<Type> columnTypes)
    {
        Properties schema = new Properties();
        schema.setProperty(SERIALIZATION_LIB, format.getSerDe());
        schema.setProperty(FILE_INPUT_FORMAT, format.getInputFormat());
        schema.setProperty(META_TABLE_COLUMNS, join(",", columnNames));
        schema.setProperty(META_TABLE_COLUMN_TYPES, columnTypes.stream()
                .map(HiveType::toHiveType)
                .map(HiveType::getHiveTypeName)
                .map(HiveTypeName::toString)
                .collect(joining(":")));
        return schema;
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
                    false,
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
                    compressionCodec.getParquetCompressionCodec());
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
