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
package io.prestosql.plugin.hive.benchmark;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.OutputStreamSliceOutput;
import io.prestosql.orc.OrcReaderOptions;
import io.prestosql.orc.OrcWriter;
import io.prestosql.orc.OrcWriterOptions;
import io.prestosql.orc.OrcWriterStats;
import io.prestosql.orc.OutputStreamOrcDataSink;
import io.prestosql.orc.metadata.OrcType;
import io.prestosql.parquet.writer.ParquetSchemaConverter;
import io.prestosql.parquet.writer.ParquetWriter;
import io.prestosql.parquet.writer.ParquetWriterOptions;
import io.prestosql.plugin.hive.FileFormatDataSourceStats;
import io.prestosql.plugin.hive.GenericHiveRecordCursorProvider;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HiveColumnHandle;
import io.prestosql.plugin.hive.HiveCompressionCodec;
import io.prestosql.plugin.hive.HiveConfig;
import io.prestosql.plugin.hive.HivePageSourceFactory;
import io.prestosql.plugin.hive.HivePageSourceFactory.ReaderPageSourceWithProjections;
import io.prestosql.plugin.hive.HivePageSourceProvider;
import io.prestosql.plugin.hive.HiveRecordCursorProvider;
import io.prestosql.plugin.hive.HiveRecordCursorProvider.ReaderRecordCursorWithProjections;
import io.prestosql.plugin.hive.HiveSplit;
import io.prestosql.plugin.hive.HiveStorageFormat;
import io.prestosql.plugin.hive.HiveTableHandle;
import io.prestosql.plugin.hive.HiveType;
import io.prestosql.plugin.hive.HiveTypeName;
import io.prestosql.plugin.hive.RecordFileWriter;
import io.prestosql.plugin.hive.TableToPartitionMapping;
import io.prestosql.plugin.hive.orc.OrcPageSourceFactory;
import io.prestosql.plugin.hive.parquet.ParquetPageSourceFactory;
import io.prestosql.plugin.hive.parquet.ParquetReaderConfig;
import io.prestosql.plugin.hive.rcfile.RcFilePageSourceFactory;
import io.prestosql.rcfile.AircompressorCodecFactory;
import io.prestosql.rcfile.HadoopCodecFactory;
import io.prestosql.rcfile.RcFileEncoding;
import io.prestosql.rcfile.RcFileWriter;
import io.prestosql.rcfile.binary.BinaryRcFileEncoding;
import io.prestosql.rcfile.text.TextRcFileEncoding;
import io.prestosql.spi.Page;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.RecordPageSource;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.planner.TestingConnectorTransactionHandle;
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
import static io.prestosql.orc.OrcWriteValidation.OrcWriteValidationMode.BOTH;
import static io.prestosql.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.prestosql.plugin.hive.HiveColumnHandle.createBaseColumn;
import static io.prestosql.plugin.hive.HiveTestUtils.TYPE_MANAGER;
import static io.prestosql.plugin.hive.HiveTestUtils.createGenericHiveRecordCursorProvider;
import static io.prestosql.plugin.hive.HiveType.toHiveType;
import static io.prestosql.plugin.hive.metastore.StorageFormat.fromHiveStorageFormat;
import static io.prestosql.plugin.hive.util.CompressionConfigUtil.configureCompression;
import static java.lang.String.join;
import static java.util.stream.Collectors.joining;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.FILE_INPUT_FORMAT;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_COLUMNS;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_COLUMN_TYPES;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_LIB;
import static org.joda.time.DateTimeZone.UTC;

public enum FileFormat
{
    PRESTO_RCBINARY {
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

    PRESTO_RCTEXT {
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

    PRESTO_ORC {
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

    PRESTO_PARQUET {
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
                getHivePageSourceFactory(hdfsEnvironment).map(ImmutableSet::of).orElse(ImmutableSet.of()),
                getHiveRecordCursorProvider(hdfsEnvironment).map(ImmutableSet::of).orElse(ImmutableSet.of()),
                new GenericHiveRecordCursorProvider(hdfsEnvironment, new HiveConfig()));

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
                false,
                TableToPartitionMapping.empty(),
                Optional.empty(),
                false,
                Optional.empty());

        ConnectorPageSource hivePageSource = factory.createPageSource(
                TestingConnectorTransactionHandle.INSTANCE,
                session, split,
                new HiveTableHandle("schema_name", "table_name", ImmutableMap.of(), ImmutableList.of(), Optional.empty()),
                readColumns,
                TupleDomain.all());

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
        checkState(!recordCursorWithProjections.get().getProjectedReaderColumns().isPresent(), "projection should not be required");
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
        Optional<ReaderPageSourceWithProjections> readerPageSourceWithProjections = pageSourceFactory
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
                        Optional.empty());

        checkState(readerPageSourceWithProjections.isPresent(), "readerPageSourceWithProjections is not present");
        checkState(!readerPageSourceWithProjections.get().getProjectedReaderColumns().isPresent(), "projection should not be required");
        return readerPageSourceWithProjections.get().getConnectorPageSource();
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
