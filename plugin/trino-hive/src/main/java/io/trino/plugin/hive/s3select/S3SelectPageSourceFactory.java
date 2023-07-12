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
package io.trino.plugin.hive.s3select;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.s3.S3FileSystem;
import io.trino.filesystem.s3.S3FileSystemFactory;
import io.trino.hive.formats.compression.CompressionKind;
import io.trino.hive.formats.line.Column;
import io.trino.hive.formats.line.LineDeserializer;
import io.trino.hive.formats.line.LineDeserializerFactory;
import io.trino.hive.formats.line.LineReader;
import io.trino.hive.formats.line.json.JsonDeserializerFactory;
import io.trino.hive.formats.line.simple.SimpleDeserializerFactory;
import io.trino.hive.formats.line.text.TextLineReaderFactory;
import io.trino.plugin.hive.AcidInfo;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HiveConfig;
import io.trino.plugin.hive.HiveErrorCode;
import io.trino.plugin.hive.HivePageSourceFactory;
import io.trino.plugin.hive.ReaderColumns;
import io.trino.plugin.hive.ReaderPageSource;
import io.trino.plugin.hive.acid.AcidTransaction;
import io.trino.plugin.hive.line.LinePageSource;
import io.trino.plugin.hive.s3select.csv.S3SelectCsvRecordReader;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.TypeManager;
import software.amazon.awssdk.services.s3.model.CSVInput;
import software.amazon.awssdk.services.s3.model.CSVOutput;
import software.amazon.awssdk.services.s3.model.CompressionType;
import software.amazon.awssdk.services.s3.model.InputSerialization;
import software.amazon.awssdk.services.s3.model.JSONInput;
import software.amazon.awssdk.services.s3.model.JSONOutput;
import software.amazon.awssdk.services.s3.model.JSONType;
import software.amazon.awssdk.services.s3.model.OutputSerialization;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.hive.formats.line.LineDeserializer.EMPTY_LINE_DESERIALIZER;
import static io.trino.hive.thrift.metastore.hive_metastoreConstants.FILE_INPUT_FORMAT;
import static io.trino.plugin.hive.HivePageSourceProvider.projectBaseColumns;
import static io.trino.plugin.hive.HiveSessionProperties.isJsonNativeReaderEnabled;
import static io.trino.plugin.hive.HiveSessionProperties.isTextFileNativeReaderEnabled;
import static io.trino.plugin.hive.s3select.S3SelectUtils.hasFilters;
import static io.trino.plugin.hive.s3select.csv.S3SelectCsvRecordReader.COMMENTS_CHAR_STR;
import static io.trino.plugin.hive.s3select.csv.S3SelectCsvRecordReader.DEFAULT_FIELD_DELIMITER;
import static io.trino.plugin.hive.util.HiveUtil.getDeserializerClassName;
import static io.trino.plugin.hive.util.HiveUtil.getFooterCount;
import static io.trino.plugin.hive.util.HiveUtil.getHeaderCount;
import static io.trino.plugin.hive.util.SerdeConstants.ESCAPE_CHAR;
import static io.trino.plugin.hive.util.SerdeConstants.FIELD_DELIM;
import static io.trino.plugin.hive.util.SerdeConstants.LINE_DELIM;
import static io.trino.plugin.hive.util.SerdeConstants.QUOTE_CHAR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class S3SelectPageSourceFactory
        implements HivePageSourceFactory
{
    private final TextLineReaderFactory lineReaderFactory;
    private final TypeManager typeManager;
    private final S3FileSystemFactory s3FileSystemFactory;

    @Inject
    public S3SelectPageSourceFactory(
            HiveConfig hiveConfig,
            TypeManager typeManager,
            S3FileSystemFactory s3FileSystemFactory)
    {
        this.lineReaderFactory = new TextLineReaderFactory(1024, 1024, toIntExact(hiveConfig.getTextMaxLineLength().toBytes()));
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.s3FileSystemFactory = requireNonNull(s3FileSystemFactory, "s3FileSystemFactory is null");
    }

    @Override
    public Optional<ReaderPageSource> createPageSource(
            ConnectorSession session,
            Location path,
            long start,
            long length,
            long estimatedFileSize,
            Properties schema,
            List<HiveColumnHandle> columns,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            Optional<AcidInfo> acidInfo,
            OptionalInt bucketNumber,
            boolean originalFile,
            AcidTransaction transaction,
            boolean s3SelectPushdownEnabled)
    {
        if (!s3SelectPushdownEnabled || acidInfo.isPresent()) {
            return Optional.empty();
        }

        List<HiveColumnHandle> projectedReaderColumns = columns;
        Optional<ReaderColumns> readerProjections = projectBaseColumns(columns);
        if (readerProjections.isPresent()) {
            projectedReaderColumns = readerProjections.get().get().stream()
                    .map(HiveColumnHandle.class::cast)
                    .collect(toImmutableList());
        }

        // Ignore predicates on partial columns for now.
        effectivePredicate = effectivePredicate.filter((column, domain) -> column.isBaseColumn());

        // Query is not going to filter any data, no need to use S3 Select
        if (!hasFilters(schema, effectivePredicate, projectedReaderColumns)) {
            return Optional.empty();
        }

        String serdeName = getDeserializerClassName(schema);
        Optional<S3SelectDataType> s3SelectDataTypeOptional = S3SelectSerDeDataTypeMapper.getDataType(serdeName);

        if (s3SelectDataTypeOptional.isEmpty()) {
            return Optional.empty();
        }

        S3SelectDataType s3SelectDataType = s3SelectDataTypeOptional.get();
        CompressionType compressionType = CompressionKind.forFile(path.fileName())
                .map(S3SelectPageSourceFactory::compressionType)
                .orElse(CompressionType.NONE);

        LineDeserializerFactory lineDeserializerFactory;
        InputSerialization inputSerialization;
        OutputSerialization outputSerialization;
        boolean enableScanRange;
        switch (s3SelectDataType) {
            case CSV -> {
                // CSV is actually mapped to SimpleLazySerde in S3SelectSerDeDataTypeMapper
                lineDeserializerFactory = new SimpleDeserializerFactory();
                if (!isTextFileNativeReaderEnabled(session)) {
                    return Optional.empty();
                }

                String fieldDelimiter = schema.getProperty(FIELD_DELIM, DEFAULT_FIELD_DELIMITER);
                String quoteChar = schema.getProperty(QUOTE_CHAR, null);
                String escapeChar = schema.getProperty(ESCAPE_CHAR, null);
                String recordDelimiter = schema.getProperty(LINE_DELIM, "\n");

                CSVInput selectObjectCSVInputSerialization = CSVInput.builder()
                        .recordDelimiter(recordDelimiter)
                        .fieldDelimiter(fieldDelimiter)
                        .comments(COMMENTS_CHAR_STR)
                        .quoteCharacter(quoteChar)
                        .quoteEscapeCharacter(escapeChar)
                        .build();

                inputSerialization = InputSerialization.builder()
                        .compressionType(compressionType)
                        .csv(selectObjectCSVInputSerialization)
                        .build();

                CSVOutput selectObjectCSVOutputSerialization = CSVOutput.builder()
                        .recordDelimiter(recordDelimiter)
                        .fieldDelimiter(fieldDelimiter)
                        .quoteCharacter(quoteChar)
                        .quoteEscapeCharacter(escapeChar)
                        .build();
                outputSerialization = OutputSerialization.builder()
                        .csv(selectObjectCSVOutputSerialization)
                        .build();

                // Works for CSV if AllowQuotedRecordDelimiter is disabled.
                boolean isQuotedRecordDelimiterAllowed = Boolean.TRUE.equals(
                        inputSerialization.csv().allowQuotedRecordDelimiter());
                enableScanRange = CompressionType.NONE.equals(compressionType) && !isQuotedRecordDelimiterAllowed;
            }
            case JSON -> {
                lineDeserializerFactory = new JsonDeserializerFactory();
                if (!isJsonNativeReaderEnabled(session)) {
                    return Optional.empty();
                }

                // JSONType.LINES is the only JSON format supported by the Hive JsonSerDe.
                JSONInput selectObjectJSONInputSerialization = JSONInput.builder()
                        .type(JSONType.LINES)
                        .build();

                inputSerialization = InputSerialization.builder()
                        .compressionType(compressionType)
                        .json(selectObjectJSONInputSerialization)
                        .build();

                JSONOutput selectObjectJSONOutputSerialization = JSONOutput.builder().build();
                outputSerialization = OutputSerialization.builder()
                        .json(selectObjectJSONOutputSerialization)
                        .build();

                enableScanRange = CompressionType.NONE.equals(compressionType);
            }
            default -> throw new IllegalStateException("Unknown s3 select data type: " + s3SelectDataType);
        }

        if (!lineReaderFactory.getHiveOutputFormatClassName().equals(schema.getProperty(FILE_INPUT_FORMAT)) ||
                !lineDeserializerFactory.getHiveSerDeClassNames().contains(getDeserializerClassName(schema))) {
            return Optional.empty();
        }

        // get header and footer count
        int headerCount = getHeaderCount(schema);
        if (headerCount > 1) {
            checkArgument(start == 0, "Multiple header rows are not supported for a split file");
        }
        int footerCount = getFooterCount(schema);
        if (footerCount > 0) {
            checkArgument(start == 0, "Footer not supported for a split file");
        }

        // create deserializer
        LineDeserializer lineDeserializer = EMPTY_LINE_DESERIALIZER;
        if (!columns.isEmpty()) {
            ImmutableList.Builder<Column> deserializerColumns = ImmutableList.builder();
            for (int columnIndex = 0; columnIndex < projectedReaderColumns.size(); columnIndex++) {
                HiveColumnHandle column = projectedReaderColumns.get(columnIndex);
                deserializerColumns.add(new Column(column.getName(), column.getType(), columnIndex));
            }
            lineDeserializer = lineDeserializerFactory.create(deserializerColumns.build(), Maps.fromProperties(schema));
        }

        Optional<String> nullCharacterEncoding = Optional.empty();
        if (s3SelectDataType == S3SelectDataType.CSV) {
            nullCharacterEncoding = S3SelectCsvRecordReader.nullCharacterEncoding(schema);
        }
        IonSqlQueryBuilder queryBuilder = new IonSqlQueryBuilder(typeManager, s3SelectDataType, nullCharacterEncoding);
        String ionSqlQuery = queryBuilder.buildSql(projectedReaderColumns, effectivePredicate);

        S3FileSystem fileSystem = (S3FileSystem) s3FileSystemFactory.create(session);
        TrinoInputFile inputFile = fileSystem.newS3SelectInputFile(
                path,
                ionSqlQuery,
                enableScanRange,
                inputSerialization,
                outputSerialization);
        try {
            LineReader lineReader = lineReaderFactory.createLineReader(inputFile, start, length, headerCount, footerCount, Optional.empty());
            LinePageSource pageSource = new LinePageSource(lineReader, lineDeserializer, lineReaderFactory.createLineBuffer(), path);
            return Optional.of(new ReaderPageSource(pageSource, readerProjections));
        }
        catch (IOException e) {
            throw new TrinoException(HiveErrorCode.HIVE_FILESYSTEM_ERROR, "Failed to open s3 select reader", e);
        }
    }

    private static CompressionType compressionType(CompressionKind compressionKind)
    {
        return switch (compressionKind) {
            case GZIP -> CompressionType.GZIP;
            case BZIP2 -> CompressionType.BZIP2;
            default -> throw new TrinoException(NOT_SUPPORTED, "Compression extension not supported for S3 Select: " + compressionKind);
        };
    }
}
