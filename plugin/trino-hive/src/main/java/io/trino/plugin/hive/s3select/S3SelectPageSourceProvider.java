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

import com.amazonaws.services.s3.model.CSVInput;
import com.amazonaws.services.s3.model.CSVOutput;
import com.amazonaws.services.s3.model.CompressionType;
import com.amazonaws.services.s3.model.InputSerialization;
import com.amazonaws.services.s3.model.JSONInput;
import com.amazonaws.services.s3.model.JSONOutput;
import com.amazonaws.services.s3.model.JSONType;
import com.amazonaws.services.s3.model.OutputSerialization;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoInputFile;
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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.GzipCodec;

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

public class S3SelectPageSourceProvider
        implements HivePageSourceFactory
{
    private final TextLineReaderFactory lineReaderFactory;
    private final TypeManager typeManager;
    private final TrinoS3ClientFactory s3ClientFactory;

    @Inject
    public S3SelectPageSourceProvider(
            HiveConfig hiveConfig,
            TypeManager typeManager,
            TrinoS3ClientFactory s3ClientFactory)
    {
        this.lineReaderFactory = new TextLineReaderFactory(1024, 1024, toIntExact(hiveConfig.getTextMaxLineLength().toBytes()));
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.s3ClientFactory = requireNonNull(s3ClientFactory, "s3ClientFactory is null");
    }

    @Override
    public Optional<ReaderPageSource> createPageSource(
            Configuration configuration,
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
        CompressionType compressionType = compressionType(new CompressionCodecFactory(configuration), path);

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

                CSVInput selectObjectCSVInputSerialization = new CSVInput();
                selectObjectCSVInputSerialization.setRecordDelimiter(recordDelimiter);
                selectObjectCSVInputSerialization.setFieldDelimiter(fieldDelimiter);
                selectObjectCSVInputSerialization.setComments(COMMENTS_CHAR_STR);
                selectObjectCSVInputSerialization.setQuoteCharacter(quoteChar);
                selectObjectCSVInputSerialization.setQuoteEscapeCharacter(escapeChar);

                inputSerialization = new InputSerialization();
                inputSerialization.setCompressionType(compressionType);
                inputSerialization.setCsv(selectObjectCSVInputSerialization);

                outputSerialization = new OutputSerialization();
                CSVOutput selectObjectCSVOutputSerialization = new CSVOutput();
                selectObjectCSVOutputSerialization.setRecordDelimiter(recordDelimiter);
                selectObjectCSVOutputSerialization.setFieldDelimiter(fieldDelimiter);
                selectObjectCSVOutputSerialization.setQuoteCharacter(quoteChar);
                selectObjectCSVOutputSerialization.setQuoteEscapeCharacter(escapeChar);
                outputSerialization.setCsv(selectObjectCSVOutputSerialization);

                // Works for CSV if AllowQuotedRecordDelimiter is disabled.
                boolean isQuotedRecordDelimiterAllowed = Boolean.TRUE.equals(
                        inputSerialization.getCsv().getAllowQuotedRecordDelimiter());
                enableScanRange = CompressionType.NONE.equals(compressionType) && !isQuotedRecordDelimiterAllowed;
            }
            case JSON -> {
                lineDeserializerFactory = new JsonDeserializerFactory();
                if (!isJsonNativeReaderEnabled(session)) {
                    return Optional.empty();
                }

                // JSONType.LINES is the only JSON format supported by the Hive JsonSerDe.
                JSONInput selectObjectJSONInputSerialization = new JSONInput();
                selectObjectJSONInputSerialization.setType(JSONType.LINES);

                inputSerialization = new InputSerialization();
                inputSerialization.setCompressionType(compressionType);
                inputSerialization.setJson(selectObjectJSONInputSerialization);

                outputSerialization = new OutputSerialization();
                JSONOutput selectObjectJSONOutputSerialization = new JSONOutput();
                outputSerialization.setJson(selectObjectJSONOutputSerialization);

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

        TrinoInputFile inputFile = new S3SelectInputFile(
                s3ClientFactory.getS3Client(configuration),
                new TrinoS3SelectClient(configuration, s3ClientFactory),
                ionSqlQuery,
                path,
                start,
                length,
                inputSerialization,
                outputSerialization,
                enableScanRange);

        try {
            LineReader lineReader = lineReaderFactory.createLineReader(inputFile, start, length, headerCount, footerCount, Optional.empty());
            LinePageSource pageSource = new LinePageSource(lineReader, lineDeserializer, lineReaderFactory.createLineBuffer(), path);
            return Optional.of(new ReaderPageSource(pageSource, readerProjections));
        }
        catch (IOException e) {
            throw new TrinoException(HiveErrorCode.HIVE_FILESYSTEM_ERROR, "Failed to open s3 select reader", e);
        }
    }

    private static CompressionType compressionType(CompressionCodecFactory compressionCodecFactory, Location location)
    {
        CompressionCodec codec = compressionCodecFactory.getCodec(new Path(location.toString()));
        if (codec == null) {
            return CompressionType.NONE;
        }
        if (codec instanceof GzipCodec) {
            return CompressionType.GZIP;
        }
        if (codec instanceof BZip2Codec) {
            return CompressionType.BZIP2;
        }
        throw new TrinoException(NOT_SUPPORTED, "Compression extension not supported for S3 Select: " + location);
    }
}
