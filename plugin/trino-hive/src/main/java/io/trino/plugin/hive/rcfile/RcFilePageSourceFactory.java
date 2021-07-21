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
package io.trino.plugin.hive.rcfile;

import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import io.trino.plugin.hive.AcidInfo;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HiveConfig;
import io.trino.plugin.hive.HivePageSourceFactory;
import io.trino.plugin.hive.HiveTimestampPrecision;
import io.trino.plugin.hive.ReaderColumns;
import io.trino.plugin.hive.ReaderPageSource;
import io.trino.plugin.hive.acid.AcidTransaction;
import io.trino.plugin.hive.util.FSDataInputStreamTail;
import io.trino.rcfile.AircompressorCodecFactory;
import io.trino.rcfile.HadoopCodecFactory;
import io.trino.rcfile.MemoryRcFileDataSource;
import io.trino.rcfile.RcFileCorruptionException;
import io.trino.rcfile.RcFileDataSource;
import io.trino.rcfile.RcFileDataSourceId;
import io.trino.rcfile.RcFileEncoding;
import io.trino.rcfile.RcFileReader;
import io.trino.rcfile.binary.BinaryRcFileEncoding;
import io.trino.rcfile.text.TextRcFileEncoding;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.EmptyPageSource;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.BlockMissingException;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe;
import org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe;
import org.joda.time.DateTimeZone;

import javax.inject.Inject;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_BAD_DATA;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_CANNOT_OPEN_SPLIT;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_MISSING_DATA;
import static io.trino.plugin.hive.HivePageSourceProvider.projectBaseColumns;
import static io.trino.plugin.hive.HiveSessionProperties.getTimestampPrecision;
import static io.trino.plugin.hive.ReaderPageSource.noProjectionAdaptation;
import static io.trino.plugin.hive.util.HiveUtil.getDeserializerClassName;
import static io.trino.rcfile.text.TextRcFileEncoding.DEFAULT_NULL_SEQUENCE;
import static io.trino.rcfile.text.TextRcFileEncoding.DEFAULT_SEPARATORS;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.hive.serde.serdeConstants.COLLECTION_DELIM;
import static org.apache.hadoop.hive.serde.serdeConstants.ESCAPE_CHAR;
import static org.apache.hadoop.hive.serde.serdeConstants.FIELD_DELIM;
import static org.apache.hadoop.hive.serde.serdeConstants.MAPKEY_DELIM;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_FORMAT;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_LAST_COLUMN_TAKES_REST;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_NULL_FORMAT;
import static org.apache.hadoop.hive.serde2.lazy.LazySerDeParameters.SERIALIZATION_EXTEND_NESTING_LEVELS;
import static org.apache.hadoop.hive.serde2.lazy.LazyUtils.getByte;

public class RcFilePageSourceFactory
        implements HivePageSourceFactory
{
    private static final int TEXT_LEGACY_NESTING_LEVELS = 8;
    private static final int TEXT_EXTENDED_NESTING_LEVELS = 29;
    private static final DataSize BUFFER_SIZE = DataSize.of(8, Unit.MEGABYTE);

    private final TypeManager typeManager;
    private final HdfsEnvironment hdfsEnvironment;
    private final FileFormatDataSourceStats stats;
    private final DateTimeZone timeZone;

    @Inject
    public RcFilePageSourceFactory(TypeManager typeManager, HdfsEnvironment hdfsEnvironment, FileFormatDataSourceStats stats, HiveConfig hiveConfig)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.stats = requireNonNull(stats, "stats is null");
        this.timeZone = requireNonNull(hiveConfig, "hiveConfig is null").getRcfileDateTimeZone();
    }

    @Override
    public Optional<ReaderPageSource> createPageSource(
            Configuration configuration,
            ConnectorSession session,
            Path path,
            long start,
            long length,
            long estimatedFileSize,
            Properties schema,
            List<HiveColumnHandle> columns,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            Optional<AcidInfo> acidInfo,
            OptionalInt bucketNumber,
            boolean originalFile,
            AcidTransaction transaction)
    {
        RcFileEncoding rcFileEncoding;
        String deserializerClassName = getDeserializerClassName(schema);
        if (deserializerClassName.equals(LazyBinaryColumnarSerDe.class.getName())) {
            rcFileEncoding = new BinaryRcFileEncoding(timeZone);
        }
        else if (deserializerClassName.equals(ColumnarSerDe.class.getName())) {
            rcFileEncoding = createTextVectorEncoding(schema);
        }
        else {
            return Optional.empty();
        }

        checkArgument(acidInfo.isEmpty(), "Acid is not supported");

        if (estimatedFileSize == 0) {
            throw new TrinoException(HIVE_BAD_DATA, "RCFile is empty: " + path);
        }

        List<HiveColumnHandle> projectedReaderColumns = columns;
        Optional<ReaderColumns> readerProjections = projectBaseColumns(columns);

        if (readerProjections.isPresent()) {
            projectedReaderColumns = readerProjections.get().get().stream()
                    .map(HiveColumnHandle.class::cast)
                    .collect(toImmutableList());
        }

        RcFileDataSource dataSource;
        try {
            FileSystem fileSystem = hdfsEnvironment.getFileSystem(session.getIdentity(), path, configuration);
            FSDataInputStream inputStream = hdfsEnvironment.doAs(session.getIdentity(), () -> fileSystem.open(path));
            if (estimatedFileSize < BUFFER_SIZE.toBytes()) {
                //  Handle potentially imprecise file lengths by reading the footer
                try {
                    FSDataInputStreamTail fileTail = FSDataInputStreamTail.readTail(path.toString(), estimatedFileSize, inputStream, toIntExact(BUFFER_SIZE.toBytes()));
                    dataSource = new MemoryRcFileDataSource(new RcFileDataSourceId(path.toString()), fileTail.getTailSlice());
                }
                finally {
                    inputStream.close();
                }
            }
            else {
                long fileSize = hdfsEnvironment.doAs(session.getIdentity(), () -> fileSystem.getFileStatus(path).getLen());
                dataSource = new HdfsRcFileDataSource(path.toString(), inputStream, fileSize, stats);
            }
        }
        catch (Exception e) {
            if (nullToEmpty(e.getMessage()).trim().equals("Filesystem closed") ||
                    e instanceof FileNotFoundException) {
                throw new TrinoException(HIVE_CANNOT_OPEN_SPLIT, e);
            }
            throw new TrinoException(HIVE_CANNOT_OPEN_SPLIT, splitError(e, path, start, length), e);
        }

        length = min(dataSource.getSize() - start, length);
        // Split may be empty now that the correct file size is known
        if (length <= 0) {
            return Optional.of(noProjectionAdaptation(new EmptyPageSource()));
        }

        try {
            ImmutableMap.Builder<Integer, Type> readColumns = ImmutableMap.builder();
            HiveTimestampPrecision timestampPrecision = getTimestampPrecision(session);
            for (HiveColumnHandle column : projectedReaderColumns) {
                readColumns.put(column.getBaseHiveColumnIndex(), column.getHiveType().getType(typeManager, timestampPrecision));
            }

            RcFileReader rcFileReader = new RcFileReader(
                    dataSource,
                    rcFileEncoding,
                    readColumns.build(),
                    new AircompressorCodecFactory(new HadoopCodecFactory(configuration.getClassLoader())),
                    start,
                    length,
                    BUFFER_SIZE);

            ConnectorPageSource pageSource = new RcFilePageSource(rcFileReader, projectedReaderColumns);
            return Optional.of(new ReaderPageSource(pageSource, readerProjections));
        }
        catch (Throwable e) {
            try {
                dataSource.close();
            }
            catch (IOException ignored) {
            }
            if (e instanceof TrinoException) {
                throw (TrinoException) e;
            }
            String message = splitError(e, path, start, length);
            if (e instanceof RcFileCorruptionException) {
                throw new TrinoException(HIVE_BAD_DATA, message, e);
            }
            if (e instanceof BlockMissingException) {
                throw new TrinoException(HIVE_MISSING_DATA, message, e);
            }
            throw new TrinoException(HIVE_CANNOT_OPEN_SPLIT, message, e);
        }
    }

    private static String splitError(Throwable t, Path path, long start, long length)
    {
        return format("Error opening Hive split %s (offset=%s, length=%s): %s", path, start, length, t.getMessage());
    }

    public static TextRcFileEncoding createTextVectorEncoding(Properties schema)
    {
        // separators
        int nestingLevels;
        if (!"true".equalsIgnoreCase(schema.getProperty(SERIALIZATION_EXTEND_NESTING_LEVELS))) {
            nestingLevels = TEXT_LEGACY_NESTING_LEVELS;
        }
        else {
            nestingLevels = TEXT_EXTENDED_NESTING_LEVELS;
        }
        byte[] separators = Arrays.copyOf(DEFAULT_SEPARATORS, nestingLevels);

        // the first three separators are set by old-old properties
        separators[0] = getByte(schema.getProperty(FIELD_DELIM, schema.getProperty(SERIALIZATION_FORMAT)), DEFAULT_SEPARATORS[0]);
        // for map field collection delimiter, Hive 1.x uses "colelction.delim" but Hive 3.x uses "collection.delim"
        // https://issues.apache.org/jira/browse/HIVE-16922
        separators[1] = getByte(schema.getProperty(COLLECTION_DELIM, schema.getProperty("colelction.delim")), DEFAULT_SEPARATORS[1]);
        separators[2] = getByte(schema.getProperty(MAPKEY_DELIM), DEFAULT_SEPARATORS[2]);

        // null sequence
        Slice nullSequence;
        String nullSequenceString = schema.getProperty(SERIALIZATION_NULL_FORMAT);
        if (nullSequenceString == null) {
            nullSequence = DEFAULT_NULL_SEQUENCE;
        }
        else {
            nullSequence = Slices.utf8Slice(nullSequenceString);
        }

        // last column takes rest
        String lastColumnTakesRestString = schema.getProperty(SERIALIZATION_LAST_COLUMN_TAKES_REST);
        boolean lastColumnTakesRest = "true".equalsIgnoreCase(lastColumnTakesRestString);

        // escaped
        String escapeProperty = schema.getProperty(ESCAPE_CHAR);
        Byte escapeByte = null;
        if (escapeProperty != null) {
            escapeByte = getByte(escapeProperty, (byte) '\\');
        }

        return new TextRcFileEncoding(
                nullSequence,
                separators,
                escapeByte,
                lastColumnTakesRest);
    }
}
