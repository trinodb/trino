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
package io.trino.plugin.redshift;

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.parquet.writer.ParquetSchemaConverter;
import io.trino.parquet.writer.ParquetWriterOptions;
import io.trino.plugin.hive.parquet.ParquetFileWriter;
import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkId;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import org.apache.parquet.format.CompressionCodec;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class RedshiftBatchedInsertsCopyPageSink
        implements ConnectorPageSink
{
    private static final Logger log = Logger.get(RedshiftBatchedInsertsCopyPageSink.class);

    private static final int MAX_ROWS_PER_FILE = 100_000;

    private final TrinoFileSystem fileSystem;
    private final TypeOperators typeOperators;
    private final Location copyLocation;
    private final List<Type> columnTypes;
    private final String trinoVersion;
    private final ConnectorPageSinkId pageSinkId;
    private ParquetFileWriter parquetWriter;
    private long rowsInCurrentFile;
    private int filePartNumber;

    public RedshiftBatchedInsertsCopyPageSink(
            ConnectorSession session,
            ConnectorPageSinkId pageSinkId,
            TrinoFileSystemFactory fileSystemFactory,
            TypeOperators typeOperators,
            Location copyLocation,
            List<String> columnNames,
            List<Type> columnTypes,
            String trinoVersion)
    {
        this.pageSinkId = requireNonNull(pageSinkId, "pageSinkId is null");
        this.fileSystem = requireNonNull(fileSystemFactory, "fileSystemFactory is null").create(session);
        this.typeOperators = requireNonNull(typeOperators, "typeOperators is null");
        this.copyLocation = copyLocation;
        this.columnTypes = ImmutableList.copyOf(requireNonNull(columnTypes, "columnTypes is null"));
        this.trinoVersion = trinoVersion;
        checkArgument(columnNames.size() == columnTypes.size(), "columnNames and columnTypes must have the same size");

        startNewPart();
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        long positionCount = page.getPositionCount();
        if (positionCount == 0) {
            return NOT_BLOCKED;
        }

        parquetWriter.appendRows(page);
        rowsInCurrentFile += positionCount;

        if (rowsInCurrentFile >= MAX_ROWS_PER_FILE) {
            flushCurrentFile();
            startNewPart();
            rowsInCurrentFile = 0;
        }

        return NOT_BLOCKED;
    }

    private void startNewPart()
    {
        Location objectKey = copyLocation.appendPath(Long.toHexString(pageSinkId.getId())).appendPath(format("part-%d.parquet", filePartNumber++));
        this.parquetWriter = createParquetFileWriter(objectKey);
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        if (rowsInCurrentFile > 0) {
            flushCurrentFile();
        }

        return CompletableFuture.completedFuture(List.of());
    }

    @Override
    public void abort()
    {
        cleanupFiles();
    }

    public void cleanupFiles()
    {
        try {
            fileSystem.deleteDirectory(copyLocation);
        }
        catch (IOException e) {
            log.warn("Unable to cleanup location %s: %s", copyLocation, e.getMessage());
            // We don't want to rethrow here, as the query has already completed successfully
        }
    }

    private ParquetFileWriter createParquetFileWriter(Location path)
    {
        log.debug("Creating parquet file at location: %s", path.toString());
        ParquetWriterOptions parquetWriterOptions = ParquetWriterOptions.builder().build();
        CompressionCodec compressionCodec = CompressionCodec.SNAPPY;

        try {
            Closeable rollbackAction = this::abort;

            // According to Redshift docs, COPY inserts columns in the same order as the columns in the parquet. We
            // don't need the column names to match. We will instead create arbitrary column name to avoid any of the
            // parquet restrictions on column names.
            // See: https://docs.aws.amazon.com/redshift/latest/dg/copy-usage_notes-copy-from-columnar.html
            List<String> columnNames = new ArrayList<>();
            for (int i = 0; i < columnTypes.size(); i++) {
                columnNames.add(String.format("col%d", i));
            }

            ParquetSchemaConverter converter = new ParquetSchemaConverter(
                    columnTypes,
                    columnNames,
                    false,
                    false);

            List<Type> parquetTypes = columnTypes.stream()
                    .map(type -> RedshiftParquetTypes.toParquetType(typeOperators, type))
                    .collect(toImmutableList());

            // we use identity column mapping; input page already contains only data columns per
            // DataLagePageSink.getDataPage()
            int[] identityMapping = new int[columnTypes.size()];
            for (int i = 0; i < identityMapping.length; ++i) {
                identityMapping[i] = i;
            }

            return new ParquetFileWriter(
                    fileSystem.newOutputFile(path),
                    rollbackAction,
                    parquetTypes,
                    columnNames,
                    converter.getMessageType(),
                    converter.getPrimitiveTypes(),
                    parquetWriterOptions,
                    identityMapping,
                    compressionCodec,
                    trinoVersion,
                    Optional.empty(),
                    Optional.empty());
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void flushCurrentFile()
    {
        parquetWriter.commit();
    }

    public static final class RedshiftParquetTypes
    {
        public static Type toParquetType(TypeOperators typeOperators, Type type)
        {
            if (type instanceof TimestampWithTimeZoneType timestamp) {
                verify(timestamp.getPrecision() == 3, "Unsupported type: %s", type);
                return TIMESTAMP_MILLIS;
            }
            if (type instanceof ArrayType arrayType) {
                return new ArrayType(toParquetType(typeOperators, arrayType.getElementType()));
            }
            if (type instanceof MapType mapType) {
                return new MapType(toParquetType(typeOperators, mapType.getKeyType()), toParquetType(typeOperators, mapType.getValueType()), typeOperators);
            }
            if (type instanceof RowType rowType) {
                return RowType.from(rowType.getFields().stream()
                        .map(field -> RowType.field(field.getName().orElseThrow(), toParquetType(typeOperators, field.getType())))
                        .collect(toImmutableList()));
            }
            return type;
        }
    }
}
