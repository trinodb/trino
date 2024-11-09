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
package io.trino.plugin.hive.line;

import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.memory.MemoryInputFile;
import io.trino.hive.formats.line.Column;
import io.trino.hive.formats.line.LineDeserializer;
import io.trino.hive.formats.line.LineDeserializerFactory;
import io.trino.hive.formats.line.LineReader;
import io.trino.hive.formats.line.LineReaderFactory;
import io.trino.plugin.hive.AcidInfo;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HivePageSourceFactory;
import io.trino.plugin.hive.ReaderPageSource;
import io.trino.plugin.hive.Schema;
import io.trino.plugin.hive.acid.AcidTransaction;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.EmptyPageSource;
import io.trino.spi.predicate.TupleDomain;

import java.io.InputStream;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.hive.formats.line.LineDeserializer.EMPTY_LINE_DESERIALIZER;
import static io.trino.hive.thrift.metastore.hive_metastoreConstants.FILE_INPUT_FORMAT;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_CANNOT_OPEN_SPLIT;
import static io.trino.plugin.hive.HivePageSourceProvider.projectColumnDereferences;
import static io.trino.plugin.hive.util.HiveUtil.getFooterCount;
import static io.trino.plugin.hive.util.HiveUtil.getHeaderCount;
import static io.trino.plugin.hive.util.HiveUtil.splitError;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

public abstract class LinePageSourceFactory
        implements HivePageSourceFactory
{
    private static final DataSize SMALL_FILE_SIZE = DataSize.of(8, Unit.MEGABYTE);

    private final TrinoFileSystemFactory fileSystemFactory;
    private final LineDeserializerFactory lineDeserializerFactory;
    private final LineReaderFactory lineReaderFactory;

    protected LinePageSourceFactory(
            TrinoFileSystemFactory fileSystemFactory,
            LineDeserializerFactory lineDeserializerFactory,
            LineReaderFactory lineReaderFactory)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.lineDeserializerFactory = requireNonNull(lineDeserializerFactory, "lineDeserializerFactory is null");
        this.lineReaderFactory = requireNonNull(lineReaderFactory, "lineReaderFactory is null");
    }

    @Override
    public Optional<ReaderPageSource> createPageSource(
            ConnectorSession session,
            Location path,
            long start,
            long length,
            long estimatedFileSize,
            long fileModifiedTime,
            Schema schema,
            List<HiveColumnHandle> columns,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            Optional<AcidInfo> acidInfo,
            OptionalInt bucketNumber,
            boolean originalFile,
            AcidTransaction transaction)
    {
        if (!lineReaderFactory.getHiveInputFormatClassNames().contains(schema.serdeProperties().get(FILE_INPUT_FORMAT)) ||
                !lineDeserializerFactory.getHiveSerDeClassNames().contains(schema.serializationLibraryName())) {
            return Optional.empty();
        }

        checkArgument(acidInfo.isEmpty(), "Acid is not supported");

        ConnectorPageSource pageSource = projectColumnDereferences(columns, baseColumns -> createPageSource(session, path, start, length, estimatedFileSize, schema, baseColumns));
        return Optional.of(new ReaderPageSource(pageSource, Optional.empty()));
    }

    private ConnectorPageSource createPageSource(
            ConnectorSession session,
            Location path,
            long start,
            long length,
            long estimatedFileSize,
            Schema schema,
            List<HiveColumnHandle> columns)
    {
        // get header and footer counts
        int headerCount = getHeaderCount(schema.serdeProperties());
        if (headerCount > 1) {
            checkArgument(start == 0, "Multiple header rows are not supported for a split file");
        }
        int footerCount = getFooterCount(schema.serdeProperties());
        if (footerCount > 0) {
            checkArgument(start == 0, "Footer not supported for a split file");
        }

        // create deserializer
        LineDeserializer lineDeserializer = EMPTY_LINE_DESERIALIZER;
        if (!columns.isEmpty()) {
            lineDeserializer = lineDeserializerFactory.create(
                    columns.stream()
                            .map(column -> new Column(column.getName(), column.getType(), column.getBaseHiveColumnIndex()))
                            .collect(toImmutableList()),
                    schema.serdeProperties());
        }

        TrinoFileSystem trinoFileSystem = fileSystemFactory.create(session);
        TrinoInputFile inputFile = trinoFileSystem.newInputFile(path);
        try {
            // buffer file if small
            if (estimatedFileSize < SMALL_FILE_SIZE.toBytes()) {
                try (InputStream inputStream = inputFile.newStream()) {
                    byte[] data = inputStream.readAllBytes();
                    inputFile = new MemoryInputFile(path, Slices.wrappedBuffer(data));
                }
            }
            length = min(inputFile.length() - start, length);

            // Skip empty inputs
            if (length <= 0) {
                return new EmptyPageSource();
            }

            LineReader lineReader = lineReaderFactory.createLineReader(inputFile, start, length, headerCount, footerCount);
            // Split may be empty after discovering the real file size and skipping headers
            if (lineReader.isClosed()) {
                return new EmptyPageSource();
            }
            return new LinePageSource(lineReader, lineDeserializer, lineReaderFactory.createLineBuffer(), path);
        }
        catch (TrinoException e) {
            throw e;
        }
        catch (Exception e) {
            throw new TrinoException(HIVE_CANNOT_OPEN_SPLIT, splitError(e, path, start, length), e);
        }
    }
}
