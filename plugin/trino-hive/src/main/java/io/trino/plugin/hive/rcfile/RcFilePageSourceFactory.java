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
import com.google.inject.Inject;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.memory.MemoryInputFile;
import io.trino.hive.formats.FileCorruptionException;
import io.trino.hive.formats.encodings.ColumnEncodingFactory;
import io.trino.hive.formats.encodings.binary.BinaryColumnEncodingFactory;
import io.trino.hive.formats.encodings.text.TextColumnEncodingFactory;
import io.trino.hive.formats.encodings.text.TextEncodingOptions;
import io.trino.hive.formats.rcfile.RcFileReader;
import io.trino.plugin.hive.AcidInfo;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HiveConfig;
import io.trino.plugin.hive.HivePageSourceFactory;
import io.trino.plugin.hive.Schema;
import io.trino.plugin.hive.acid.AcidTransaction;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.EmptyPageSource;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import org.joda.time.DateTimeZone;

import java.io.InputStream;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.hive.formats.HiveClassNames.COLUMNAR_SERDE_CLASS;
import static io.trino.hive.formats.HiveClassNames.LAZY_BINARY_COLUMNAR_SERDE_CLASS;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_BAD_DATA;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_CANNOT_OPEN_SPLIT;
import static io.trino.plugin.hive.HivePageSourceProvider.projectColumnDereferences;
import static io.trino.plugin.hive.util.HiveUtil.splitError;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

public class RcFilePageSourceFactory
        implements HivePageSourceFactory
{
    private static final DataSize BUFFER_SIZE = DataSize.of(8, Unit.MEGABYTE);

    private final TrinoFileSystemFactory fileSystemFactory;
    private final DateTimeZone timeZone;

    @Inject
    public RcFilePageSourceFactory(TrinoFileSystemFactory fileSystemFactory, HiveConfig hiveConfig)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.timeZone = hiveConfig.getRcfileDateTimeZone();
    }

    public static boolean stripUnnecessaryProperties(String serializationLibraryName)
    {
        return LAZY_BINARY_COLUMNAR_SERDE_CLASS.equals(serializationLibraryName);
    }

    @Override
    public Optional<ConnectorPageSource> createPageSource(
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
        ColumnEncodingFactory columnEncodingFactory;
        String serializationLibraryName = schema.serializationLibraryName();
        if (serializationLibraryName.equals(LAZY_BINARY_COLUMNAR_SERDE_CLASS)) {
            columnEncodingFactory = new BinaryColumnEncodingFactory(timeZone);
        }
        else if (serializationLibraryName.equals(COLUMNAR_SERDE_CLASS)) {
            columnEncodingFactory = new TextColumnEncodingFactory(TextEncodingOptions.fromSchema(schema.serdeProperties()));
        }
        else {
            return Optional.empty();
        }

        checkArgument(acidInfo.isEmpty(), "Acid is not supported");

        return Optional.of(projectColumnDereferences(columns, baseColumns -> createPageSource(session, path, start, length, estimatedFileSize, baseColumns, columnEncodingFactory)));
    }

    private ConnectorPageSource createPageSource(
            ConnectorSession session,
            Location path,
            long start,
            long length,
            long estimatedFileSize,
            List<HiveColumnHandle> columns,
            ColumnEncodingFactory columnEncodingFactory)
    {
        TrinoFileSystem trinoFileSystem = fileSystemFactory.create(session);
        TrinoInputFile inputFile = trinoFileSystem.newInputFile(path);
        try {
            if (!inputFile.exists()) {
                throw new TrinoException(HIVE_CANNOT_OPEN_SPLIT, "File does not exist");
            }
            if (estimatedFileSize < BUFFER_SIZE.toBytes()) {
                try (InputStream inputStream = inputFile.newStream()) {
                    byte[] data = inputStream.readAllBytes();
                    inputFile = new MemoryInputFile(path, Slices.wrappedBuffer(data));
                }
            }
            length = min(inputFile.length() - start, length);
        }
        catch (TrinoException e) {
            throw e;
        }
        catch (Exception e) {
            throw new TrinoException(HIVE_CANNOT_OPEN_SPLIT, splitError(e, path, start, length), e);
        }

        // Split may be empty now that the correct file size is known
        if (length <= 0) {
            return new EmptyPageSource();
        }

        try {
            ImmutableMap.Builder<Integer, Type> readColumns = ImmutableMap.builder();
            for (HiveColumnHandle column : columns) {
                readColumns.put(column.getBaseHiveColumnIndex(), column.getType());
            }

            RcFileReader rcFileReader = new RcFileReader(
                    inputFile,
                    columnEncodingFactory,
                    readColumns.buildOrThrow(),
                    start,
                    length);

            return new RcFilePageSource(rcFileReader, columns);
        }
        catch (TrinoException e) {
            throw e;
        }
        catch (Throwable e) {
            String message = splitError(e, path, start, length);
            if (e instanceof FileCorruptionException) {
                throw new TrinoException(HIVE_BAD_DATA, message, e);
            }
            throw new TrinoException(HIVE_CANNOT_OPEN_SPLIT, message, e);
        }
    }
}
