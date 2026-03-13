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
package io.trino.plugin.ducklake;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInputFile;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.parquet.Column;
import io.trino.parquet.Field;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.ParquetDataSourceId;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.metadata.FileMetadata;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.parquet.reader.MetadataReader;
import io.trino.parquet.reader.ParquetReader;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.hive.parquet.ParquetPageSource;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.EmptyPageSource;
import io.trino.spi.type.TypeManager;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.parquet.ParquetTypeUtils.getColumnIO;
import static io.trino.plugin.hive.parquet.ParquetPageSourceFactory.createDataSource;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;
import static org.joda.time.DateTimeZone.UTC;

/**
 * PageSourceProvider for Ducklake connector.
 * Leverages Trino's ParquetPageSource for all Parquet reading logic.
 */
public class DucklakePageSourceProvider
        implements ConnectorPageSourceProvider
{
    private static final Logger log = Logger.get(DucklakePageSourceProvider.class);

    private final TrinoFileSystemFactory fileSystemFactory;
    private final FileFormatDataSourceStats fileFormatDataSourceStats;
    private final ParquetReaderOptions parquetReaderOptions;
    private final TypeManager typeManager;

    @Inject
    public DucklakePageSourceProvider(
            TrinoFileSystemFactory fileSystemFactory,
            FileFormatDataSourceStats fileFormatDataSourceStats,
            ParquetReaderOptions parquetReaderOptions,
            TypeManager typeManager)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.fileFormatDataSourceStats = requireNonNull(fileFormatDataSourceStats, "fileFormatDataSourceStats is null");
        this.parquetReaderOptions = requireNonNull(parquetReaderOptions, "parquetReaderOptions is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter)
    {
        requireNonNull(split, "split is null");
        requireNonNull(columns, "columns is null");

        DucklakeSplit ducklakeSplit = (DucklakeSplit) split;

        // Extract column information
        List<DucklakeColumnHandle> ducklakeColumns = columns.stream()
                .map(DucklakeColumnHandle.class::cast)
                .collect(toImmutableList());

        // If no columns requested, return empty page source
        if (ducklakeColumns.isEmpty()) {
            return new EmptyPageSource();
        }

        log.debug("Creating page source for file: %s", ducklakeSplit.dataFilePath());

        try {
            // Get file system for the session
            TrinoFileSystem fileSystem = fileSystemFactory.create(session);

            // Open the data file
            Location dataFileLocation = Location.of(ducklakeSplit.dataFilePath());
            TrinoInputFile inputFile = fileSystem.newInputFile(dataFileLocation);

            // Verify file format
            if (!"parquet".equalsIgnoreCase(ducklakeSplit.fileFormat())) {
                throw new IllegalArgumentException("Unsupported file format: " + ducklakeSplit.fileFormat());
            }

            // Create Parquet page source using Trino's infrastructure
            return createParquetPageSource(
                    session,
                    inputFile,
                    ducklakeColumns,
                    ducklakeSplit);
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to create page source for file: " + ducklakeSplit.dataFilePath(), e);
        }
    }

    private ConnectorPageSource createParquetPageSource(
            ConnectorSession session,
            TrinoInputFile inputFile,
            List<DucklakeColumnHandle> columns,
            DucklakeSplit split)
            throws IOException
    {
        // Create memory context for reading
        AggregatedMemoryContext memoryContext = newSimpleAggregatedMemoryContext();

        ParquetDataSource dataSource = null;
        try {
            // Create Parquet data source
            dataSource = createDataSource(
                    inputFile,
                    OptionalLong.of(split.fileSizeBytes()),
                    parquetReaderOptions,
                    memoryContext,
                    fileFormatDataSourceStats);

            // Read Parquet metadata
            ParquetMetadata parquetMetadata = MetadataReader.readFooter(
                    dataSource,
                    parquetReaderOptions.getMaxFooterReadSize(),
                    Optional.empty());
            FileMetadata fileMetadata = parquetMetadata.getFileMetaData();
            MessageType fileSchema = fileMetadata.getSchema();

            // Build list of columns to read
            ImmutableList.Builder<Column> parquetColumns = ImmutableList.builder();
            MessageColumnIO messageColumnIO = getColumnIO(fileSchema, fileSchema);

            for (DucklakeColumnHandle column : columns) {
                String columnName = column.columnName();
                org.apache.parquet.io.ColumnIO columnIO = messageColumnIO.getChild(columnName);

                if (columnIO == null) {
                    throw new TrinoException(
                            NOT_SUPPORTED,
                            "Column not found in Parquet file: " + columnName);
                }

                Field field = DucklakeParquetTypeUtils.constructField(
                        column.columnType(),
                        columnIO);

                parquetColumns.add(new Column(columnName, field));
            }

            // Create ParquetReader
            ParquetDataSourceId dataSourceId = dataSource.getId();
            ParquetReader parquetReader = new ParquetReader(
                    Optional.ofNullable(fileMetadata.getCreatedBy()),
                    parquetColumns.build(),
                    false, // appendRowNumberColumn
                    ImmutableList.of(), // rowGroups - empty means all
                    dataSource,
                    UTC,
                    memoryContext,
                    parquetReaderOptions,
                    exception -> handleParquetException(dataSourceId, exception),
                    Optional.empty(), // fileRowCount
                    Optional.empty(), // bloomFilterStore
                    Optional.empty()); // rowFilter

            // Wrap in ParquetPageSource
            ConnectorPageSource pageSource = new ParquetPageSource(parquetReader);

            // TODO: Handle delete files (Phase 3)
            // if (split.deleteFilePath().isPresent()) {
            //     return applyDeleteFiles(pageSource, split);
            // }

            log.debug("Created Parquet page source for %d columns from file: %s",
                    columns.size(), split.dataFilePath());

            return pageSource;
        }
        catch (IOException | RuntimeException e) {
            if (dataSource != null) {
                try {
                    dataSource.close();
                }
                catch (IOException ex) {
                    if (!e.equals(ex)) {
                        e.addSuppressed(ex);
                    }
                }
            }
            throw new RuntimeException("Failed to create Parquet page source for file: " + split.dataFilePath(), e);
        }
    }

    private static RuntimeException handleParquetException(ParquetDataSourceId dataSourceId, Exception exception)
    {
        if (exception instanceof TrinoException) {
            return (TrinoException) exception;
        }
        return new TrinoException(
                NOT_SUPPORTED,
                "Error reading Parquet file: " + dataSourceId,
                exception);
    }

    // TODO: Phase 3 - Delete file handling
    // private ConnectorPageSource applyDeleteFiles(
    //         ConnectorPageSource dataSource,
    //         DucklakeSplit split)
    // {
    //     // Load delete file
    //     // Apply position deletes using DeleteManager
    //     // Return filtered page source
    //     return dataSource; // Placeholder
    // }
}
