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
package io.trino.plugin.iceberg;

import io.trino.Session;
import io.trino.filesystem.FileEntry;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInputFile;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.HiveMetastoreFactory;
import io.trino.metastore.cache.CachingHiveMetastore;
import io.trino.orc.OrcColumn;
import io.trino.orc.OrcDataSource;
import io.trino.orc.OrcPredicate;
import io.trino.orc.OrcReader;
import io.trino.orc.OrcReaderOptions;
import io.trino.orc.OrcRecordReader;
import io.trino.orc.metadata.OrcType;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.ParquetTestUtils;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.parquet.reader.MetadataReader;
import io.trino.parquet.reader.ParquetReader;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.hive.TrinoViewHiveMetastore;
import io.trino.plugin.hive.orc.OrcReaderConfig;
import io.trino.plugin.hive.orc.OrcWriterConfig;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.hive.parquet.ParquetWriterConfig;
import io.trino.plugin.hive.parquet.TrinoParquetDataSource;
import io.trino.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.catalog.file.FileMetastoreTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.hms.TrinoHiveCatalog;
import io.trino.plugin.iceberg.encryption.DefaultEncryptionManagerFactory;
import io.trino.plugin.iceberg.encryption.EncryptionManagerFactory;
import io.trino.plugin.iceberg.encryption.IcebergEncryptionConfig;
import io.trino.plugin.iceberg.encryption.PlaintextEncryptionManagerFactory;
import io.trino.plugin.iceberg.fileio.ForwardingFileIoFactory;
import io.trino.plugin.iceberg.fileio.ForwardingInputFile;
import io.trino.spi.block.Block;
import io.trino.spi.block.RowBlock;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorSession;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.parquet.io.ColumnIO;
import org.apache.parquet.io.GroupColumnIO;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.metastore.cache.CachingHiveMetastore.createPerTransactionCache;
import static io.trino.orc.OrcReader.INITIAL_BATCH_SIZE;
import static io.trino.parquet.ParquetTypeUtils.getColumnIO;
import static io.trino.parquet.ParquetTypeUtils.lookupColumnByName;
import static io.trino.plugin.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static io.trino.plugin.iceberg.IcebergUtil.loadIcebergTable;
import static io.trino.plugin.iceberg.util.FileOperationUtils.FileType.METADATA_JSON;
import static io.trino.plugin.iceberg.util.FileOperationUtils.FileType.fromFilePath;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.joda.time.DateTimeZone.UTC;

public final class IcebergTestUtils
{
    public static final ConnectorSession SESSION = TestingConnectorSession.builder()
            .setPropertyMetadata(new IcebergSessionProperties(
                    new IcebergConfig(),
                    new IcebergEncryptionConfig(),
                    new OrcReaderConfig(),
                    new OrcWriterConfig(),
                    new ParquetReaderConfig(),
                    new ParquetWriterConfig()).getSessionProperties())
            .build();

    public static final TableStatisticsReader TABLE_STATISTICS_READER = new TableStatisticsReader(
            TESTING_TYPE_MANAGER,
            newDirectExecutorService());

    public static final ForwardingFileIoFactory FILE_IO_FACTORY = new ForwardingFileIoFactory(newDirectExecutorService());

    public static final EncryptionManagerFactory ENCRYPTION_MANAGER_FACTORY = new PlaintextEncryptionManagerFactory();

    private IcebergTestUtils() {}

    public static Session withSmallRowGroups(Session session)
    {
        return Session.builder(session)
                .setCatalogSessionProperty("iceberg", "orc_writer_max_stripe_rows", "20")
                .setCatalogSessionProperty("iceberg", "parquet_writer_batch_size", "20")
                .build();
    }

    public static boolean checkOrcFileSorting(TrinoFileSystem fileSystem, Location path, String sortColumnName)
    {
        return checkOrcFileSorting(() -> {
            try {
                return new TrinoOrcDataSource(fileSystem.newInputFile(path), new OrcReaderOptions(), new FileFormatDataSourceStats());
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }, sortColumnName);
    }

    private static boolean checkOrcFileSorting(Supplier<OrcDataSource> dataSourceSupplier, String sortColumnName)
    {
        OrcReaderOptions readerOptions = new OrcReaderOptions();
        try (OrcDataSource dataSource = dataSourceSupplier.get()) {
            OrcReader orcReader = OrcReader.createOrcReader(dataSource, readerOptions).orElseThrow();
            String[] pathParts = sortColumnName.split("\\.");

            // Read only the top-level column; nulls on intermediate struct columns are tracked there,
            // not in the leaf column's present stream
            OrcColumn topColumn = orcReader.getRootColumn().getNestedColumns().stream()
                    .filter(column -> column.getColumnName().equals(pathParts[0]))
                    .collect(onlyElement());

            // Walk to the leaf to determine its type, but read from topColumn
            OrcColumn leafColumn = topColumn;
            for (int i = 1; i < pathParts.length; i++) {
                String part = pathParts[i];
                leafColumn = leafColumn.getNestedColumns().stream()
                        .filter(column -> column.getColumnName().equals(part))
                        .collect(onlyElement());
            }
            Type leafType = getType(leafColumn.getColumnType().getOrcTypeKind());
            Type columnType = buildColumnType(pathParts, leafType);

            try (OrcRecordReader recordReader = orcReader.createRecordReader(
                    List.of(topColumn),
                    List.of(columnType),
                    false,
                    OrcPredicate.TRUE,
                    UTC,
                    newSimpleAggregatedMemoryContext(),
                    INITIAL_BATCH_SIZE,
                    RuntimeException::new)) {
                return checkSortOrder(recordReader::nextPage, pathParts.length, leafType);
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static Type getType(OrcType.OrcTypeKind orcTypeKind)
    {
        return switch (orcTypeKind) {
            case OrcType.OrcTypeKind.STRING, OrcType.OrcTypeKind.VARCHAR -> VARCHAR;
            default -> throw new IllegalArgumentException("Unsupported orc type: " + orcTypeKind);
        };
    }

    public static boolean checkParquetFileSorting(TrinoInputFile inputFile, String sortColumnName)
    {
        try (TrinoParquetDataSource dataSource = new TrinoParquetDataSource(inputFile, ParquetReaderOptions.defaultOptions(), new FileFormatDataSourceStats())) {
            ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, Optional.empty());
            MessageType fileSchema = parquetMetadata.getFileMetaData().getSchema();
            MessageColumnIO messageColumnIO = getColumnIO(fileSchema, fileSchema);

            String[] pathParts = sortColumnName.split("\\.");
            ColumnIO columnIO = messageColumnIO;
            for (String part : pathParts) {
                columnIO = lookupColumnByName((GroupColumnIO) columnIO, part);
                checkState(columnIO != null, "Column not found in Parquet schema: %s (while resolving %s)", part, sortColumnName);
            }
            Type leafType = getType(columnIO.getType().asPrimitiveType().getPrimitiveTypeName());
            // e.g. "row_t.name" → RowType(field("name", VARCHAR)), read column "row_t"
            Type columnType = buildColumnType(pathParts, leafType);
            String topLevelColumnName = pathParts[0];

            try (ParquetReader parquetReader = ParquetTestUtils.createParquetReader(
                    dataSource,
                    parquetMetadata,
                    List.of(columnType),
                    List.of(topLevelColumnName))) {
                return checkSortOrder(parquetReader::nextPage, pathParts.length, leafType);
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static Type getType(PrimitiveType.PrimitiveTypeName primitiveTypeName)
    {
        return switch (primitiveTypeName) {
            case BINARY -> VARCHAR;
            default -> throw new IllegalArgumentException("Unsupported parquet primitive type: " + primitiveTypeName);
        };
    }

    // Builds a nested RowType wrapping leafType from the inside out.
    // e.g. pathParts=["row_t","name"], leafType=VARCHAR → RowType(field("name", VARCHAR))
    private static Type buildColumnType(String[] pathParts, Type leafType)
    {
        Type columnType = leafType;
        for (int i = pathParts.length - 2; i >= 0; i--) {
            columnType = RowType.rowType(RowType.field(pathParts[i + 1], columnType));
        }
        return columnType;
    }

    // Checks NULLS FIRST ascending order across all pages from the supplier.
    // Returns false if any non-null value is followed by a smaller value, or if a null follows a non-null.
    @SuppressWarnings("unchecked")
    private static boolean checkSortOrder(PageSupplier pageSupplier, int pathDepth, Type leafType)
            throws IOException
    {
        Comparable<Object> previousMax = null;
        boolean seenNonNull = false;
        for (SourcePage page = pageSupplier.nextPage(); page != null; page = pageSupplier.nextPage()) {
            Block topBlock = page.getBlock(0);
            for (int position = 0; position < topBlock.getPositionCount(); position++) {
                Comparable<Object> current = drillToLeaf(topBlock, position, pathDepth, leafType);
                if (current == null) {
                    if (seenNonNull) {
                        return false;
                    }
                    continue;
                }
                seenNonNull = true;
                if (previousMax != null && previousMax.compareTo(current) > 0) {
                    return false;
                }
                previousMax = current;
            }
        }
        return true;
    }

    @FunctionalInterface
    private interface PageSupplier
    {
        SourcePage nextPage() throws IOException;
    }

    // Drills into nested RowBlocks per position, returning null if any level is null.
    // Each intermediate RowType is a single-field wrapper, so the field index is always 0 at each level.
    @SuppressWarnings("unchecked")
    private static Comparable<Object> drillToLeaf(Block block, int position, int pathDepth, Type leafType)
    {
        for (int depth = 1; depth < pathDepth; depth++) {
            if (block.isNull(position)) {
                return null;
            }
            block = RowBlock.getRowFieldsFromBlock(block).get(0);
        }
        if (block.isNull(position)) {
            return null;
        }
        return (Comparable<Object>) readNativeValue(leafType, block, position);
    }

    public static <T> T getConnectorService(QueryRunner queryRunner, Class<T> clazz)
    {
        Connector connector = queryRunner.getCoordinator().getConnector(ICEBERG_CATALOG);
        return ((IcebergConnector) connector).getInjector().getInstance(clazz);
    }

    public static TrinoFileSystemFactory getFileSystemFactory(QueryRunner queryRunner)
    {
        return getConnectorService(queryRunner, TrinoFileSystemFactory.class);
    }

    public static HiveMetastore getHiveMetastore(QueryRunner queryRunner)
    {
        return getConnectorService(queryRunner, HiveMetastoreFactory.class)
                .createMetastore(Optional.empty());
    }

    public static BaseTable loadTable(
            String tableName,
            HiveMetastore metastore,
            TrinoFileSystemFactory fileSystemFactory,
            String catalogName,
            String schemaName)
    {
        IcebergTableOperationsProvider tableOperationsProvider = new FileMetastoreTableOperationsProvider(fileSystemFactory, FILE_IO_FACTORY, new DefaultEncryptionManagerFactory(Optional.of(new TestingFileMetastoreKeyManagementClient())));
        TrinoCatalog catalog = getTrinoCatalog(metastore, fileSystemFactory, catalogName);
        return loadIcebergTable(catalog, tableOperationsProvider, SESSION, new SchemaTableName(schemaName, tableName));
    }

    public static TrinoCatalog getTrinoCatalog(
            HiveMetastore metastore,
            TrinoFileSystemFactory fileSystemFactory,
            String catalogName)
    {
        return getTrinoCatalog(metastore, fileSystemFactory, catalogName, new DefaultEncryptionManagerFactory(Optional.of(new TestingFileMetastoreKeyManagementClient())));
    }

    public static TrinoCatalog getTrinoCatalog(
            HiveMetastore metastore,
            TrinoFileSystemFactory fileSystemFactory,
            String catalogName,
            EncryptionManagerFactory encryptionManagerFactory)
    {
        IcebergTableOperationsProvider tableOperationsProvider = new FileMetastoreTableOperationsProvider(fileSystemFactory, FILE_IO_FACTORY, encryptionManagerFactory);
        CachingHiveMetastore cachingHiveMetastore = createPerTransactionCache(metastore, 1000);
        return new TrinoHiveCatalog(
                new CatalogName(catalogName),
                cachingHiveMetastore,
                new TrinoViewHiveMetastore(cachingHiveMetastore, false, "trino-version", "test"),
                fileSystemFactory,
                FILE_IO_FACTORY,
                TESTING_TYPE_MANAGER,
                tableOperationsProvider,
                false,
                false,
                false,
                new IcebergConfig().isHideMaterializedViewStorageTable(),
                directExecutor(),
                newDirectExecutorService());
    }

    public static Map<String, Long> getMetadataFileAndUpdatedMillis(TrinoFileSystem trinoFileSystem, String tableLocation)
            throws IOException
    {
        FileIterator fileIterator = trinoFileSystem.listFiles(Location.of(tableLocation + "/metadata"));
        Map<String, Long> metadataFiles = new HashMap<>();
        while (fileIterator.hasNext()) {
            FileEntry entry = fileIterator.next();
            if (fromFilePath(entry.location().path()) == METADATA_JSON) {
                TableMetadata tableMetadata = TableMetadataParser.read(new ForwardingInputFile(trinoFileSystem.newInputFile(entry.location())));
                metadataFiles.put(entry.location().path(), tableMetadata.lastUpdatedMillis());
            }
        }
        return metadataFiles;
    }

    public static ParquetMetadata getParquetFileMetadata(TrinoInputFile inputFile)
    {
        try (TrinoParquetDataSource dataSource = new TrinoParquetDataSource(inputFile, ParquetReaderOptions.defaultOptions(), new FileFormatDataSourceStats())) {
            return MetadataReader.readFooter(dataSource, Optional.empty());
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
