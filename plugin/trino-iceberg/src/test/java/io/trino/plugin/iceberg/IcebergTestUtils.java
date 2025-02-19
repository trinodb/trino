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
import io.trino.parquet.metadata.BlockMetadata;
import io.trino.parquet.metadata.ColumnChunkMetadata;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.parquet.reader.MetadataReader;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.hive.TrinoViewHiveMetastore;
import io.trino.plugin.hive.parquet.TrinoParquetDataSource;
import io.trino.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.catalog.file.FileMetastoreTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.hms.TrinoHiveCatalog;
import io.trino.plugin.iceberg.fileio.ForwardingInputFile;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.TestingTypeManager;
import io.trino.spi.type.Type;
import io.trino.testing.QueryRunner;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Iterators.getOnlyElement;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.metastore.cache.CachingHiveMetastore.createPerTransactionCache;
import static io.trino.orc.OrcReader.INITIAL_BATCH_SIZE;
import static io.trino.plugin.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static io.trino.plugin.iceberg.IcebergUtil.loadIcebergTable;
import static io.trino.plugin.iceberg.util.FileOperationUtils.FileType.METADATA_JSON;
import static io.trino.plugin.iceberg.util.FileOperationUtils.FileType.fromFilePath;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.joda.time.DateTimeZone.UTC;

public final class IcebergTestUtils
{
    private IcebergTestUtils() {}

    public static Session withSmallRowGroups(Session session)
    {
        return Session.builder(session)
                .setCatalogSessionProperty("iceberg", "orc_writer_max_stripe_rows", "20")
                .setCatalogSessionProperty("iceberg", "parquet_writer_block_size", "1kB")
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
            OrcColumn sortColumn = orcReader.getRootColumn().getNestedColumns().stream()
                    .filter(column -> column.getColumnName().equals(sortColumnName))
                    .collect(onlyElement());
            Type sortColumnType = getType(sortColumn.getColumnType().getOrcTypeKind());
            try (OrcRecordReader recordReader = orcReader.createRecordReader(
                    List.of(sortColumn),
                    List.of(sortColumnType),
                    OrcPredicate.TRUE,
                    UTC,
                    newSimpleAggregatedMemoryContext(),
                    INITIAL_BATCH_SIZE,
                    RuntimeException::new)) {
                Comparable<Object> previousMax = null;
                for (Page page = recordReader.nextPage(); page != null; page = recordReader.nextPage()) {
                    Block block = page.getLoadedPage().getBlock(0);
                    for (int position = 0; position < block.getPositionCount(); position++) {
                        Comparable<Object> current = (Comparable<Object>) readNativeValue(sortColumnType, block, position);
                        if (previousMax != null && previousMax.compareTo(current) > 0) {
                            return false;
                        }
                        previousMax = current;
                    }
                }
            }
            return true;
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

    @SuppressWarnings({"unchecked", "rawtypes"})
    public static boolean checkParquetFileSorting(TrinoInputFile inputFile, String sortColumnName)
    {
        ParquetMetadata parquetMetadata = getParquetFileMetadata(inputFile);
        List<BlockMetadata> blocks;
        try {
            blocks = parquetMetadata.getBlocks();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        Comparable previousMax = null;
        verify(blocks.size() > 1, "Test must produce at least two row groups");
        for (BlockMetadata blockMetaData : blocks) {
            ColumnChunkMetadata columnMetadata = blockMetaData.columns().stream()
                    .filter(column -> getOnlyElement(column.getPath().iterator()).equalsIgnoreCase(sortColumnName))
                    .collect(onlyElement());
            if (previousMax != null) {
                if (previousMax.compareTo(columnMetadata.getStatistics().genericGetMin()) > 0) {
                    return false;
                }
            }
            previousMax = columnMetadata.getStatistics().genericGetMax();
        }
        return true;
    }

    public static TrinoFileSystemFactory getFileSystemFactory(QueryRunner queryRunner)
    {
        return ((IcebergConnector) queryRunner.getCoordinator().getConnector(ICEBERG_CATALOG))
                .getInjector().getInstance(TrinoFileSystemFactory.class);
    }

    public static HiveMetastore getHiveMetastore(QueryRunner queryRunner)
    {
        return ((IcebergConnector) queryRunner.getCoordinator().getConnector(ICEBERG_CATALOG)).getInjector()
                .getInstance(HiveMetastoreFactory.class)
                .createMetastore(Optional.empty());
    }

    public static BaseTable loadTable(String tableName,
            HiveMetastore metastore,
            TrinoFileSystemFactory fileSystemFactory,
            String catalogName,
            String schemaName)
    {
        IcebergTableOperationsProvider tableOperationsProvider = new FileMetastoreTableOperationsProvider(fileSystemFactory);
        CachingHiveMetastore cachingHiveMetastore = createPerTransactionCache(metastore, 1000);
        TrinoCatalog catalog = new TrinoHiveCatalog(
                new CatalogName(catalogName),
                cachingHiveMetastore,
                new TrinoViewHiveMetastore(cachingHiveMetastore, false, "trino-version", "test"),
                fileSystemFactory,
                new TestingTypeManager(),
                tableOperationsProvider,
                false,
                false,
                false,
                new IcebergConfig().isHideMaterializedViewStorageTable(),
                directExecutor());
        return loadIcebergTable(catalog, tableOperationsProvider, SESSION, new SchemaTableName(schemaName, tableName));
    }

    public static Map<String, Long> getMetadataFileAndUpdatedMillis(TrinoFileSystem trinoFileSystem, String tableLocation)
            throws IOException
    {
        FileIterator fileIterator = trinoFileSystem.listFiles(Location.of(tableLocation + "/metadata"));
        Map<String, Long> metadataFiles = new HashMap<>();
        while (fileIterator.hasNext()) {
            FileEntry entry = fileIterator.next();
            if (fromFilePath(entry.location().path()) == METADATA_JSON) {
                TableMetadata tableMetadata = TableMetadataParser.read(null, new ForwardingInputFile(trinoFileSystem.newInputFile(entry.location())));
                metadataFiles.put(entry.location().path(), tableMetadata.lastUpdatedMillis());
            }
        }
        return metadataFiles;
    }

    public static ParquetMetadata getParquetFileMetadata(TrinoInputFile inputFile)
    {
        try {
            return MetadataReader.readFooter(
                    new TrinoParquetDataSource(inputFile, new ParquetReaderOptions(), new FileFormatDataSourceStats()),
                    Optional.empty());
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
