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
package io.trino.plugin.deltalake.kernel.engine;

import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.engine.ParquetHandler;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.DataFileStatus;
import io.delta.kernel.utils.FileStatus;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.metastore.HiveType;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.plugin.deltalake.DeltaHiveTypeTranslator;
import io.trino.plugin.deltalake.kernel.KernelSchemaUtils;
import io.trino.plugin.deltalake.kernel.data.TrinoColumnarBatchWrapper;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.ReaderPageSource;
import io.trino.plugin.hive.parquet.ParquetPageSourceFactory;
import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import static io.trino.plugin.hive.parquet.ParquetPageSourceFactory.PARQUET_ROW_INDEX_COLUMN;
import static java.util.Objects.requireNonNull;

public class TrinoParquetHandler
        implements ParquetHandler
{
    private final TrinoFileSystem fileSystem;
    private final TypeManager typeManager;

    public TrinoParquetHandler(TrinoFileSystem fileSystem, TypeManager typeManager)
    {
        this.fileSystem = fileSystem;
        this.typeManager = typeManager;
    }

    @Override
    public CloseableIterator<ColumnarBatch> readParquetFiles(
            CloseableIterator<FileStatus> closeableIterator,
            StructType structType,
            Optional<Predicate> optional)
            throws IOException
    {
        return readFiles(closeableIterator, structType);
    }

    @Override
    public CloseableIterator<DataFileStatus> writeParquetFiles(String directoryPath, CloseableIterator<FilteredColumnarBatch> dataIter, List<Column> statsColumns)
            throws IOException
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public void writeParquetFileAtomically(String filePath, CloseableIterator<FilteredColumnarBatch> data)
            throws IOException
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    private CloseableIterator<ColumnarBatch> readFiles(CloseableIterator<FileStatus> fileIter, StructType physicalSchema)
    {
        return new CloseableIterator<>()
        {
            private CloseableParquetBatchReader currentFileReader;

            @Override
            public void close()
            {
                Utils.closeCloseables(fileIter, currentFileReader);
            }

            @Override
            public boolean hasNext()
            {
                if (currentFileReader != null && currentFileReader.hasNext()) {
                    return true;
                }

                Utils.closeCloseables(currentFileReader);
                if (!fileIter.hasNext()) {
                    return false; // no more files to read.
                }
                FileStatus fileStatus = fileIter.next();
                TrinoInputFile inputFile = fileSystem.newInputFile(Location.of(fileStatus.getPath()));

                try {
                    ReaderPageSource pageSource = ParquetPageSourceFactory.createPageSource(
                            inputFile,
                            0L /* start index in file */,
                            inputFile.length(),
                            createHiveHandles(physicalSchema),
                            Collections.singletonList(TupleDomain.all()) /* disjunctTupleDomains */,
                            true /* useColumnNames */,
                            DateTimeZone.getDefault(),
                            new FileFormatDataSourceStats(),
                            new ParquetReaderOptions(),
                            Optional.empty() /* parquet writer validation */,
                            100 /* Maximum ranges to allow in a tuple domain without compacting it */,
                            OptionalLong.empty());

                    currentFileReader = new CloseableParquetBatchReader(physicalSchema, pageSource.get());
                }
                catch (IOException ioe) {
                    throw new UncheckedIOException(ioe);
                }
                return currentFileReader.hasNext();
            }

            @Override
            public ColumnarBatch next()
            {
                return currentFileReader.next();
            }
        };
    }

    private List<HiveColumnHandle> createHiveHandles(StructType deltaSchema)
    {
        List<HiveColumnHandle> hiveColumnHandles = new ArrayList<>();
        for (StructField structField : deltaSchema.fields()) {
            DataType kernelType = structField.getDataType();
            String name = structField.getName();

            if (structField.isMetadataColumn() &&
                    StructField.METADATA_ROW_INDEX_COLUMN_NAME.equalsIgnoreCase(name)) {
                hiveColumnHandles.add(PARQUET_ROW_INDEX_COLUMN);
                continue;
            }

            Type trinoType = KernelSchemaUtils
                    .toTrinoType(new SchemaTableName("test", "test"), typeManager, kernelType);
            HiveType hiveType = DeltaHiveTypeTranslator.toHiveType(trinoType);

            HiveColumnHandle hiveColumnHandle =
                    new HiveColumnHandle(
                            name, // this name is used for accessing Parquet files, so it should be physical name
                            0, // hiveColumnIndex; we provide fake value because we always find columns by name
                            hiveType,
                            trinoType,
                            Optional.empty(),
                            HiveColumnHandle.ColumnType.REGULAR,
                            Optional.empty());

            hiveColumnHandles.add(hiveColumnHandle);
        }

        return hiveColumnHandles;
    }

    private static class CloseableParquetBatchReader
            implements CloseableIterator<ColumnarBatch>
    {
        private final ConnectorPageSource deltaPageSource;
        private final StructType schema;

        private Page nextPage;

        public CloseableParquetBatchReader(StructType schema, ConnectorPageSource deltaPageSource)
        {
            this.deltaPageSource = requireNonNull(deltaPageSource, "deltaPageSource is null");
            this.schema = requireNonNull(schema, "schema is null");
        }

        @Override
        public boolean hasNext()
        {
            if (nextPage != null) {
                return true;
            }
            nextPage = deltaPageSource.getNextPage();

            return nextPage != null;
        }

        @Override
        public ColumnarBatch next()
        {
            Page page = nextPage != null ? nextPage : deltaPageSource.getNextPage();
            nextPage = null;
            return new TrinoColumnarBatchWrapper(schema, page);
        }

        @Override
        public void close()
                throws IOException
        {
            deltaPageSource.close();
        }
    }
}
