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
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInputFile;
import io.trino.parquet.Column;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.metadata.BlockMetadata;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.parquet.reader.MetadataReader;
import io.trino.parquet.reader.ParquetReader;
import io.trino.parquet.reader.RowGroupInfo;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcSplit;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.RecordPageSource;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.schema.MessageType;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.parquet.ParquetTypeUtils.constructField;
import static io.trino.parquet.ParquetTypeUtils.getColumnIO;
import static io.trino.parquet.ParquetTypeUtils.getDescriptors;
import static io.trino.parquet.ParquetTypeUtils.lookupColumnByName;
import static io.trino.parquet.metadata.PrunedBlockMetadata.createPrunedColumnsMetadata;
import static io.trino.plugin.redshift.RedshiftErrorCode.REDSHIFT_PARQUET_CURSOR_ERROR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class RedshiftPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final ConnectorRecordSetProvider recordSetProvider;
    private final TrinoFileSystemFactory fileSystemFactory;
    private final FileFormatDataSourceStats fileFormatDataSourceStats;

    public RedshiftPageSourceProvider(ConnectorRecordSetProvider recordSetProvider, TrinoFileSystemFactory fileSystemFactory, FileFormatDataSourceStats fileFormatDataSourceStats)
    {
        this.recordSetProvider = requireNonNull(recordSetProvider, "recordSetProvider is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.fileFormatDataSourceStats = requireNonNull(fileFormatDataSourceStats, "fileFormatDataSourceStats is null");
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
        if (split instanceof JdbcSplit) {
            return new RecordPageSource(recordSetProvider.getRecordSet(transaction, session, split, table, columns));
        }

        RedshiftUnloadSplit redshiftUnloadSplit = ((RedshiftUnloadSplit) split);
        String path = redshiftUnloadSplit.path();
        Location location = Location.of(path);
        TrinoFileSystem fileSystem = fileSystemFactory.create(session);
        TrinoInputFile inputFile = fileSystem.newInputFile(location, redshiftUnloadSplit.length());
        ParquetReader parquetReader;
        try {
            parquetReader = parquetReader(inputFile, columns);
        }
        catch (IOException e) {
            throw new TrinoException(REDSHIFT_PARQUET_CURSOR_ERROR, format("Failed to open Parquet file: %s", path), e);
        }
        return new RedshiftParquetPageSource(parquetReader);
    }

    private ParquetReader parquetReader(TrinoInputFile inputFile, List<ColumnHandle> columns)
            throws IOException
    {
        ParquetReaderOptions options = new ParquetReaderOptions();
        TrinoParquetDataSource dataSource = new TrinoParquetDataSource(inputFile, options, fileFormatDataSourceStats);
        ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, Optional.empty());
        MessageType fileSchema = parquetMetadata.getFileMetaData().getSchema();
        MessageColumnIO messageColumn = getColumnIO(fileSchema, fileSchema);
        Map<List<String>, ColumnDescriptor> descriptorsByPath = getDescriptors(fileSchema, fileSchema);
        DateTimeZone timeZone = DateTimeZone.UTC;
        List<Column> fields = fields(columns, messageColumn);
        long nextStart = 0;
        ImmutableList.Builder<RowGroupInfo> rowGroupInfoBuilder = ImmutableList.builder();
        for (BlockMetadata block : parquetMetadata.getBlocks()) {
            rowGroupInfoBuilder.add(new RowGroupInfo(createPrunedColumnsMetadata(block, dataSource.getId(), descriptorsByPath), nextStart, Optional.empty()));
            nextStart += block.rowCount();
        }
        return new ParquetReader(
                Optional.ofNullable(parquetMetadata.getFileMetaData().getCreatedBy()),
                fields,
                rowGroupInfoBuilder.build(),
                dataSource,
                timeZone,
                newSimpleAggregatedMemoryContext(),
                options,
                RedshiftParquetPageSource::handleException,
                Optional.empty(),
                Optional.empty());
    }

    private static List<Column> fields(List<ColumnHandle> columns, MessageColumnIO messageColumn)
    {
        ImmutableList.Builder<Column> parquetColumnFieldsBuilder = ImmutableList.builder();
        for (ColumnHandle column : columns) {
            JdbcColumnHandle jdbcColumn = (JdbcColumnHandle) column;
            constructField(jdbcColumn.getColumnType(), lookupColumnByName(messageColumn, jdbcColumn.getColumnName()))
                    .ifPresent(field -> parquetColumnFieldsBuilder.add(new Column(jdbcColumn.getColumnName(), field)));
        }

        return parquetColumnFieldsBuilder.build();
    }
}
