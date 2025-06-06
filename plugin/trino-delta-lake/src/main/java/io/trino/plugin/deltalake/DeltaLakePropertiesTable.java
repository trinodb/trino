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
package io.trino.plugin.deltalake;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.plugin.deltalake.metastore.DeltaMetastoreTable;
import io.trino.plugin.deltalake.transactionlog.MetadataEntry;
import io.trino.plugin.deltalake.transactionlog.ProtocolEntry;
import io.trino.plugin.deltalake.transactionlog.TableSnapshot;
import io.trino.plugin.deltalake.transactionlog.TransactionLogAccess;
import io.trino.plugin.deltalake.util.PageListBuilder;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.FixedPageSource;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.predicate.TupleDomain;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class DeltaLakePropertiesTable
        implements SystemTable
{
    private static final String DELTA_FEATURE_PREFIX = "delta.feature.";
    private static final String MIN_READER_VERSION_KEY = "delta.minReaderVersion";
    private static final String MIN_WRITER_VERSION_KEY = "delta.minWriterVersion";

    private static final List<ColumnMetadata> COLUMNS = ImmutableList.<ColumnMetadata>builder()
            .add(new ColumnMetadata("key", VARCHAR))
            .add(new ColumnMetadata("value", VARCHAR))
            .build();

    private final DeltaMetastoreTable table;
    private final TransactionLogAccess transactionLogAccess;
    private final ConnectorTableMetadata tableMetadata;

    public DeltaLakePropertiesTable(DeltaMetastoreTable table, TransactionLogAccess transactionLogAccess)
    {
        this.table = requireNonNull(table, "table is null");
        this.transactionLogAccess = requireNonNull(transactionLogAccess, "transactionLogAccess is null");
        this.tableMetadata = new ConnectorTableMetadata(requireNonNull(table.schemaTableName(), "tableName is null"), COLUMNS);
    }

    @Override
    public Distribution getDistribution()
    {
        return Distribution.SINGLE_COORDINATOR;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata()
    {
        return tableMetadata;
    }

    @Override
    public ConnectorPageSource pageSource(ConnectorTransactionHandle transactionHandle, ConnectorSession session, TupleDomain<Integer> constraint)
    {
        MetadataEntry metadataEntry;
        ProtocolEntry protocolEntry;

        try {
            TableSnapshot tableSnapshot = transactionLogAccess.loadSnapshot(session, table, Optional.empty());
            metadataEntry = transactionLogAccess.getMetadataEntry(session, tableSnapshot);
            protocolEntry = transactionLogAccess.getProtocolEntry(session, tableSnapshot);
        }
        catch (IOException e) {
            throw new TrinoException(DeltaLakeErrorCode.DELTA_LAKE_INVALID_SCHEMA, "Unable to load table metadata from location: " + table.location(), e);
        }

        return new FixedPageSource(buildPages(metadataEntry, protocolEntry));
    }

    private List<Page> buildPages(MetadataEntry metadataEntry, ProtocolEntry protocolEntry)
    {
        PageListBuilder pagesBuilder = PageListBuilder.forTable(tableMetadata);

        metadataEntry.getConfiguration().forEach((key, value) -> {
            pagesBuilder.beginRow();
            pagesBuilder.appendVarchar(key);
            pagesBuilder.appendVarchar(value);
            pagesBuilder.endRow();
        });

        pagesBuilder.beginRow();
        pagesBuilder.appendVarchar(MIN_READER_VERSION_KEY);
        pagesBuilder.appendVarchar(String.valueOf(protocolEntry.minReaderVersion()));
        pagesBuilder.endRow();

        pagesBuilder.beginRow();
        pagesBuilder.appendVarchar(MIN_WRITER_VERSION_KEY);
        pagesBuilder.appendVarchar(String.valueOf(protocolEntry.minWriterVersion()));
        pagesBuilder.endRow();

        ImmutableSet.<String>builder()
                .addAll(protocolEntry.readerFeatures().orElseGet(ImmutableSet::of))
                .addAll(protocolEntry.writerFeatures().orElseGet(ImmutableSet::of))
                .build().forEach(feature -> {
                    pagesBuilder.beginRow();
                    pagesBuilder.appendVarchar(DELTA_FEATURE_PREFIX + feature);
                    pagesBuilder.appendVarchar("supported");
                    pagesBuilder.endRow();
                });

        return pagesBuilder.build();
    }
}
