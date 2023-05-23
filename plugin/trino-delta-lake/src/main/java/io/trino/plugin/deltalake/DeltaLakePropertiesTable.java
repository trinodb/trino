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
import io.trino.plugin.deltalake.transactionlog.MetadataEntry;
import io.trino.plugin.deltalake.transactionlog.ProtocolEntry;
import io.trino.plugin.deltalake.util.PageListBuilder;
import io.trino.spi.Page;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.FixedPageSource;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.predicate.TupleDomain;

import java.util.List;

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

    private final ConnectorTableMetadata tableMetadata;
    private final MetadataEntry metadataEntry;
    private final ProtocolEntry protocolEntry;

    public DeltaLakePropertiesTable(SchemaTableName tableName, MetadataEntry metadataEntry, ProtocolEntry protocolEntry)
    {
        this.metadataEntry = requireNonNull(metadataEntry, "metadataEntry is null");
        this.protocolEntry = requireNonNull(protocolEntry, "protocolEntry is null");
        this.tableMetadata = new ConnectorTableMetadata(requireNonNull(tableName, "tableName is null"), COLUMNS);
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
        return new FixedPageSource(buildPages());
    }

    private List<Page> buildPages()
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
        pagesBuilder.appendVarchar(String.valueOf(protocolEntry.getMinReaderVersion()));
        pagesBuilder.endRow();

        pagesBuilder.beginRow();
        pagesBuilder.appendVarchar(MIN_WRITER_VERSION_KEY);
        pagesBuilder.appendVarchar(String.valueOf(protocolEntry.getMinWriterVersion()));
        pagesBuilder.endRow();

        ImmutableSet.<String>builder()
                .addAll(protocolEntry.getReaderFeatures().orElseGet(ImmutableSet::of))
                .addAll(protocolEntry.getWriterFeatures().orElseGet(ImmutableSet::of))
                .build().forEach(feature -> {
                    pagesBuilder.beginRow();
                    pagesBuilder.appendVarchar(DELTA_FEATURE_PREFIX + feature);
                    pagesBuilder.appendVarchar("supported");
                    pagesBuilder.endRow();
                });

        return pagesBuilder.build();
    }
}
