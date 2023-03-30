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

import com.google.common.collect.ImmutableList;
import io.trino.plugin.iceberg.util.PageListBuilder;
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
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeSignature;
import org.apache.iceberg.Table;

import java.util.List;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class SnapshotsTable
        implements SystemTable
{
    private final ConnectorTableMetadata tableMetadata;
    private final Table icebergTable;

    public SnapshotsTable(SchemaTableName tableName, TypeManager typeManager, Table icebergTable)
    {
        requireNonNull(typeManager, "typeManager is null");

        this.icebergTable = requireNonNull(icebergTable, "icebergTable is null");
        tableMetadata = new ConnectorTableMetadata(requireNonNull(tableName, "tableName is null"),
                ImmutableList.<ColumnMetadata>builder()
                        .add(new ColumnMetadata("committed_at", TIMESTAMP_TZ_MILLIS))
                        .add(new ColumnMetadata("snapshot_id", BIGINT))
                        .add(new ColumnMetadata("parent_id", BIGINT))
                        .add(new ColumnMetadata("operation", VARCHAR))
                        .add(new ColumnMetadata("manifest_list", VARCHAR))
                        .add(new ColumnMetadata("summary", typeManager.getType(TypeSignature.mapType(VARCHAR.getTypeSignature(), VARCHAR.getTypeSignature()))))
                        .build());
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
        return new FixedPageSource(buildPages(tableMetadata, session, icebergTable));
    }

    private static List<Page> buildPages(ConnectorTableMetadata tableMetadata, ConnectorSession session, Table icebergTable)
    {
        PageListBuilder pagesBuilder = PageListBuilder.forTable(tableMetadata);

        TimeZoneKey timeZoneKey = session.getTimeZoneKey();
        icebergTable.snapshots().forEach(snapshot -> {
            pagesBuilder.beginRow();
            pagesBuilder.appendTimestampTzMillis(snapshot.timestampMillis(), timeZoneKey);
            pagesBuilder.appendBigint(snapshot.snapshotId());
            pagesBuilder.appendBigint(snapshot.parentId());
            pagesBuilder.appendVarchar(snapshot.operation());
            pagesBuilder.appendVarchar(snapshot.manifestListLocation());
            pagesBuilder.appendVarcharVarcharMap(snapshot.summary());
            pagesBuilder.endRow();
        });

        return pagesBuilder.build();
    }
}
