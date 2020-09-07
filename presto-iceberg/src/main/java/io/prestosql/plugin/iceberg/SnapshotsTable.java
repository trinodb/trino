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
package io.prestosql.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import io.prestosql.plugin.iceberg.util.PageListBuilder;
import io.prestosql.spi.Page;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.FixedPageSource;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.SystemTable;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.TimeZoneKey;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeSignature;
import org.apache.iceberg.Table;

import java.util.List;

import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
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
            if (checkNonNull(snapshot.parentId(), pagesBuilder)) {
                pagesBuilder.appendBigint(snapshot.parentId());
            }
            if (checkNonNull(snapshot.operation(), pagesBuilder)) {
                pagesBuilder.appendVarchar(snapshot.operation());
            }
            if (checkNonNull(snapshot.manifestListLocation(), pagesBuilder)) {
                pagesBuilder.appendVarchar(snapshot.manifestListLocation());
            }
            if (checkNonNull(snapshot.summary(), pagesBuilder)) {
                pagesBuilder.appendVarcharVarcharMap(snapshot.summary());
            }
            pagesBuilder.endRow();
        });

        return pagesBuilder.build();
    }

    private static boolean checkNonNull(Object object, PageListBuilder pagesBuilder)
    {
        if (object == null) {
            pagesBuilder.appendNull();
            return false;
        }
        return true;
    }
}
