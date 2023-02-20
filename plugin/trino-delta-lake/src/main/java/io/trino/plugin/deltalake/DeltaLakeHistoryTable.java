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
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.deltalake.metastore.DeltaLakeMetastore;
import io.trino.plugin.deltalake.transactionlog.CommitInfoEntry;
import io.trino.plugin.deltalake.transactionlog.DeltaLakeTransactionLogEntry;
import io.trino.plugin.deltalake.transactionlog.DeltaLakeTransactionLogIterator;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.InMemoryRecordSet;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import org.apache.hadoop.fs.Path;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Streams.stream;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.TypeSignature.mapType;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class DeltaLakeHistoryTable
        implements SystemTable
{
    private static final int VERSION_COLUMN_INDEX = 0;
    private final SchemaTableName tableName;
    private final ConnectorTableMetadata tableMetadata;
    private final List<Type> types;
    private final TrinoFileSystemFactory fileSystemFactory;
    private final DeltaLakeMetastore metastore;

    private final Type varcharToVarcharMapType;

    public DeltaLakeHistoryTable(
            SchemaTableName tableName,
            TrinoFileSystemFactory fileSystemFactory,
            DeltaLakeMetastore metastore,
            TypeManager typeManager)
    {
        this.tableName = requireNonNull(tableName, "tableName is null");
        requireNonNull(typeManager, "typeManager is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.metastore = requireNonNull(metastore, "metastore is null");

        SchemaTableName systemTableName = new SchemaTableName(
                tableName.getSchemaName(),
                new DeltaLakeTableName(tableName.getTableName(), DeltaLakeTableType.HISTORY).getTableNameWithType());
        tableMetadata = new ConnectorTableMetadata(
                systemTableName,
                ImmutableList.<ColumnMetadata>builder()
                        .add(new ColumnMetadata("version", BIGINT))
                        .add(new ColumnMetadata("timestamp", TIMESTAMP_TZ_MILLIS))
                        .add(new ColumnMetadata("user_id", VARCHAR))
                        .add(new ColumnMetadata("user_name", VARCHAR))
                        .add(new ColumnMetadata("operation", VARCHAR))
                        .add(new ColumnMetadata("operation_parameters", typeManager.getType(mapType(VARCHAR.getTypeSignature(), VARCHAR.getTypeSignature()))))
                        .add(new ColumnMetadata("cluster_id", VARCHAR))
                        .add(new ColumnMetadata("read_version", BIGINT))
                        .add(new ColumnMetadata("isolation_level", VARCHAR))
                        .add(new ColumnMetadata("is_blind_append", BOOLEAN))
                        //TODO add support for operationMetrics, userMetadata, engineInfo
                        .build());
        types = tableMetadata.getColumns().stream()
                .map(ColumnMetadata::getType)
                .collect(toImmutableList());
        varcharToVarcharMapType = typeManager.getType(mapType(VARCHAR.getTypeSignature(), VARCHAR.getTypeSignature()));
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
    public RecordCursor cursor(ConnectorTransactionHandle transactionHandle, ConnectorSession session, TupleDomain<Integer> constraint)
    {
        VersionRange versionRange = extractVersionRange(constraint);
        TimeZoneKey timeZoneKey = session.getTimeZoneKey();
        TrinoFileSystem fileSystem = fileSystemFactory.create(session);
        Iterable<List<Object>> records = () ->
                stream(new DeltaLakeTransactionLogIterator(
                        fileSystem,
                        new Path(metastore.getTableLocation(tableName, session)),
                        versionRange.startVersion,
                        versionRange.endVersion))
                        .flatMap(Collection::stream)
                        .map(DeltaLakeTransactionLogEntry::getCommitInfo)
                        .filter(Objects::nonNull)
                        .map(commitInfoEntry -> getRecord(commitInfoEntry, timeZoneKey))
                        .iterator();

        return new InMemoryRecordSet(types, records).cursor();
    }

    private List<Object> getRecord(CommitInfoEntry commitInfoEntry, TimeZoneKey timeZoneKey)
    {
        List<Object> columns = new ArrayList<>();
        columns.add(commitInfoEntry.getVersion());
        columns.add(packDateTimeWithZone(commitInfoEntry.getTimestamp(), timeZoneKey));
        columns.add(commitInfoEntry.getUserId());
        columns.add(commitInfoEntry.getUserName());
        columns.add(commitInfoEntry.getOperation());
        if (commitInfoEntry.getOperationParameters() == null) {
            columns.add(null);
        }
        else {
            columns.add(toVarcharVarcharMapBlock(commitInfoEntry.getOperationParameters()));
        }
        columns.add(commitInfoEntry.getClusterId());
        columns.add(commitInfoEntry.getReadVersion());
        columns.add(commitInfoEntry.getIsolationLevel());
        columns.add(commitInfoEntry.isBlindAppend().orElse(null));

        return columns;
    }

    private Object toVarcharVarcharMapBlock(Map<String, String> values)
    {
        BlockBuilder blockBuilder = varcharToVarcharMapType.createBlockBuilder(null, 1);
        BlockBuilder singleMapBlockBuilder = blockBuilder.beginBlockEntry();
        values.forEach((key, value) -> {
            VARCHAR.writeString(singleMapBlockBuilder, key);
            VARCHAR.writeString(singleMapBlockBuilder, value);
        });
        blockBuilder.closeEntry();
        return varcharToVarcharMapType.getObject(blockBuilder, 0);
    }

    private static VersionRange extractVersionRange(TupleDomain<Integer> constraint)
    {
        Domain versionDomain = constraint.getDomains()
                .map(map -> map.get(VERSION_COLUMN_INDEX))
                .orElse(Domain.all(BIGINT));
        Optional<Long> startVersion = Optional.empty();
        Optional<Long> endVersion = Optional.empty();
        if (versionDomain.isAll() || versionDomain.isNone()) {
            return new VersionRange(startVersion, endVersion);
        }
        List<Range> orderedRanges = versionDomain.getValues().getRanges().getOrderedRanges();
        if (orderedRanges.size() == 1) {
            // Opt for a rather pragmatical choice of extracting the version range
            // only when dealing with a single range
            Range range = orderedRanges.get(0);
            if (range.isSingleValue()) {
                long version = (long) range.getLowBoundedValue();
                startVersion = Optional.of(version);
                endVersion = Optional.of(version);
            }
            else {
                if (!range.isLowUnbounded()) {
                    long version = (long) range.getLowBoundedValue();
                    if (!range.isLowInclusive()) {
                        version++;
                    }
                    startVersion = Optional.of(version);
                }
                if (!range.isHighUnbounded()) {
                    long version = (long) range.getHighBoundedValue();
                    if (!range.isHighInclusive()) {
                        version--;
                    }
                    endVersion = Optional.of(version);
                }
            }
        }
        return new VersionRange(startVersion, endVersion);
    }

    private record VersionRange(Optional<Long> startVersion, Optional<Long> endVersion)
    {
        @SuppressWarnings("UnusedVariable") // TODO: Remove once https://github.com/google/error-prone/issues/2713 is fixed
        private VersionRange
        {
            requireNonNull(startVersion, "startVersion is null");
            requireNonNull(endVersion, "endVersion is null");
            verify(startVersion.orElse(0L) <= endVersion.orElse(startVersion.orElse(0L)), "startVersion is greater than endVersion");
        }
    }
}
