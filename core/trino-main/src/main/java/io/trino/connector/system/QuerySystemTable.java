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
package io.trino.connector.system;

import com.google.inject.Inject;
import io.airlift.units.Duration;
import io.trino.FullConnectorSession;
import io.trino.dispatcher.DispatchManager;
import io.trino.execution.QueryInfo;
import io.trino.execution.QueryStats;
import io.trino.security.AccessControl;
import io.trino.server.BasicQueryInfo;
import io.trino.spi.ErrorCode;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.InMemoryRecordSet;
import io.trino.spi.connector.InMemoryRecordSet.Builder;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.resourcegroups.ResourceGroupId;
import io.trino.spi.type.ArrayType;
import org.joda.time.DateTime;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.metadata.MetadataUtil.TableMetadataBuilder.tableMetadataBuilder;
import static io.trino.security.AccessControlUtil.filterQueries;
import static io.trino.spi.connector.SystemTable.Distribution.ALL_COORDINATORS;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.util.Objects.requireNonNull;

public class QuerySystemTable
        implements SystemTable
{
    public static final SchemaTableName QUERY_TABLE_NAME = new SchemaTableName("runtime", "queries");

    public static final ConnectorTableMetadata QUERY_TABLE = tableMetadataBuilder(QUERY_TABLE_NAME)
            .column("query_id", createUnboundedVarcharType())
            .column("state", createUnboundedVarcharType())
            .column("user", createUnboundedVarcharType())
            .column("source", createUnboundedVarcharType())
            .column("query", createUnboundedVarcharType())
            .column("resource_group_id", new ArrayType(createUnboundedVarcharType()))

            .column("queued_time_ms", BIGINT)
            .column("analysis_time_ms", BIGINT)
            .column("planning_time_ms", BIGINT)

            .column("created", TIMESTAMP_TZ_MILLIS)
            .column("started", TIMESTAMP_TZ_MILLIS)
            .column("last_heartbeat", TIMESTAMP_TZ_MILLIS)
            .column("end", TIMESTAMP_TZ_MILLIS)

            .column("error_type", createUnboundedVarcharType())
            .column("error_code", createUnboundedVarcharType())
            .build();

    private final Optional<DispatchManager> dispatchManager;
    private final AccessControl accessControl;

    @Inject
    public QuerySystemTable(Optional<DispatchManager> dispatchManager, AccessControl accessControl)
    {
        this.dispatchManager = requireNonNull(dispatchManager, "dispatchManager is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
    }

    @Override
    public Distribution getDistribution()
    {
        return ALL_COORDINATORS;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata()
    {
        return QUERY_TABLE;
    }

    @Override
    public RecordCursor cursor(ConnectorTransactionHandle transactionHandle, ConnectorSession session, TupleDomain<Integer> constraint)
    {
        checkState(dispatchManager.isPresent(), "Query system table can return results only on coordinator");

        List<BasicQueryInfo> queries = dispatchManager.get().getQueries();
        queries = filterQueries(((FullConnectorSession) session).getSession().getIdentity(), queries, accessControl);

        Builder table = InMemoryRecordSet.builder(QUERY_TABLE);
        for (BasicQueryInfo queryInfo : queries) {
            Optional<QueryInfo> fullQueryInfo = dispatchManager.get().getFullQueryInfo(queryInfo.getQueryId());
            if (fullQueryInfo.isEmpty()) {
                continue;
            }
            QueryStats queryStats = fullQueryInfo.get().getQueryStats();
            table.addRow(
                    queryInfo.getQueryId().toString(),
                    queryInfo.getState().toString(),
                    queryInfo.getSession().getUser(),
                    queryInfo.getSession().getSource().orElse(null),
                    queryInfo.getQuery(),
                    queryInfo.getResourceGroupId().map(QuerySystemTable::resourceGroupIdToBlock).orElse(null),

                    toMillis(queryStats.getQueuedTime()),
                    toMillis(queryStats.getAnalysisTime()),
                    toMillis(queryStats.getPlanningTime()),

                    toTimestampWithTimeZoneMillis(queryStats.getCreateTime()),
                    toTimestampWithTimeZoneMillis(queryStats.getExecutionStartTime()),
                    toTimestampWithTimeZoneMillis(queryStats.getLastHeartbeat()),
                    toTimestampWithTimeZoneMillis(queryStats.getEndTime()),

                    Optional.ofNullable(queryInfo.getErrorType()).map(Enum::name).orElse(null),
                    Optional.ofNullable(queryInfo.getErrorCode()).map(ErrorCode::getName).orElse(null));
        }
        return table.build().cursor();
    }

    private static Block resourceGroupIdToBlock(ResourceGroupId resourceGroupId)
    {
        requireNonNull(resourceGroupId, "resourceGroupId is null");
        List<String> segments = resourceGroupId.getSegments();
        BlockBuilder blockBuilder = createUnboundedVarcharType().createBlockBuilder(null, segments.size());
        for (String segment : segments) {
            createUnboundedVarcharType().writeSlice(blockBuilder, utf8Slice(segment));
        }
        return blockBuilder.build();
    }

    private static Long toMillis(Duration duration)
    {
        if (duration == null) {
            return null;
        }
        return duration.toMillis();
    }

    private static Long toTimestampWithTimeZoneMillis(DateTime dateTime)
    {
        if (dateTime == null) {
            return null;
        }
        // dateTime.getZone() is the server zone, should be of no interest to the user
        return packDateTimeWithZone(dateTime.getMillis(), UTC_KEY);
    }
}
