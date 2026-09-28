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
package io.trino.plugin.clickhouse;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.trino.plugin.jdbc.DefaultJdbcMetadata;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcQueryEventListener;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.TimestampTimeZoneDomain;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableExecuteHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.RetryMode;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.trino.plugin.clickhouse.ClickHouseClient.clickhouseVarcharLiteral;
import static io.trino.plugin.clickhouse.DropPartitionTableProcedure.NAME;
import static io.trino.plugin.clickhouse.DropPartitionTableProcedure.PARTITION_PROPERTY;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ClickHouseMetadata
        extends DefaultJdbcMetadata
{
    private final JdbcClient jdbcClient;

    @Inject
    public ClickHouseMetadata(JdbcClient jdbcClient, TimestampTimeZoneDomain timestampTimeZoneDomain, Set<JdbcQueryEventListener> jdbcQueryEventListeners)
    {
        super(jdbcClient, timestampTimeZoneDomain, true, jdbcQueryEventListeners);
        this.jdbcClient = requireNonNull(jdbcClient, "jdbcClient is null");
    }

    @Override
    public Optional<ConnectorTableExecuteHandle> getTableHandleForExecute(
            ConnectorSession session,
            ConnectorAccessControl accessControl,
            ConnectorTableHandle tableHandle,
            String procedureName,
            Map<String, Object> executeProperties,
            RetryMode retryMode)
    {
        if (!procedureName.equals(NAME)) {
            return Optional.empty();
        }
        return Optional.of(new ClickHouseDropPartitionHandle(
                ((JdbcTableHandle) tableHandle).asPlainTable().getRemoteTableName(),
                (String) executeProperties.get(PARTITION_PROPERTY)));
    }

    @Override
    public Map<String, Long> executeTableExecute(ConnectorSession session, ConnectorTableExecuteHandle tableExecuteHandle)
    {
        ClickHouseDropPartitionHandle executeHandle = (ClickHouseDropPartitionHandle) tableExecuteHandle;
        // The partition is identified by the value of the table's partition expression, which ClickHouse
        // accepts quoted for every partition key except a composite one. It casts the literal itself, so a
        // date-valued key takes '2020-01-01' and a numeric one takes '202001'. Dropping a partition that
        // does not exist is a no-op rather than an error.
        jdbcClient.execute(session, format(
                "ALTER TABLE %s DROP PARTITION %s",
                jdbcClient.quoted(executeHandle.remoteTableName()),
                clickhouseVarcharLiteral(executeHandle.partition())));
        return ImmutableMap.of();
    }
}
