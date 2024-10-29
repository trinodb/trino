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

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.trino.plugin.jdbc.ForRecordCursor;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcProcedureHandle;
import io.trino.plugin.jdbc.JdbcSplit;
import io.trino.plugin.jdbc.JdbcSplitManager;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.PreparedQuery;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

import static io.trino.plugin.jdbc.JdbcDynamicFilteringSessionProperties.dynamicFilteringEnabled;
import static io.trino.plugin.redshift.RedshiftSessionProperties.useUnload;
import static java.util.Objects.requireNonNull;

public class RedshiftUnloadSplitManager
        implements ConnectorSplitManager
{
    private final JdbcClient jdbcClient;
    private final QueryBuilder queryBuilder;
    private final RemoteQueryModifier queryModifier;
    private final JdbcSplitManager jdbcSplitManager;
    private final String unloadLocation;
    private final String unloadOptions;
    private final String unloadAuthorization;
    private final ExecutorService executor;

    @Inject
    public RedshiftUnloadSplitManager(
            JdbcClient jdbcClient,
            QueryBuilder queryBuilder,
            RemoteQueryModifier queryModifier,
            JdbcSplitManager jdbcSplitManager,
            RedshiftConfig redshiftConfig,
            @ForRecordCursor ExecutorService executor)
    {
        this.jdbcClient = requireNonNull(jdbcClient, "jdbcClient is null");
        this.queryBuilder = requireNonNull(queryBuilder, "queryBuilder is null");
        this.queryModifier = requireNonNull(queryModifier, "queryModifier is null");
        this.jdbcSplitManager = requireNonNull(jdbcSplitManager, "jdbcSplitManager is null");

        this.unloadLocation = redshiftConfig.getUnloadLocation()
                .map(location -> location.replaceAll("/$", "") + "/")
                .orElse(null);
        this.unloadOptions = redshiftConfig.getUnloadOptions().orElse("");
        if (redshiftConfig.getIamRole().isPresent()) {
            this.unloadAuthorization = "IAM_ROLE '%s'".formatted(redshiftConfig.getIamRole().get());
        }
        else {
            this.unloadAuthorization = "";
        }
        this.executor = requireNonNull(executor, "executor is null");
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorTableHandle table, DynamicFilter dynamicFilter, Constraint constraint)
    {
        if (table instanceof JdbcProcedureHandle) {
            return jdbcSplitManager.getSplits(transaction, session, table, dynamicFilter, constraint);
        }
        if (!useUnload(session)) {
            return new FixedSplitSource(new JdbcSplit(Optional.empty()));
        }
        JdbcTableHandle jdbcTableHandle = dynamicFilteringEnabled(session) ? ((JdbcTableHandle) table).intersectedWithConstraint(dynamicFilter.getCurrentPredicate()) : (JdbcTableHandle) table;
        List<JdbcColumnHandle> columns = jdbcTableHandle.getColumns()
                .orElseGet(() -> jdbcClient.getColumns(
                        session,
                        jdbcTableHandle.getRequiredNamedRelation().getSchemaTableName(),
                        jdbcTableHandle.getRequiredNamedRelation().getRemoteTableName()));
        Connection connection;
        PreparedStatement statement;
        try {
            connection = jdbcClient.getConnection(session);
            statement = buildUnloadSql(session, connection, jdbcTableHandle, columns);
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return new RedshiftUnloadSplitSource(executor, connection, statement);
    }

    private PreparedStatement buildUnloadSql(ConnectorSession session, Connection connection, JdbcTableHandle table, List<JdbcColumnHandle> columns)
            throws SQLException
    {
        PreparedQuery preparedQuery = jdbcClient.prepareQuery(session, table, Optional.empty(), columns, ImmutableMap.of());
        String modifiedQuery = queryModifier.apply(session, preparedQuery.query());

        String sql = "UNLOAD ('%s') TO '%s' %s FORMAT PARQUET %s".formatted(
                formatStringLiteral(modifiedQuery),
                unloadLocation + session.getQueryId() + "-" + UUID.randomUUID() + "/",
                unloadAuthorization,
                unloadOptions);
        return queryBuilder.prepareStatement(jdbcClient, session, connection, new PreparedQuery(sql, preparedQuery.parameters()), Optional.of(columns.size()));
    }

    private static String formatStringLiteral(String x)
    {
        return x.replace("'", "''");
    }
}
