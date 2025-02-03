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

import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.s3.FileSystemS3;
import io.trino.plugin.jdbc.ForRecordCursor;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcProcedureHandle;
import io.trino.plugin.jdbc.JdbcSplit;
import io.trino.plugin.jdbc.JdbcSplitManager;
import io.trino.plugin.jdbc.JdbcTableHandle;
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
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.VarbinaryType;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import static io.trino.plugin.jdbc.JdbcDynamicFilteringSessionProperties.dynamicFilteringEnabled;
import static io.trino.plugin.redshift.RedshiftSessionProperties.isUnloadEnabled;
import static java.util.Objects.requireNonNull;

public class RedshiftSplitManager
        implements ConnectorSplitManager
{
    private static final Logger log = Logger.get(RedshiftSplitManager.class);

    private final JdbcClient jdbcClient;
    private final QueryBuilder queryBuilder;
    private final RemoteQueryModifier queryModifier;
    private final JdbcSplitManager jdbcSplitManager;
    private final Optional<String> unloadLocation;
    private final Optional<String> unloadAuthorization;
    private final ExecutorService executor;
    private final TrinoFileSystemFactory fileSystemFactory;

    @Inject
    public RedshiftSplitManager(
            JdbcClient jdbcClient,
            QueryBuilder queryBuilder,
            RemoteQueryModifier queryModifier,
            JdbcSplitManager jdbcSplitManager,
            RedshiftConfig redshiftConfig,
            @FileSystemS3 TrinoFileSystemFactory fileSystemFactory,
            @ForRecordCursor ExecutorService executor)
    {
        this.jdbcClient = requireNonNull(jdbcClient, "jdbcClient is null");
        this.queryBuilder = requireNonNull(queryBuilder, "queryBuilder is null");
        this.queryModifier = requireNonNull(queryModifier, "queryModifier is null");
        this.jdbcSplitManager = requireNonNull(jdbcSplitManager, "jdbcSplitManager is null");
        this.unloadLocation = redshiftConfig.getUnloadLocation();
        this.unloadAuthorization = redshiftConfig.getUnloadIamRole();
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.executor = requireNonNull(executor, "executor is null");
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorTableHandle table, DynamicFilter dynamicFilter, Constraint constraint)
    {
        if (table instanceof JdbcProcedureHandle) {
            return jdbcSplitManager.getSplits(transaction, session, table, dynamicFilter, constraint);
        }
        ConnectorSplitSource fallbackSplitSource = new FixedSplitSource(new JdbcSplit(Optional.empty()));
        if (!isUnloadEnabled(session)) {
            return fallbackSplitSource;
        }
        JdbcTableHandle jdbcTable = (JdbcTableHandle) table;
        JdbcTableHandle jdbcTableHandle = dynamicFilteringEnabled(session) ? jdbcTable.intersectedWithConstraint(dynamicFilter.getCurrentPredicate()) : jdbcTable;
        List<JdbcColumnHandle> columns = jdbcTableHandle.getColumns()
                .orElseGet(() -> jdbcClient.getColumns(
                        session,
                        jdbcTableHandle.getRequiredNamedRelation().getSchemaTableName(),
                        jdbcTableHandle.getRequiredNamedRelation().getRemoteTableName()));

        if (!isUnloadSupported(jdbcTable, columns)) {
            log.debug("Unsupported query shape detected. Falling back to using JDBC");
            return fallbackSplitSource;
        }
        return new RedshiftUnloadSplitSource(
                executor,
                session,
                jdbcClient,
                jdbcTableHandle,
                columns,
                queryBuilder,
                queryModifier,
                unloadLocation.orElseThrow(),
                unloadAuthorization,
                fileSystemFactory.create(session));
    }

    private static boolean isUnloadSupported(JdbcTableHandle table, List<JdbcColumnHandle> columns)
    {
        // Nothing to unload as there are no columns to be fetched from Redshift
        if (table.getColumns().isPresent() && table.getColumns().get().isEmpty()) {
            return false;
        }
        if (containsUnsupportedType(columns)) {
            return false;
        }
        // Unload command doesn't support limit clause. However, Trino can implement the workaround of wrapping limit query as inner query. See https://github.com/trinodb/trino/issues/24480
        if (table.getLimit().isPresent()) {
            return false;
        }
        if (containsFilterConditionOnDecimalTypeColumn(table)) {
            return false;
        }
        return true;
    }

    // Unsupported unload command data types when using Parquet output file format
    private static boolean containsUnsupportedType(List<JdbcColumnHandle> columns)
    {
        // ERROR: UNLOAD varbyte column "col_0" is only supported for TEXT/CSV.
        // ERROR: UNLOAD time without time zone column "value" is only supported for TEXT/CSV.
        return columns.stream().anyMatch(column -> column.getColumnType() instanceof TimeType || column.getColumnType() instanceof VarbinaryType);
    }

    // Redshift driver generates incorrect cast precision in select query for filter condition on decimal columns. See https://github.com/aws/amazon-redshift-jdbc-driver/issues/129
    private static boolean containsFilterConditionOnDecimalTypeColumn(JdbcTableHandle table)
    {
        if (table.getConstraint().getDomains()
                .map(domains -> domains.keySet().stream().anyMatch(column -> ((JdbcColumnHandle) column).getColumnType() instanceof DecimalType))
                .orElse(false)) {
            return true;
        }
        return table.getConstraintExpressions().stream()
                .flatMap(expression -> expression.parameters().stream())
                .anyMatch(parameter -> parameter.getType() instanceof DecimalType);
    }
}
