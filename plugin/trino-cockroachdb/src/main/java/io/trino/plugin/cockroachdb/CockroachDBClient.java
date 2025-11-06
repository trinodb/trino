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
package io.trino.plugin.cockroachdb;

import com.google.inject.Inject;
import io.airlift.slice.Slice;
import io.trino.plugin.base.mapping.IdentifierMapping;
import io.trino.plugin.base.mapping.RemoteIdentifiers;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.JdbcStatisticsConfig;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.PreparedQuery;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.RemoteTableName;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.plugin.postgresql.PostgreSqlClient;
import io.trino.plugin.postgresql.PostgreSqlConfig;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.PointerType;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.TypeManager;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;

public class CockroachDBClient
        extends PostgreSqlClient
{
    @Inject
    public CockroachDBClient(BaseJdbcConfig config, PostgreSqlConfig postgreSqlConfig, JdbcStatisticsConfig statisticsConfig, ConnectionFactory connectionFactory, QueryBuilder queryBuilder, TypeManager typeManager, IdentifierMapping identifierMapping, RemoteQueryModifier queryModifier)
    {
        super(config, postgreSqlConfig, statisticsConfig, connectionFactory, queryBuilder, typeManager, identifierMapping, queryModifier);
    }

    // Overridden to allow returning a table handle with a read timestamp
    @Override
    public Optional<JdbcTableHandle> getTableHandle(ConnectorSession session, SchemaTableName schemaTableName, Optional<ConnectorTableVersion> endVersion)
    {
        if (endVersion.isPresent() && endVersion.get().getPointerType() == PointerType.TEMPORAL) {
            throw new TrinoException(NOT_SUPPORTED, "This connector only supports reading tables AS OF VERSION, not TIMESTAMP");
        }
        if (endVersion.isPresent() && !(endVersion.get().getVersion() instanceof Slice)) {
            throw new TrinoException(NOT_SUPPORTED, "This connector only allows passing in strings as a read version");
        }
        try (Connection connection = connectionFactory.openConnection(session)) {
            ConnectorIdentity identity = session.getIdentity();
            RemoteIdentifiers remoteIdentifiers = getRemoteIdentifiers(connection);
            String remoteSchema = getIdentifierMapping().toRemoteSchemaName(remoteIdentifiers, identity, schemaTableName.getSchemaName());
            String remoteTable = getIdentifierMapping().toRemoteTableName(remoteIdentifiers, identity, remoteSchema, schemaTableName.getTableName());
            try (ResultSet resultSet = getTables(connection, Optional.of(remoteSchema), Optional.of(remoteTable))) {
                List<JdbcTableHandle> tableHandles = new ArrayList<>();
                while (resultSet.next()) {
                    tableHandles.add(new JdbcTableHandle(schemaTableName, getRemoteTable(resultSet), getTableComment(resultSet), endVersion.map(aVersion -> ((Slice) aVersion.getVersion()).toStringUtf8())));
                }
                if (tableHandles.isEmpty()) {
                    return Optional.empty();
                }
                if (tableHandles.size() > 1) {
                    throw new TrinoException(NOT_SUPPORTED, "Multiple tables matched: " + schemaTableName);
                }
                return Optional.of(getOnlyElement(tableHandles));
            }
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    public OptionalLong update(ConnectorSession session, JdbcTableHandle handle)
    {
        checkArgument(handle.isNamedRelation(), "Unable to update from synthetic table: %s", handle);
        checkArgument(handle.getLimit().isEmpty(), "Unable to update when limit is set: %s", handle);
        checkArgument(handle.getSortOrder().isEmpty(), "Unable to update when sort order is set: %s", handle);
        checkArgument(!handle.getUpdateAssignments().isEmpty(), "Unable to update when update assignments are not set: %s", handle);
        verify(handle.getAuthorization().isEmpty(), "Unexpected authorization is required for table: %s", handle);
        try (Connection connection = connectionFactory.openConnection(session)) {
            verify(connection.getAutoCommit());
            PreparedQuery preparedQuery = queryBuilder.prepareUpdateQuery(
                    this,
                    session,
                    connection,
                    handle.getRequiredNamedRelation(),
                    handle.getConstraint(),
                    getAdditionalPredicate(handle.getConstraintExpressions(), Optional.empty()),
                    handle.getUpdateAssignments());
            try (PreparedStatement preparedStatement = queryBuilder.prepareStatement(this, session, connection, preparedQuery, Optional.empty())) {
                return OptionalLong.of(preparedStatement.executeUpdate());
            }
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    public OptionalLong delete(ConnectorSession session, JdbcTableHandle handle)
    {
        checkArgument(handle.isNamedRelation(), "Unable to delete from synthetic table: %s", handle);
        checkArgument(handle.getLimit().isEmpty(), "Unable to delete when limit is set: %s", handle);
        checkArgument(handle.getSortOrder().isEmpty(), "Unable to delete when sort order is set: %s", handle);
        checkArgument(handle.getUpdateAssignments().isEmpty(), "Unable to delete when update assignments are set: %s", handle);
        verify(handle.getAuthorization().isEmpty(), "Unexpected authorization is required for table: %s", handle);
        try (Connection connection = connectionFactory.openConnection(session)) {
            verify(connection.getAutoCommit());
            PreparedQuery preparedQuery = queryBuilder.prepareDeleteQuery(
                    this,
                    session,
                    connection,
                    handle.getRequiredNamedRelation(),
                    handle.getConstraint(),
                    getAdditionalPredicate(handle.getConstraintExpressions(), Optional.empty()));
            try (PreparedStatement preparedStatement = queryBuilder.prepareStatement(this, session, connection, preparedQuery, Optional.empty())) {
                return OptionalLong.of(preparedStatement.executeUpdate());
            }
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    // Overridden to commit instantly or else cockroach returns error about versioned reads
    @Override
    public PreparedStatement getPreparedStatement(Connection connection, String sql, Optional<Integer> columnCount)
            throws SQLException
    {
        return connection.prepareStatement(sql);
    }

    @Override
    protected void verifySchemaName(DatabaseMetaData databaseMetadata, String schemaName)
    {
    }

    @Override
    protected void verifyTableName(DatabaseMetaData databaseMetadata, String tableName)
    {
    }

    @Override
    protected void verifyColumnName(DatabaseMetaData databaseMetadata, String columnName)
    {
    }

    @Override
    public OptionalInt getMaxColumnNameLength(ConnectorSession session)
    {
        return OptionalInt.empty();
    }

    @Override
    protected Set<String> hiddenColumnNames(Connection connection, RemoteTableName remoteTableName)
    {
        String catalogString = remoteTableName.getCatalogName().isPresent() ? String.format("table_catalog = '%s'", remoteTableName.getCatalogName().get()) : "table_catalog IS NULL";
        String schemaString = remoteTableName.getSchemaName().isPresent() ? String.format("table_schema = '%s'", remoteTableName.getSchemaName().get()) : "table_schema IS NULL";
        String tableString = String.format("table_name = '%s'", remoteTableName.getTableName());
        try {
            Set<String> hiddenColumns = new HashSet<>();
            ResultSet resultSet = connection.prepareStatement(String.format("SELECT * FROM information_schema.columns WHERE is_hidden = 'YES' AND %s AND %s AND %s;", catalogString, schemaString, tableString)).executeQuery();
            while (resultSet.next()) {
                hiddenColumns.add(resultSet.getString("column_name"));
            }
            return hiddenColumns;
        }
        catch (SQLException sqlException) {
            throw new RuntimeException(sqlException);
        }
    }
}
