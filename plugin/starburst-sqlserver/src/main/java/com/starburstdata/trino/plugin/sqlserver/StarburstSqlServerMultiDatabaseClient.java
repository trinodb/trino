/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.sqlserver;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.trino.plugin.base.mapping.IdentifierMapping;
import io.trino.plugin.base.mapping.RemoteIdentifiers;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcOutputTableHandle;
import io.trino.plugin.jdbc.JdbcStatisticsConfig;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.ConnectorIdentity;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class StarburstSqlServerMultiDatabaseClient
        extends StarburstSqlServerClient
{
    @VisibleForTesting
    static final String DATABASE_SEPARATOR = ".";

    private static final Splitter DATABASE_SPLITTER = Splitter.on(DATABASE_SEPARATOR);
    private final IdentifierMapping identifierMapping;

    @Inject
    public StarburstSqlServerMultiDatabaseClient(
            BaseJdbcConfig config,
            JdbcStatisticsConfig statisticsConfig,
            ConnectionFactory connectionFactory,
            QueryBuilder queryBuilder,
            IdentifierMapping identifierMapping,
            RemoteQueryModifier queryModifier)
    {
        super(config, statisticsConfig, connectionFactory, queryBuilder, identifierMapping, queryModifier);
        this.identifierMapping = requireNonNull(identifierMapping, "identifierMapping is null");
    }

    @Override
    public Collection<String> listSchemas(Connection connection)
    {
        Collection<String> catalogNames = listCatalogs(connection);
        ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
        for (String catalogName : catalogNames) {
            try (ResultSet resultSet = connection.getMetaData().getSchemas(catalogName, null)) {
                while (resultSet.next()) {
                    String schemaName = resultSet.getString("TABLE_SCHEM");
                    // skip internal schemas
                    if (filterSchema(schemaName)) {
                        schemaNames.add(format("%s%s%s", catalogName, DATABASE_SEPARATOR, schemaName));
                    }
                }
            }
            catch (SQLException e) {
                throw new TrinoException(JDBC_ERROR, e);
            }
        }
        return schemaNames.build();
    }

    private Collection<String> listCatalogs(Connection connection)
    {
        try (ResultSet resultSet = connection.getMetaData().getCatalogs()) {
            ImmutableSet.Builder<String> catalogNames = ImmutableSet.builder();
            while (resultSet.next()) {
                String catalogName = resultSet.getString("TABLE_CAT");
                catalogNames.add(catalogName);
            }
            return catalogNames.build();
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    protected void createSchema(ConnectorSession session, Connection connection, String remoteSchemaName)
            throws SQLException
    {
        DatabaseSchemaName databaseSchema = parseDatabaseSchemaName(remoteSchemaName);
        connection.setCatalog(databaseSchema.databaseName);
        super.createSchema(session, connection, databaseSchema.schemaName);
    }

    @Override
    protected void dropSchema(ConnectorSession session, Connection connection, String remoteSchemaName, boolean cascade)
            throws SQLException
    {
        DatabaseSchemaName databaseSchema = parseDatabaseSchemaName(remoteSchemaName);
        connection.setCatalog(databaseSchema.databaseName);
        super.dropSchema(session, connection, databaseSchema.schemaName, cascade);
    }

    @Override
    protected JdbcOutputTableHandle createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, String targetTableName, Optional<ColumnMetadata> pageSinkIdColumn)
            throws SQLException
    {
        ConnectorIdentity identity = session.getIdentity();
        SchemaTableName schemaTableName = tableMetadata.getTable();
        DatabaseSchemaName databaseSchema = parseDatabaseSchemaName(schemaTableName.getSchemaName());

        if (!getSchemaNames(session).contains(schemaTableName.getSchemaName())) {
            throw new SchemaNotFoundException(schemaTableName.getSchemaName());
        }

        try (Connection connection = connectionFactory.openConnection(session)) {
            connection.setCatalog(databaseSchema.databaseName);
            verify(connection.getAutoCommit());
            RemoteIdentifiers remoteIdentifiers = getRemoteIdentifiers(connection);
            String remoteSchema = identifierMapping.toRemoteSchemaName(remoteIdentifiers, identity, databaseSchema.schemaName);
            String remoteTable = identifierMapping.toRemoteTableName(remoteIdentifiers, identity, remoteSchema, schemaTableName.getTableName());
            String remoteTargetTableName = identifierMapping.toRemoteTableName(remoteIdentifiers, identity, remoteSchema, targetTableName);
            String catalog = connection.getCatalog();

            verifyTableName(connection.getMetaData(), remoteTargetTableName);

            return createTable(
                    session,
                    connection,
                    tableMetadata,
                    remoteIdentifiers,
                    catalog,
                    remoteSchema,
                    remoteTable,
                    remoteTargetTableName,
                    pageSinkIdColumn);
        }
    }

    @Override
    public JdbcOutputTableHandle beginInsertTable(ConnectorSession session, JdbcTableHandle tableHandle, List<JdbcColumnHandle> columns)
    {
        SchemaTableName schemaTableName = tableHandle.asPlainTable().getSchemaTableName();
        DatabaseSchemaName databaseSchema = parseDatabaseSchemaName(schemaTableName.getSchemaName());

        ConnectorIdentity identity = session.getIdentity();
        verify(tableHandle.getAuthorization().isEmpty(), "Unexpected authorization is required for table: %s".formatted(tableHandle));
        try (Connection connection = connectionFactory.openConnection(session)) {
            connection.setCatalog(databaseSchema.databaseName);
            verify(connection.getAutoCommit());
            RemoteIdentifiers remoteIdentifiers = getRemoteIdentifiers(connection);
            String remoteSchema = identifierMapping.toRemoteSchemaName(remoteIdentifiers, identity, databaseSchema.schemaName);
            String remoteTable = identifierMapping.toRemoteTableName(remoteIdentifiers, identity, remoteSchema, schemaTableName.getTableName());
            String catalog = connection.getCatalog();

            JdbcOutputTableHandle table = beginInsertTable(
                    session,
                    connection,
                    remoteIdentifiers,
                    catalog,
                    remoteSchema,
                    remoteTable,
                    columns);
            this.enableTableLockOnBulkLoadTableOption(session, table);
            return table;
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    protected void renameTable(
            ConnectorSession session,
            Connection connection,
            String catalogName,
            String remoteSchemaName,
            String remoteTableName,
            String newRemoteSchemaName,
            String newRemoteTableName)
            throws SQLException
    {
        DatabaseSchemaName newDatabaseSchema = parseDatabaseSchemaName(newRemoteSchemaName);
        if (!catalogName.equals(newDatabaseSchema.databaseName)) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support renaming tables across catalogs");
        }
        super.renameTable(
                session,
                connection,
                catalogName,
                remoteSchemaName,
                remoteTableName,
                newDatabaseSchema.schemaName,
                newRemoteTableName);
    }

    @Override
    public ResultSet getTables(Connection connection, Optional<String> remoteSchemaName, Optional<String> remoteTableName)
            throws SQLException
    {
        Optional<DatabaseSchemaName> databaseSchema = remoteSchemaName.map(StarburstSqlServerMultiDatabaseClient::parseDatabaseSchemaName);

        DatabaseMetaData metadata = connection.getMetaData();

        return metadata.getTables(
                databaseSchema.map(DatabaseSchemaName::databaseName).orElse(null),
                escapeObjectNameForMetadataQuery(databaseSchema.map(DatabaseSchemaName::schemaName), metadata.getSearchStringEscape()).orElse(null),
                escapeObjectNameForMetadataQuery(remoteTableName, metadata.getSearchStringEscape()).orElse(null),
                getTableTypes().map(types -> types.toArray(String[]::new)).orElse(null));
    }

    @Override
    public String getTableSchemaName(ResultSet resultSet)
            throws SQLException
    {
        return resultSet.getString("TABLE_CAT") + DATABASE_SEPARATOR + resultSet.getString("TABLE_SCHEM");
    }

    private static DatabaseSchemaName parseDatabaseSchemaName(String schemaName)
    {
        List<String> databaseSchemaName = DATABASE_SPLITTER.splitToList(schemaName);
        if (databaseSchemaName.size() < 2) {
            throw new TrinoException(INVALID_ARGUMENTS, "The expected format is '<database name>.<schema name>': " + schemaName);
        }
        if (databaseSchemaName.size() > 2) {
            throw new TrinoException(INVALID_ARGUMENTS, "Too many identifier parts found");
        }
        return new DatabaseSchemaName(databaseSchemaName.get(0), databaseSchemaName.get(1));
    }

    record DatabaseSchemaName(String databaseName, String schemaName) {}
}
