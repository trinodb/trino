/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */

package io.trino.plugin.jdbc.mapping;

import com.google.common.collect.Table;
import io.trino.spi.security.ConnectorIdentity;

import java.sql.Connection;
import java.util.Map;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableTable.toImmutableTable;
import static java.util.Objects.requireNonNull;

public class RuleBasedIdentifierMapping
        implements IdentifierMapping
{
    private final Map<String, String> fromRemoteSchema;
    private final Map<String, String> toRemoteSchema;
    private final Table<String, String, String> fromRemoteTable;
    private final Table<String, String, String> toRemoteTable;
    private final Map<FromRemoteColumnMappingKey, String> fromRemoteColumn;
    private final Map<ToRemoteColumnMappingKey, String> toRemoteColumn;
    private final IdentifierMapping delegate;

    public RuleBasedIdentifierMapping(IdentifierMappingRules rules, IdentifierMapping delegate)
    {
        requireNonNull(rules, "rules is null");
        requireNonNull(delegate, "defaultIdentifierMapping is null");

        fromRemoteSchema = rules.getSchemaMapping().stream()
                .collect(toImmutableMap(SchemaMappingRule::getRemoteSchema, SchemaMappingRule::getMapping));
        toRemoteSchema = rules.getSchemaMapping().stream()
                .collect(toImmutableMap(SchemaMappingRule::getMapping, SchemaMappingRule::getRemoteSchema));

        fromRemoteTable = rules.getTableMapping().stream()
                .collect(toImmutableTable(
                        TableMappingRule::getRemoteSchema,
                        TableMappingRule::getRemoteTable,
                        TableMappingRule::getMapping));
        toRemoteTable = rules.getTableMapping().stream()
                .collect(toImmutableTable(
                        TableMappingRule::getRemoteSchema,
                        TableMappingRule::getMapping,
                        TableMappingRule::getRemoteTable));

        fromRemoteColumn = rules.getColumnMapping().stream()
                .collect(toImmutableMap(
                        columnMappingRule -> new FromRemoteColumnMappingKey(columnMappingRule.getRemoteSchema(), columnMappingRule.getRemoteTable(), columnMappingRule.getRemoteColumn()),
                        ColumnMappingRule::getMapping));
        toRemoteColumn = rules.getColumnMapping().stream()
                .collect(toImmutableMap(
                        columnMappingRule -> new ToRemoteColumnMappingKey(columnMappingRule.getRemoteSchema(), columnMappingRule.getRemoteTable(), columnMappingRule.getMapping()),
                        ColumnMappingRule::getRemoteColumn));

        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public String fromRemoteSchemaName(String remoteSchemaName)
    {
        String schemaName = fromRemoteSchema.get(remoteSchemaName);
        if (schemaName == null) {
            schemaName = delegate.fromRemoteSchemaName(remoteSchemaName);
        }
        return schemaName;
    }

    @Override
    public String fromRemoteTableName(String remoteSchemaName, String remoteTableName)
    {
        String tableName = fromRemoteTable.get(remoteSchemaName, remoteTableName);
        if (tableName == null) {
            tableName = delegate.fromRemoteTableName(remoteSchemaName, remoteTableName);
        }
        return tableName;
    }

    @Override
    public String fromRemoteColumnName(String remoteSchemaName, String remoteTableName, String remoteColumnName)
    {
        String columnName = fromRemoteColumn.get(new FromRemoteColumnMappingKey(remoteSchemaName, remoteTableName, remoteColumnName));
        if (columnName == null) {
            columnName = delegate.fromRemoteColumnName(remoteSchemaName, remoteTableName, remoteColumnName);
        }
        return columnName;
    }

    @Override
    public String toRemoteSchemaName(ConnectorIdentity identity, Connection connection, String schemaName)
    {
        String remoteSchemaName = toRemoteSchema.get(schemaName);
        if (remoteSchemaName == null) {
            remoteSchemaName = delegate.toRemoteSchemaName(identity, connection, schemaName);
        }
        return remoteSchemaName;
    }

    @Override
    public String toRemoteTableName(ConnectorIdentity identity, Connection connection, String remoteSchema, String tableName)
    {
        String remoteTableName = toRemoteTable.get(remoteSchema, tableName);
        if (remoteTableName == null) {
            remoteTableName = delegate.toRemoteTableName(identity, connection, remoteSchema, tableName);
        }
        return remoteTableName;
    }

    @Override
    public String toRemoteColumnName(Connection connection, String remoteSchema, String remoteTableName, String columnName)
    {
        String remoteColumnName = toRemoteColumn.get(new ToRemoteColumnMappingKey(remoteSchema, remoteTableName, columnName));
        if (remoteColumnName == null) {
            remoteColumnName = delegate.toRemoteColumnName(connection, remoteSchema, remoteTableName, columnName);
        }
        return remoteColumnName;
    }
}
