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
package io.trino.plugin.base.mapping;

import com.google.common.collect.Table;
import io.trino.spi.security.ConnectorIdentity;

import java.util.AbstractMap;
import java.util.Map;
import java.util.Objects;

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
    private final Map<ColumnMapping, String> fromRemoteColumn;
    private final Map<ColumnMapping, String> toRemoteColumn;
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

        fromRemoteColumn = rules.getColumns().stream()
                .map(r -> new AbstractMap.SimpleEntry<>(new ColumnMapping(r.getRemoteSchema(), r.getRemoteTable(), r.getRemoteColumn()), r.getMapping()))
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
        toRemoteColumn = rules.getColumns().stream()
                .map(r -> new AbstractMap.SimpleEntry<>(new ColumnMapping(r.getRemoteSchema(), r.getRemoteTable(), r.getMapping()), r.getRemoteColumn()))
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));

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
        ColumnMapping columnMapping = new ColumnMapping(remoteSchemaName, remoteTableName, remoteColumnName);
        String columnName = fromRemoteColumn.get(columnMapping);
        if (columnName == null) {
            columnName = delegate.fromRemoteColumnName(remoteSchemaName, remoteTableName, remoteColumnName);
        }
        return columnName;
    }

    @Override
    public String toRemoteSchemaName(RemoteIdentifiers remoteIdentifiers, ConnectorIdentity identity, String schemaName)
    {
        String remoteSchemaName = toRemoteSchema.get(schemaName);
        if (remoteSchemaName == null) {
            remoteSchemaName = delegate.toRemoteSchemaName(remoteIdentifiers, identity, schemaName);
        }
        return remoteSchemaName;
    }

    @Override
    public String toRemoteTableName(RemoteIdentifiers remoteIdentifiers, ConnectorIdentity identity, String remoteSchema, String tableName)
    {
        String remoteTableName = toRemoteTable.get(remoteSchema, tableName);
        if (remoteTableName == null) {
            remoteTableName = delegate.toRemoteTableName(remoteIdentifiers, identity, remoteSchema, tableName);
        }
        return remoteTableName;
    }

    @Override
    public String toRemoteColumnName(RemoteIdentifiers remoteIdentifiers, ConnectorIdentity identity, String remoteSchemaName, String remoteTableName, String columnName)
    {
        ColumnMapping columnMapping = new ColumnMapping(remoteSchemaName, remoteTableName, columnName);
        String remoteColumnName = toRemoteColumn.get(columnMapping);
        if (remoteColumnName == null) {
            remoteColumnName = delegate.toRemoteColumnName(remoteIdentifiers, identity, remoteSchemaName, remoteTableName, columnName);
        }
        return remoteColumnName;
    }

    private static class ColumnMapping
    {
        private final String schema;
        private final String table;
        private final String mapping;

        private ColumnMapping(String schema, String table, String mapping)
        {
            this.schema = schema;
            this.table = table;
            this.mapping = mapping;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ColumnMapping that = (ColumnMapping) o;
            return schema.equals(that.schema) && table.equals(that.table) && mapping.equals(that.mapping);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(schema, table, mapping);
        }
    }
}
