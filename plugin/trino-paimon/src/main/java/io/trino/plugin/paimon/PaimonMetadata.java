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
package io.trino.plugin.paimon;

import com.google.common.collect.ImmutableSet;
import io.trino.plugin.paimon.catalog.PaimonTrinoCatalog;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.SaveMode;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.predicate.TupleDomain;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Catalog.DatabaseNotExistException;
import org.apache.paimon.catalog.Catalog.TableNotExistException;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.utils.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static io.trino.plugin.paimon.PaimonErrorCode.PAIMON_METADATA_FETCH_FAILED;
import static io.trino.plugin.paimon.PaimonTypeUtils.toPaimonType;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.SCHEMA_NOT_FOUND;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public record PaimonMetadata(PaimonTrinoCatalog catalog)
        implements ConnectorMetadata
{
    private static final int MAX_TABLE_LENGTH = 128;

    public PaimonMetadata(PaimonTrinoCatalog catalog)
    {
        this.catalog = requireNonNull(catalog, "catalog is null");
    }

    private static Schema prepareSchema(ConnectorTableMetadata tableMetadata)
    {
        Map<String, Object> properties = new HashMap<>(tableMetadata.getProperties());
        Schema.Builder builder = Schema.newBuilder()
                .primaryKey(PaimonTableOptions.getPrimaryKeys(properties))
                .partitionKeys(PaimonTableOptions.getPartitionedKeys(properties))
                .comment(tableMetadata.getComment().orElse(null));

        for (ColumnMetadata column : tableMetadata.getColumns()) {
            builder.column(column.getName(), toPaimonType(column.getType()), column.getComment());
        }

        PaimonTableOptionUtils.buildOptions(builder, properties);

        return builder.build();
    }

    @Override
    public boolean schemaExists(ConnectorSession session, String schemaName)
    {
        try {
            catalog.getDatabase(session, schemaName);
            return true;
        }
        catch (DatabaseNotExistException e) {
            return false;
        }
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return catalog.listDatabases(session);
    }

    @Override
    public ConnectorTableHandle getTableHandle(
            ConnectorSession session,
            SchemaTableName tableName,
            Optional<ConnectorTableVersion> startVersion,
            Optional<ConnectorTableVersion> endVersion)
    {
        if (startVersion.isPresent() || endVersion.isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support versioned tables");
        }

        return getTableHandle(session, tableName, Collections.emptyMap());
    }

    public PaimonTableHandle getTableHandle(
            ConnectorSession session,
            SchemaTableName tableName,
            Map<String, String> dynamicOptions)
    {
        try {
            catalog.getTable(session, Identifier.create(tableName.getSchemaName(), tableName.getTableName()));
            return new PaimonTableHandle(tableName.getSchemaName(), tableName.getTableName(), dynamicOptions, TupleDomain.all(), ImmutableSet.of(), OptionalLong.empty());
        }
        catch (TableNotExistException e) {
            return null;
        }
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(
            ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        try {
            return ((PaimonTableHandle) tableHandle).tableMetadata(session, catalog);
        }
        catch (Exception e) {
            throw new TrinoException(PAIMON_METADATA_FETCH_FAILED, "Failed to get table metadata!", e);
        }
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        List<SchemaTableName> tables = new ArrayList<>();
        schemaName.map(Collections::singletonList)
                .orElseGet(() -> catalog.listDatabases(session))
                .forEach(schema -> tables.addAll(listTables(session, schema)));
        return tables;
    }

    private List<SchemaTableName> listTables(ConnectorSession session, String schema)
    {
        try {
            return catalog.listTables(session, schema).stream()
                    .map(table -> new SchemaTableName(schema, table))
                    .collect(toList());
        }
        catch (DatabaseNotExistException e) {
            throw new RuntimeException(e);
        }
    }

    // Do not support creating tables in trino yet, this method is only used for paimon connector testing.
    public void createPaimonTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, SaveMode saveMode)
    {
        SchemaTableName table = tableMetadata.getTable();
        Identifier identifier = Identifier.create(table.getSchemaName(), table.getTableName());

        if (identifier.getTableName().length() > MAX_TABLE_LENGTH) {
            throw new TrinoException(NOT_SUPPORTED, format("Table name must be shorter than or equal to '%s' characters but got '%s'", MAX_TABLE_LENGTH, identifier.getTableName().length()));
        }

        try {
            catalog.createTable(session, identifier, prepareSchema(tableMetadata), false);
        }
        catch (DatabaseNotExistException e) {
            throw new TrinoException(SCHEMA_NOT_FOUND, format("Schema %s not found", table.getSchemaName()));
        }
        catch (Catalog.TableAlreadyExistException e) {
            switch (saveMode) {
                case IGNORE -> {}
                case REPLACE -> {
                    try {
                        catalog.dropTable(session, identifier, false);
                        catalog.createTable(session, identifier, prepareSchema(tableMetadata), true);
                    }
                    catch (DatabaseNotExistException ex) {
                        throw new RuntimeException(format("database not existed: '%s'", table.getTableName()));
                    }
                    catch (Catalog.TableAlreadyExistException ex) {
                        throw new RuntimeException(format("table already exists: '%s'", table.getTableName()));
                    }
                    catch (TableNotExistException ex) {
                        throw new RuntimeException(format("table not exists: '%s'", table.getTableName()));
                    }
                }
                case FAIL -> throw new RuntimeException(format("table already existed: '%s'", table.getTableName()));
            }
        }
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        PaimonTableHandle table = (PaimonTableHandle) tableHandle;
        Map<String, ColumnHandle> columnHandles = new HashMap<>();
        for (ColumnMetadata column : table.columnMetadatas(session, catalog)) {
            columnHandles.put(column.getName(), table.columnHandle(session, catalog, column.getName()));
        }
        return columnHandles;
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        PaimonColumnHandle column = (PaimonColumnHandle) columnHandle;
        return new ColumnMetadata(column.columnName(), column.trinoType());
    }

    // TODO Implement streamRelationColumns method
    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");

        List<SchemaTableName> tableNames;
        if (prefix.getTable().isPresent()) {
            tableNames = Collections.singletonList(prefix.toSchemaTableName());
        }
        else {
            tableNames = listTables(session, prefix.getSchema());
        }

        return tableNames.stream()
                .map(table -> {
                    PaimonTableHandle handle;
                    List<ColumnMetadata> columnMetadata;
                    try {
                        handle = getTableHandle(session, table, Collections.emptyMap());
                        columnMetadata = handle.columnMetadatas(session, catalog);
                    }
                    catch (RuntimeException e) {
                        // Error when getting column metadata, return null
                        return Pair.of(table, (List<ColumnMetadata>) null);
                    }

                    return Pair.of(table, columnMetadata);
                }).filter(p -> p.getRight() != null)
                .collect(toMap(Pair::getLeft, Pair::getRight));
    }

    public void close()
    {
        try {
            this.catalog.close();
        }
        catch (Exception e) {
            throw new RuntimeException("Error happens while close catalog", e);
        }
    }

    public void rollback()
    {
        // do nothing
    }
}
