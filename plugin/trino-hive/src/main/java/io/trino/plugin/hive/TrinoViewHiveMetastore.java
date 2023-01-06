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
package io.trino.plugin.hive;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.PrincipalPrivileges;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.connector.ViewNotFoundException;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.hive.HiveMetadata.PRESTO_VIEW_COMMENT;
import static io.trino.plugin.hive.HiveMetadata.PRESTO_VIEW_EXPANDED_TEXT_MARKER;
import static io.trino.plugin.hive.HiveMetadata.TABLE_COMMENT;
import static io.trino.plugin.hive.HiveType.HIVE_STRING;
import static io.trino.plugin.hive.TableType.VIRTUAL_VIEW;
import static io.trino.plugin.hive.TrinoViewUtil.createViewProperties;
import static io.trino.plugin.hive.ViewReaderUtil.encodeViewData;
import static io.trino.plugin.hive.ViewReaderUtil.isPrestoView;
import static io.trino.plugin.hive.metastore.MetastoreUtil.buildInitialPrivilegeSet;
import static io.trino.plugin.hive.metastore.PrincipalPrivileges.NO_PRIVILEGES;
import static io.trino.plugin.hive.metastore.StorageFormat.VIEW_STORAGE_FORMAT;
import static io.trino.plugin.hive.util.HiveUtil.isHiveSystemSchema;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static java.util.Objects.requireNonNull;

public final class TrinoViewHiveMetastore
{
    private final boolean isUsingSystemSecurity;
    private final HiveMetastore metastore;
    private final String trinoVersion;
    private final String connectorName;

    public TrinoViewHiveMetastore(HiveMetastore metastore, boolean isUsingSystemSecurity, String trinoVersion, String connectorName)
    {
        this.metastore = requireNonNull(metastore, "metastore is null");
        this.isUsingSystemSecurity = isUsingSystemSecurity;
        this.trinoVersion = requireNonNull(trinoVersion, "trinoVersion is null");
        this.connectorName = requireNonNull(connectorName, "connectorName is null");
    }

    public void createView(ConnectorSession session, SchemaTableName schemaViewName, ConnectorViewDefinition definition, boolean replace)
    {
        if (isUsingSystemSecurity) {
            definition = definition.withoutOwner();
        }

        io.trino.plugin.hive.metastore.Table.Builder tableBuilder = io.trino.plugin.hive.metastore.Table.builder()
                .setDatabaseName(schemaViewName.getSchemaName())
                .setTableName(schemaViewName.getTableName())
                .setOwner(isUsingSystemSecurity ? Optional.empty() : Optional.of(session.getUser()))
                .setTableType(VIRTUAL_VIEW.name())
                .setDataColumns(ImmutableList.of(new Column("dummy", HIVE_STRING, Optional.empty())))
                .setPartitionColumns(ImmutableList.of())
                .setParameters(createViewProperties(session, trinoVersion, connectorName))
                .setViewOriginalText(Optional.of(encodeViewData(definition)))
                .setViewExpandedText(Optional.of(PRESTO_VIEW_EXPANDED_TEXT_MARKER));

        tableBuilder.getStorageBuilder()
                .setStorageFormat(VIEW_STORAGE_FORMAT)
                .setLocation("");
        io.trino.plugin.hive.metastore.Table table = tableBuilder.build();
        PrincipalPrivileges principalPrivileges = isUsingSystemSecurity ? NO_PRIVILEGES : buildInitialPrivilegeSet(session.getUser());

        Optional<io.trino.plugin.hive.metastore.Table> existing = metastore.getTable(schemaViewName.getSchemaName(), schemaViewName.getTableName());
        if (existing.isPresent()) {
            if (!replace || !isPrestoView(existing.get())) {
                throw new ViewAlreadyExistsException(schemaViewName);
            }

            metastore.replaceTable(schemaViewName.getSchemaName(), schemaViewName.getTableName(), table, principalPrivileges);
            return;
        }

        try {
            metastore.createTable(table, principalPrivileges);
        }
        catch (TableAlreadyExistsException e) {
            throw new ViewAlreadyExistsException(e.getTableName());
        }
    }

    public void dropView(SchemaTableName schemaViewName)
    {
        if (getView(schemaViewName).isEmpty()) {
            throw new ViewNotFoundException(schemaViewName);
        }

        try {
            metastore.dropTable(schemaViewName.getSchemaName(), schemaViewName.getTableName(), true);
        }
        catch (TableNotFoundException e) {
            throw new ViewNotFoundException(e.getTableName());
        }
    }

    public List<SchemaTableName> listViews(Optional<String> database)
    {
        return listDatabases(database).stream()
                .flatMap(this::listViews)
                .collect(toImmutableList());
    }

    private List<String> listDatabases(Optional<String> database)
    {
        if (database.isPresent()) {
            if (isHiveSystemSchema(database.get())) {
                return ImmutableList.of();
            }
            return ImmutableList.of(database.get());
        }
        return metastore.getAllDatabases();
    }

    public Map<SchemaTableName, ConnectorViewDefinition> getViews(Optional<String> schemaName)
    {
        ImmutableMap.Builder<SchemaTableName, ConnectorViewDefinition> views = ImmutableMap.builder();
        for (SchemaTableName name : listViews(schemaName)) {
            try {
                getView(name).ifPresent(view -> views.put(name, view));
            }
            catch (TrinoException e) {
                if (e.getErrorCode().equals(TABLE_NOT_FOUND.toErrorCode())) {
                    // Ignore view that was dropped during query execution (race condition)
                }
                else {
                    throw e;
                }
            }
        }
        return views.buildOrThrow();
    }

    private Stream<SchemaTableName> listViews(String schema)
    {
        // Filter on PRESTO_VIEW_COMMENT to distinguish from materialized views
        return metastore.getTablesWithParameter(schema, TABLE_COMMENT, PRESTO_VIEW_COMMENT).stream()
                .map(table -> new SchemaTableName(schema, table));
    }

    public Optional<ConnectorViewDefinition> getView(SchemaTableName viewName)
    {
        if (isHiveSystemSchema(viewName.getSchemaName())) {
            return Optional.empty();
        }
        return metastore.getTable(viewName.getSchemaName(), viewName.getTableName())
                .flatMap(view -> TrinoViewUtil.getView(
                        viewName,
                        view.getViewOriginalText(),
                        view.getTableType(),
                        view.getParameters(),
                        view.getOwner()));
    }
}
