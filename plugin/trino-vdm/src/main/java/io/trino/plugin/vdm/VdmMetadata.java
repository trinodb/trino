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
package io.trino.plugin.vdm;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.connector.ViewNotFoundException;
import io.trino.spi.metastore.HetuMetastore;
import io.trino.spi.metastore.model.CatalogEntity;
import io.trino.spi.metastore.model.ColumnEntity;
import io.trino.spi.metastore.model.DatabaseEntity;
import io.trino.spi.metastore.model.TableEntity;
import io.trino.spi.security.TrinoPrincipal;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.vdm.utils.VdmUtil.HETU_QUERY_ID_NAME;
import static io.trino.plugin.vdm.utils.VdmUtil.HETU_VERSION_NAME;
import static io.trino.plugin.vdm.utils.VdmUtil.HETU_VIEW_FLAG;
import static io.trino.plugin.vdm.utils.VdmUtil.decodeViewData;
import static io.trino.plugin.vdm.utils.VdmUtil.encodeViewData;
import static io.trino.plugin.vdm.utils.VdmUtil.isHetuView;
import static io.trino.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.NOT_FOUND;
import static io.trino.spi.StandardErrorCode.SCHEMA_NOT_EMPTY;
import static io.trino.spi.metastore.model.TableEntityType.VIRTUAL_VIEW;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * vdm metadata
 *
 * @since 2023-04-06
 */
public class VdmMetadata
        implements ConnectorMetadata
{
    private final HetuMetastore metastore;
    private final String vdmName;
    private final String catalogType;
    private final String version;

    /**
     * vdm metadata
     *
     * @param vdmName vdm name
     * @param metastore vdm metastore
     * @param version version
     */
    public VdmMetadata(VdmName vdmName, HetuMetastore metastore, String version)
    {
        this.vdmName = requireNonNull(vdmName.getVdmName(), "vdm name is null");
        this.catalogType = requireNonNull(vdmName.getCatalogType(), "catalog type is null");
        this.metastore = requireNonNull(metastore, "metastore is null");
        this.version = requireNonNull(version, "version is null");
    }

    @Override
    public synchronized void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties, TrinoPrincipal owner)
    {
        Optional<CatalogEntity> oldCatalog = metastore.getCatalog(vdmName);
        if (!oldCatalog.isPresent()) {
            CatalogEntity newCatalog = CatalogEntity.builder()
                    .setCatalogName(vdmName)
                    .setComment(Optional.of("Hetu vdm catalog."))
                    .setOwner(session.getUser())
                    .setCreateTime(session.getStart().toEpochMilli())
                    .setParameters(ImmutableMap.<String, String>builder().put("connector.name", catalogType).build())
                    .build();
            metastore.createCatalog(newCatalog);
        }

        DatabaseEntity.Builder databaseBuilder = DatabaseEntity.builder()
                .setCatalogName(vdmName)
                .setDatabaseName(schemaName)
                .setCreateTime(session.getStart().toEpochMilli())
                .setOwner(session.getUser())
                .setComment(Optional.of("Hetu schema."));
        metastore.createDatabase(databaseBuilder.build());
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName, boolean cascade)
    {
        if (!listViews(session, Optional.of(schemaName)).isEmpty()) {
            throw new TrinoException(SCHEMA_NOT_EMPTY, "Schema not empty: " + schemaName);
        }
        metastore.dropDatabase(vdmName, schemaName);
    }

    @Override
    public void renameSchema(ConnectorSession session, String source, String target)
    {
        Optional<DatabaseEntity> oldDatabase = metastore.getDatabase(vdmName, source);

        if (!oldDatabase.isPresent()) {
            throw new TrinoException(NOT_FOUND, format("Schema '%s' not exists.", source));
        }

        metastore.alterDatabase(vdmName, source,
                DatabaseEntity.builder(oldDatabase.get()).setDatabaseName(target).build());
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        ImmutableList.Builder<String> schemaNames = ImmutableList.builder();
        schemaNames.addAll(metastore.getAllDatabases(vdmName)
                .stream().map(DatabaseEntity::getName).collect(toImmutableList()));
        return schemaNames.build();
    }

    @Override
    public void createView(ConnectorSession session, SchemaTableName viewName, ConnectorViewDefinition definition,
            Map<String, Object> viewProperties, boolean replace)
    {
        TableEntity view = createViewInfo(session, viewName, definition);

        Optional<TableEntity> oldView = metastore.getTable(vdmName, viewName.getSchemaName(),
                viewName.getTableName());
        if (oldView.isPresent()) {
            if (!replace) {
                throw new TrinoException(ALREADY_EXISTS, format("View already exists: '%s'", viewName));
            }
            view.setOwner(oldView.get().getOwner());
            metastore.alterTable(vdmName, viewName.getSchemaName(), viewName.getTableName(), view);
            return;
        }

        metastore.createTable(view);
    }

    private TableEntity createViewInfo(ConnectorSession session, SchemaTableName viewName,
            ConnectorViewDefinition definition)
    {
        // property info
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put(HETU_VIEW_FLAG, "true")
                .put(HETU_VERSION_NAME, version)
                .put(HETU_QUERY_ID_NAME, session.getQueryId())
                .build();

        // column info
        List<ColumnEntity> columns = definition
                .getColumns()
                .stream()
                .map(viewColumn -> new ColumnEntity(viewColumn.getName(),
                        viewColumn.getType().toString(), "Hetu view column", null))
                .collect(toImmutableList());

        return TableEntity.builder()
                .setCatalogName(vdmName)
                .setDatabaseName(viewName.getSchemaName())
                .setTableName(viewName.getTableName())
                .setViewOriginalText(Optional.of(encodeViewData(definition)))
                .setCreateTime(session.getStart().toEpochMilli())
                .setOwner(session.getUser())
                .setTableType(VIRTUAL_VIEW.toString())
                .setComment("Hetu View")
                .setColumns(columns)
                .setParameters(properties)
                .build();
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        try {
            metastore.dropTable(vdmName, viewName.getSchemaName(), viewName.getTableName());
        }
        catch (TableNotFoundException e) {
            throw new ViewNotFoundException(e.getTableName());
        }
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, Optional<String> schemaName)
    {
        ImmutableList.Builder<SchemaTableName> tableNames = ImmutableList.builder();
        List<String> schemaNames = schemaName.<List<String>>map(ImmutableList::of)
                .orElseGet(() -> listSchemaNames(session));
        for (String schema : schemaNames) {
            List<TableEntity> views = metastore.getAllTables(vdmName, schema);
            for (TableEntity view : views) {
                tableNames.add(new SchemaTableName(view.getDatabaseName(), view.getName()));
            }
        }

        return tableNames.build();
    }

    @Override
    public Optional<ConnectorViewDefinition> getView(ConnectorSession session, SchemaTableName viewName)
    {
        Optional<TableEntity> viewEntity = metastore.getTable(vdmName, viewName.getSchemaName(),
                viewName.getTableName());
        if (viewEntity.isPresent() && isHetuView(viewEntity.get().getParameters().get(HETU_VIEW_FLAG))) {
            TableEntity view = viewEntity.get();
            ConnectorViewDefinition definition = decodeViewData(view.getViewOriginalText());
            // use owner from view metadata if it exists
            if (view.getOwner() != null && !definition.isRunAsInvoker()) {
                definition = new ConnectorViewDefinition(
                        definition.getOriginalSql(),
                        definition.getCatalog(),
                        definition.getSchema(),
                        definition.getColumns(),
                        definition.getComment(),
                        Optional.of(view.getOwner()),
                        false,
                        definition.getPath());
            }
            return Optional.of(definition);
        }

        return Optional.empty();
    }
}
