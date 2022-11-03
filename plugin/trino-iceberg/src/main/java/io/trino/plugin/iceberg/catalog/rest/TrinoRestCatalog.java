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
package io.trino.plugin.iceberg.catalog.rest;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.jsonwebtoken.impl.DefaultJwtBuilder;
import io.jsonwebtoken.jackson.io.JacksonSerializer;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.iceberg.ColumnIdentity;
import io.trino.plugin.iceberg.IcebergSchemaProperties;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.catalog.rest.IcebergRestCatalogConfig.SessionType;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.security.TrinoPrincipal;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.catalog.SessionCatalog.SessionContext;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.RESTException;
import org.apache.iceberg.rest.RESTSessionCatalog;
import org.apache.iceberg.rest.auth.OAuth2Properties;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.hive.HiveMetadata.TABLE_COMMENT;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_CATALOG_ERROR;
import static io.trino.plugin.iceberg.IcebergUtil.quotedTableName;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;

public class TrinoRestCatalog
        implements TrinoCatalog
{
    private final RESTSessionCatalog restSessionCatalog;
    private final CatalogName catalogName;
    private final SessionType sessionType;
    private final String trinoVersion;
    private final boolean useUniqueTableLocation;

    private final Map<String, Table> tableCache = new ConcurrentHashMap<>();

    public TrinoRestCatalog(
            RESTSessionCatalog restSessionCatalog,
            CatalogName catalogName,
            SessionType sessionType,
            String trinoVersion,
            boolean useUniqueTableLocation)
    {
        this.restSessionCatalog = requireNonNull(restSessionCatalog, "restSessionCatalog is null");
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.sessionType = requireNonNull(sessionType, "sessionType is null");
        this.trinoVersion = requireNonNull(trinoVersion, "trinoVersion is null");
        this.useUniqueTableLocation = useUniqueTableLocation;
    }

    @Override
    public boolean namespaceExists(ConnectorSession session, String namespace)
    {
        return restSessionCatalog.namespaceExists(convert(session), Namespace.of(namespace));
    }

    @Override
    public List<String> listNamespaces(ConnectorSession session)
    {
        return restSessionCatalog.listNamespaces(convert(session)).stream()
                .map(Namespace::toString)
                .collect(toImmutableList());
    }

    @Override
    public void dropNamespace(ConnectorSession session, String namespace)
    {
        try {
            restSessionCatalog.dropNamespace(convert(session), Namespace.of(namespace));
        }
        catch (NoSuchNamespaceException e) {
            throw new SchemaNotFoundException(namespace);
        }
    }

    @Override
    public Map<String, Object> loadNamespaceMetadata(ConnectorSession session, String namespace)
    {
        try {
            // Return immutable metadata as direct modifications will not be reflected on the namespace
            return ImmutableMap.copyOf(restSessionCatalog.loadNamespaceMetadata(convert(session), Namespace.of(namespace)));
        }
        catch (NoSuchNamespaceException e) {
            throw new SchemaNotFoundException(namespace);
        }
    }

    @Override
    public Optional<TrinoPrincipal> getNamespacePrincipal(ConnectorSession session, String namespace)
    {
        // the REST specification currently does not have a way of defining ownership
        return Optional.empty();
    }

    @Override
    public void createNamespace(ConnectorSession session, String namespace, Map<String, Object> properties, TrinoPrincipal owner)
    {
        restSessionCatalog.createNamespace(
                convert(session),
                Namespace.of(namespace),
                Maps.transformValues(properties, property -> {
                    if (property instanceof String stringProperty) {
                        return stringProperty;
                    }
                    throw new TrinoException(NOT_SUPPORTED, "Non-string properties are not support for Iceberg REST catalog");
                }));
    }

    @Override
    public void setNamespacePrincipal(ConnectorSession session, String namespace, TrinoPrincipal principal)
    {
        throw new TrinoException(NOT_SUPPORTED, "setNamespacePrincipal is not supported for Iceberg REST catalog");
    }

    @Override
    public void renameNamespace(ConnectorSession session, String source, String target)
    {
        throw new TrinoException(NOT_SUPPORTED, "renameNamespace is not supported for Iceberg REST catalog");
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> namespace)
    {
        SessionContext sessionContext = convert(session);
        List<Namespace> namespaces;

        if (namespace.isPresent() && namespaceExists(session, namespace.get())) {
            namespaces = ImmutableList.of(Namespace.of(namespace.get()));
        }
        else {
            namespaces = listNamespaces(session).stream()
                    .map(Namespace::of)
                    .collect(toImmutableList());
        }

        ImmutableList.Builder<SchemaTableName> tables = ImmutableList.builder();
        for (Namespace restNamespace : namespaces) {
            try {
                tables.addAll(
                        restSessionCatalog.listTables(sessionContext, restNamespace).stream()
                                .map(id -> SchemaTableName.schemaTableName(id.namespace().toString(), id.name()))
                                .collect(toImmutableList()));
            }
            catch (NoSuchNamespaceException e) {
                // Namespace may have been deleted during listing
            }
        }
        return tables.build();
    }

    @Override
    public Transaction newCreateTableTransaction(
            ConnectorSession session,
            SchemaTableName schemaTableName,
            Schema schema,
            PartitionSpec partitionSpec,
            SortOrder sortOrder,
            String location,
            Map<String, String> properties)
    {
        return restSessionCatalog.buildTable(convert(session), toIdentifier(schemaTableName), schema)
                .withPartitionSpec(partitionSpec)
                .withSortOrder(sortOrder)
                .withLocation(location)
                .withProperties(properties)
                .createTransaction();
    }

    @Override
    public void registerTable(ConnectorSession session, SchemaTableName tableName, String tableLocation, String metadataLocation)
    {
        throw new TrinoException(NOT_SUPPORTED, "registerTable is not supported for Iceberg REST catalog");
    }

    @Override
    public void unregisterTable(ConnectorSession session, SchemaTableName tableName)
    {
        throw new TrinoException(NOT_SUPPORTED, "unregisterTable is not supported for Iceberg REST catalogs");
    }

    @Override
    public void dropTable(ConnectorSession session, SchemaTableName schemaTableName)
    {
        if (!restSessionCatalog.purgeTable(convert(session), toIdentifier(schemaTableName))) {
            throw new TrinoException(ICEBERG_CATALOG_ERROR, format("Failed to drop table: %s", schemaTableName));
        }
    }

    @Override
    public void renameTable(ConnectorSession session, SchemaTableName from, SchemaTableName to)
    {
        try {
            restSessionCatalog.renameTable(convert(session), toIdentifier(from), toIdentifier(to));
        }
        catch (RESTException e) {
            throw new TrinoException(ICEBERG_CATALOG_ERROR, format("Failed to rename table %s to %s", from, to), e);
        }
    }

    @Override
    public Table loadTable(ConnectorSession session, SchemaTableName schemaTableName)
    {
        try {
            return tableCache.computeIfAbsent(
                    schemaTableName.toString(),
                    key -> {
                        BaseTable baseTable = (BaseTable) restSessionCatalog.loadTable(convert(session), toIdentifier(schemaTableName));
                        // Creating a new base table is necessary to adhere to Trino's expectations for quoted table names
                        return new BaseTable(baseTable.operations(), quotedTableName(schemaTableName));
                    });
        }
        catch (NoSuchTableException e) {
            throw new TableNotFoundException(schemaTableName, e);
        }
        catch (RuntimeException e) {
            throw new TrinoException(ICEBERG_CATALOG_ERROR, format("Failed to load table: %s", schemaTableName), e);
        }
    }

    @Override
    public void updateTableComment(ConnectorSession session, SchemaTableName schemaTableName, Optional<String> comment)
    {
        Table icebergTable = restSessionCatalog.loadTable(convert(session), toIdentifier(schemaTableName));
        if (comment.isEmpty()) {
            icebergTable.updateProperties().remove(TABLE_COMMENT).commit();
        }
        else {
            icebergTable.updateProperties().set(TABLE_COMMENT, comment.get()).commit();
        }
    }

    @Override
    public String defaultTableLocation(ConnectorSession session, SchemaTableName schemaTableName)
    {
        String tableName = createLocationForTable(schemaTableName.getTableName());

        Map<String, Object> properties = loadNamespaceMetadata(session, schemaTableName.getSchemaName());
        String databaseLocation = (String) properties.get(IcebergSchemaProperties.LOCATION_PROPERTY);
        checkArgument(databaseLocation != null, "location must be set for %s", schemaTableName.getSchemaName());

        if (databaseLocation.endsWith("/")) {
            return databaseLocation + tableName;
        }
        return join("/", databaseLocation, tableName);
    }

    private String createLocationForTable(String baseTableName)
    {
        String tableName = baseTableName;
        if (useUniqueTableLocation) {
            tableName += "-" + randomUUID().toString().replace("-", "");
        }
        return tableName;
    }

    @Override
    public void setTablePrincipal(ConnectorSession session, SchemaTableName schemaTableName, TrinoPrincipal principal)
    {
        throw new TrinoException(NOT_SUPPORTED, "setTablePrincipal is not supported for Iceberg REST catalog");
    }

    @Override
    public void createView(ConnectorSession session, SchemaTableName schemaViewName, ConnectorViewDefinition definition, boolean replace)
    {
        throw new TrinoException(NOT_SUPPORTED, "createView is not supported for Iceberg REST catalog");
    }

    @Override
    public void renameView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
    {
        throw new TrinoException(NOT_SUPPORTED, "renameView is not supported for Iceberg REST catalog");
    }

    @Override
    public void setViewPrincipal(ConnectorSession session, SchemaTableName schemaViewName, TrinoPrincipal principal)
    {
        throw new TrinoException(NOT_SUPPORTED, "setViewPrincipal is not supported for Iceberg REST catalog");
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName schemaViewName)
    {
        throw new TrinoException(NOT_SUPPORTED, "dropView is not supported for Iceberg REST catalog");
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, Optional<String> namespace)
    {
        return ImmutableList.of();
    }

    @Override
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, Optional<String> namespace)
    {
        return ImmutableMap.of();
    }

    @Override
    public Optional<ConnectorViewDefinition> getView(ConnectorSession session, SchemaTableName viewName)
    {
        return Optional.empty();
    }

    @Override
    public List<SchemaTableName> listMaterializedViews(ConnectorSession session, Optional<String> namespace)
    {
        return ImmutableList.of();
    }

    @Override
    public void createMaterializedView(ConnectorSession session, SchemaTableName viewName, ConnectorMaterializedViewDefinition definition, boolean replace, boolean ignoreExisting)
    {
        throw new TrinoException(NOT_SUPPORTED, "createMaterializedView is not supported for Iceberg REST catalog");
    }

    @Override
    public void dropMaterializedView(ConnectorSession session, SchemaTableName viewName)
    {
        throw new TrinoException(NOT_SUPPORTED, "dropMaterializedView is not supported for Iceberg REST catalog");
    }

    @Override
    public Optional<ConnectorMaterializedViewDefinition> getMaterializedView(ConnectorSession session, SchemaTableName viewName)
    {
        return Optional.empty();
    }

    @Override
    public void renameMaterializedView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
    {
        throw new TrinoException(NOT_SUPPORTED, "renameMaterializedView is not supported for Iceberg REST catalog");
    }

    @Override
    public void updateColumnComment(ConnectorSession session, SchemaTableName schemaTableName, ColumnIdentity columnIdentity, Optional<String> comment)
    {
        loadTable(session, schemaTableName).updateSchema()
                .updateColumnDoc(columnIdentity.getName(), comment.orElse(null))
                .commit();
    }

    @Override
    public Optional<CatalogSchemaTableName> redirectTable(ConnectorSession session, SchemaTableName tableName)
    {
        return Optional.empty();
    }

    @Override
    public void updateViewComment(ConnectorSession session, SchemaTableName schemaViewName, Optional<String> comment)
    {
        throw new TrinoException(NOT_SUPPORTED, "updateViewComment is not supported for Iceberg REST catalog");
    }

    @Override
    public void updateViewColumnComment(ConnectorSession session, SchemaTableName schemaViewName, String columnName, Optional<String> comment)
    {
        throw new TrinoException(NOT_SUPPORTED, "updateViewColumnComment is not supported for Iceberg REST catalog");
    }

    private SessionCatalog.SessionContext convert(ConnectorSession session)
    {
        return switch (sessionType) {
            case NONE -> SessionCatalog.SessionContext.createEmpty();
            case USER -> {
                String sessionId = format("%s-%s", session.getUser(), session.getSource().orElse("default"));

                Map<String, String> properties = ImmutableMap.of(
                        "user", session.getUser(),
                        "source", session.getSource().orElse(""),
                        "trinoCatalog", catalogName.toString(),
                        "trinoVersion", trinoVersion);

                Map<String, Object> claims = ImmutableMap.<String, Object>builder()
                        .putAll(properties)
                        .buildOrThrow();

                String subjectJwt = new DefaultJwtBuilder()
                        .setSubject(session.getUser())
                        .setIssuer(trinoVersion)
                        .setIssuedAt(new Date())
                        .addClaims(claims)
                        .serializeToJsonWith(new JacksonSerializer<>())
                        .compact();

                Map<String, String> credentials = ImmutableMap.<String, String>builder()
                        .putAll(session.getIdentity().getExtraCredentials())
                        .put(OAuth2Properties.JWT_TOKEN_TYPE, subjectJwt)
                        .buildOrThrow();

                yield new SessionCatalog.SessionContext(sessionId, session.getUser(), credentials, properties);
            }
        };
    }

    private static TableIdentifier toIdentifier(SchemaTableName schemaTableName)
    {
        return TableIdentifier.of(schemaTableName.getSchemaName(), schemaTableName.getTableName());
    }
}
