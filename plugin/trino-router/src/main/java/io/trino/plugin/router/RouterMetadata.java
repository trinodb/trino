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
package io.trino.plugin.router;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.RelationColumnsMetadata;
import io.trino.spi.connector.SchemaTableName;
import jakarta.annotation.Nullable;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.GetDatabasesRequest;
import software.amazon.awssdk.services.glue.model.GetDatabasesResponse;
import software.amazon.awssdk.services.glue.model.GetTablesRequest;
import software.amazon.awssdk.services.glue.model.GetTablesResponse;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;
import java.util.regex.Pattern;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

public class RouterMetadata
        implements ConnectorMetadata
{
    private static final Logger log = Logger.get(RouterMetadata.class);

    private final Map<String, String> schemaPrefixRules;
    private final Map<String, String> schemaPrefixGlueCatalogIds;
    private final Map<String, List<Pattern>> schemaPrefixMappedSchemas;
    private final Optional<String> defaultCatalogTarget;
    private final Optional<String> defaultCatalogId;
    private final List<Pattern> defaultMappedSchemas;
    private final GlueClient glueClient;

    @Inject
    public RouterMetadata(RouterConfig config, GlueClient glueClient)
    {
        requireNonNull(config, "config is null");
        this.schemaPrefixRules = config.getSchemaPrefixRules();
        this.schemaPrefixGlueCatalogIds = config.getSchemaPrefixGlueCatalogIds();
        this.schemaPrefixMappedSchemas = config.getSchemaPrefixMappedSchemas().entrySet().stream()
                .collect(toImmutableMap(
                        Map.Entry::getKey,
                        e -> e.getValue().stream().map(Pattern::compile).collect(toImmutableList())));
        this.defaultCatalogTarget = config.getDefaultCatalogTarget();
        this.defaultCatalogId = config.getDefaultCatalogId();
        this.defaultMappedSchemas = config.getDefaultMappedSchemas().stream()
                .map(Pattern::compile)
                .collect(toImmutableList());
        this.glueClient = requireNonNull(glueClient, "glueClient is null");

        log.info("Router catalog initialized: schemaPrefixRules=%s, schemaPrefixGlueCatalogIds=%s, schemaPrefixMappedSchemas=%s, defaultCatalogTarget=%s, defaultCatalogId=%s, defaultMappedSchemas=%s",
                schemaPrefixRules,
                schemaPrefixGlueCatalogIds,
                schemaPrefixMappedSchemas,
                defaultCatalogTarget,
                defaultCatalogId,
                defaultMappedSchemas);
    }

    private boolean schemaMatchesLocalFilter(String schemaName)
    {
        if (defaultMappedSchemas.isEmpty()) {
            return true;
        }
        return defaultMappedSchemas.stream().anyMatch(p -> p.matcher(schemaName).matches());
    }

    private boolean schemaMatchesFilter(String prefix, String targetSchema)
    {
        List<Pattern> patterns = schemaPrefixMappedSchemas.get(prefix);
        if (patterns == null || patterns.isEmpty()) {
            return true;
        }
        return patterns.stream().anyMatch(p -> p.matcher(targetSchema).matches());
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        log.debug("listSchemaNames: enumerating %s prefix rule(s); defaultCatalogTarget=%s", schemaPrefixRules.size(), defaultCatalogTarget);
        ImmutableList.Builder<String> schemas = ImmutableList.builder();

        for (Map.Entry<String, String> rule : schemaPrefixRules.entrySet()) {
            String prefix = rule.getKey();
            String glueCatalogId = schemaPrefixGlueCatalogIds.get(prefix);
            Consumer<GetDatabasesRequest.Builder> glueRequest = r -> {
                if (glueCatalogId != null) {
                    r.catalogId(glueCatalogId);
                }
            };
            List<String> glueSchemas;
            try {
                glueSchemas = glueClient.getDatabasesPaginator(glueRequest).stream()
                        .map(GetDatabasesResponse::databaseList)
                        .flatMap(List::stream)
                        .map(db -> db.name())
                        .filter(dbName -> schemaMatchesFilter(prefix, dbName))
                        .map(dbName -> prefix + dbName)
                        .toList();
            }
            catch (RuntimeException e) {
                log.error(e, "listSchemaNames: Glue getDatabases failed for prefix '%s' (glueCatalogId=%s)", prefix, glueCatalogId);
                throw e;
            }
            log.debug("listSchemaNames: prefix '%s' (glueCatalogId=%s) returned %s schema(s)", prefix, glueCatalogId, glueSchemas.size());
            schemas.addAll(glueSchemas);
        }

        if (defaultCatalogTarget.isPresent()) {
            String catalogId = defaultCatalogId.orElse(null);
            Consumer<GetDatabasesRequest.Builder> localRequest = r -> {
                if (catalogId != null) {
                    r.catalogId(catalogId);
                }
            };
            List<String> localSchemas;
            try {
                localSchemas = glueClient.getDatabasesPaginator(localRequest).stream()
                        .map(GetDatabasesResponse::databaseList)
                        .flatMap(List::stream)
                        .map(db -> db.name())
                        .filter(this::schemaMatchesLocalFilter)
                        .toList();
            }
            catch (RuntimeException e) {
                log.error(e, "listSchemaNames: Glue getDatabases failed for default catalog target '%s' (glueCatalogId=%s)", defaultCatalogTarget.get(), catalogId);
                throw e;
            }
            log.debug("listSchemaNames: default catalog target '%s' (glueCatalogId=%s) returned %s unprefixed schema(s)", defaultCatalogTarget.get(), catalogId, localSchemas.size());
            schemas.addAll(localSchemas);
        }

        List<String> result = schemas.build();
        log.debug("listSchemaNames: returning %s schema(s) total", result.size());
        return result;
    }

    @Override
    public boolean schemaExists(ConnectorSession session, String schemaName)
    {
        for (String prefix : schemaPrefixRules.keySet()) {
            if (schemaName.startsWith(prefix) && schemaName.length() > prefix.length()) {
                String targetSchema = schemaName.substring(prefix.length());
                if (schemaMatchesFilter(prefix, targetSchema)) {
                    log.debug("schemaExists: '%s' matched prefix '%s' -> true", schemaName, prefix);
                    return true;
                }
            }
        }
        if (defaultCatalogTarget.isPresent() && schemaMatchesLocalFilter(schemaName)) {
            log.debug("schemaExists: '%s' matched default catalog target '%s' -> true", schemaName, defaultCatalogTarget.get());
            return true;
        }
        log.debug("schemaExists: '%s' matched no prefix rule or default target -> false", schemaName);
        return false;
    }

    @Nullable
    @Override
    public ConnectorTableHandle getTableHandle(
            ConnectorSession session,
            SchemaTableName tableName,
            Optional<ConnectorTableVersion> startVersion,
            Optional<ConnectorTableVersion> endVersion)
    {
        return null;
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        if (schemaName.isEmpty()) {
            // Enumerating every schema would require one Glue GetTables call per schema
            // (hundreds), which is prohibitively expensive. Table enumeration is only
            // supported when scoped to a schema (e.g. SHOW TABLES IN <schema>).
            log.debug("listTables: called with no schema; skipping full enumeration");
            return ImmutableList.of();
        }
        String schema = schemaName.get();

        for (Map.Entry<String, String> rule : schemaPrefixRules.entrySet()) {
            String prefix = rule.getKey();
            if (schema.startsWith(prefix) && schema.length() > prefix.length()) {
                String targetSchema = schema.substring(prefix.length());
                if (!schemaMatchesFilter(prefix, targetSchema)) {
                    log.debug("listTables: schema '%s' filtered out for prefix '%s'", schema, prefix);
                    return ImmutableList.of();
                }
                return listGlueTables(schema, targetSchema, schemaPrefixGlueCatalogIds.get(prefix));
            }
        }

        if (defaultCatalogTarget.isPresent() && schemaMatchesLocalFilter(schema)) {
            return listGlueTables(schema, schema, defaultCatalogId.orElse(null));
        }

        log.debug("listTables: schema '%s' matched no prefix rule or default target", schema);
        return ImmutableList.of();
    }

    private List<SchemaTableName> listGlueTables(String routerSchema, String targetDatabase, @Nullable String glueCatalogId)
    {
        Consumer<GetTablesRequest.Builder> request = r -> {
            r.databaseName(targetDatabase);
            if (glueCatalogId != null) {
                r.catalogId(glueCatalogId);
            }
        };
        List<SchemaTableName> tables;
        try {
            tables = glueClient.getTablesPaginator(request).stream()
                    .map(GetTablesResponse::tableList)
                    .flatMap(List::stream)
                    .map(table -> new SchemaTableName(routerSchema, table.name()))
                    .toList();
        }
        catch (RuntimeException e) {
            log.error(e, "listTables: Glue getTables failed for schema '%s' (database '%s', glueCatalogId=%s)", routerSchema, targetDatabase, glueCatalogId);
            throw e;
        }
        log.debug("listTables: schema '%s' (database '%s', glueCatalogId=%s) returned %s table(s)", routerSchema, targetDatabase, glueCatalogId, tables.size());
        return tables;
    }

    @Override
    public Optional<CatalogSchemaTableName> redirectTable(ConnectorSession session, SchemaTableName tableName)
    {
        requireNonNull(session, "session is null");
        requireNonNull(tableName, "tableName is null");
        String schemaName = tableName.getSchemaName();

        // Never redirect the engine-provided metadata schemas. Trino implements
        // SHOW SCHEMAS/TABLES by reading <catalog>.information_schema; if redirectTable
        // sends those to the default catalog, the query returns the default catalog's
        // metadata and the router's own enumeration (listSchemaNames) is bypassed.
        if (schemaName.equals("information_schema") || schemaName.equals("system")) {
            log.debug("redirectTable: %s is a metadata schema; not redirecting", tableName);
            return Optional.empty();
        }

        for (Map.Entry<String, String> rule : schemaPrefixRules.entrySet()) {
            String prefix = rule.getKey();
            String targetCatalog = rule.getValue();
            if (schemaName.startsWith(prefix)) {
                String targetSchema = schemaName.substring(prefix.length());
                if (targetSchema.isEmpty() || !schemaMatchesFilter(prefix, targetSchema)) {
                    log.debug("redirectTable: %s matched prefix '%s' but target schema '%s' is empty or filtered out; trying next rule", tableName, prefix, targetSchema);
                    continue;
                }
                CatalogSchemaTableName target = new CatalogSchemaTableName(targetCatalog, targetSchema, tableName.getTableName());
                log.debug("redirectTable: %s redirected via prefix '%s' to %s", tableName, prefix, target);
                return Optional.of(target);
            }
        }

        if (defaultCatalogTarget.isPresent() && schemaMatchesLocalFilter(schemaName)) {
            CatalogSchemaTableName target = new CatalogSchemaTableName(defaultCatalogTarget.get(), schemaName, tableName.getTableName());
            log.debug("redirectTable: %s redirected via default catalog target to %s", tableName, target);
            return Optional.of(target);
        }

        log.debug("redirectTable: %s matched no prefix rule or default target; no redirection applied", tableName);
        return Optional.empty();
    }

    @Override
    public Iterator<RelationColumnsMetadata> streamRelationColumns(
            ConnectorSession session,
            Optional<String> schemaName,
            UnaryOperator<Set<SchemaTableName>> relationFilter)
    {
        // The router owns no local tables; all access is via redirectTable().
        return ImmutableList.<RelationColumnsMetadata>of().iterator();
    }
}
