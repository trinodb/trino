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
        ImmutableList.Builder<String> schemas = ImmutableList.builder();

        for (Map.Entry<String, String> rule : schemaPrefixRules.entrySet()) {
            String prefix = rule.getKey();
            String glueCatalogId = schemaPrefixGlueCatalogIds.get(prefix);
            Consumer<GetDatabasesRequest.Builder> glueRequest = r -> {
                if (glueCatalogId != null) {
                    r.catalogId(glueCatalogId);
                }
            };
            List<String> glueSchemas = glueClient.getDatabasesPaginator(glueRequest).stream()
                    .map(GetDatabasesResponse::databaseList)
                    .flatMap(List::stream)
                    .map(db -> db.name())
                    .filter(dbName -> schemaMatchesFilter(prefix, dbName))
                    .map(dbName -> prefix + dbName)
                    .toList();
            schemas.addAll(glueSchemas);
        }

        if (defaultCatalogTarget.isPresent()) {
            String catalogId = defaultCatalogId.orElse(null);
            Consumer<GetDatabasesRequest.Builder> localRequest = r -> {
                if (catalogId != null) {
                    r.catalogId(catalogId);
                }
            };
            List<String> localSchemas = glueClient.getDatabasesPaginator(localRequest).stream()
                    .map(GetDatabasesResponse::databaseList)
                    .flatMap(List::stream)
                    .map(db -> db.name())
                    .filter(this::schemaMatchesLocalFilter)
                    .toList();
            schemas.addAll(localSchemas);
        }

        return schemas.build();
    }

    @Override
    public boolean schemaExists(ConnectorSession session, String schemaName)
    {
        for (String prefix : schemaPrefixRules.keySet()) {
            if (schemaName.startsWith(prefix) && schemaName.length() > prefix.length()) {
                String targetSchema = schemaName.substring(prefix.length());
                if (schemaMatchesFilter(prefix, targetSchema)) {
                    return true;
                }
            }
        }
        if (defaultCatalogTarget.isPresent() && schemaMatchesLocalFilter(schemaName)) {
            return true;
        }
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
        return ImmutableList.of();
    }

    @Override
    public Optional<CatalogSchemaTableName> redirectTable(ConnectorSession session, SchemaTableName tableName)
    {
        requireNonNull(session, "session is null");
        requireNonNull(tableName, "tableName is null");
        String schemaName = tableName.getSchemaName();

        for (Map.Entry<String, String> rule : schemaPrefixRules.entrySet()) {
            String prefix = rule.getKey();
            String targetCatalog = rule.getValue();
            if (schemaName.startsWith(prefix)) {
                String targetSchema = schemaName.substring(prefix.length());
                if (targetSchema.isEmpty() || !schemaMatchesFilter(prefix, targetSchema)) {
                    continue;
                }
                return Optional.of(new CatalogSchemaTableName(targetCatalog, targetSchema, tableName.getTableName()));
            }
        }

        if (defaultCatalogTarget.isPresent() && schemaMatchesLocalFilter(schemaName)) {
            return Optional.of(new CatalogSchemaTableName(defaultCatalogTarget.get(), schemaName, tableName.getTableName()));
        }

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
