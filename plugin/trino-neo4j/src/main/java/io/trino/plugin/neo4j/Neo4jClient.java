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
package io.trino.plugin.neo4j;

import com.google.common.cache.Cache;
import com.google.common.collect.ImmutableList;
import com.google.inject.ConfigurationException;
import com.google.inject.Inject;
import com.google.inject.spi.Message;
import io.airlift.log.Logger;
import io.trino.cache.EvictableCacheBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import org.neo4j.cypherdsl.core.AliasedExpression;
import org.neo4j.cypherdsl.core.Cypher;
import org.neo4j.cypherdsl.core.Node;
import org.neo4j.cypherdsl.core.Relationship;
import org.neo4j.cypherdsl.core.Statement;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.Value;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.neo4j.Neo4jNamedRelationHandle.TableType.NODE;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static java.util.Locale.ENGLISH;

public class Neo4jClient
        implements AutoCloseable
{
    private static final Logger log = Logger.get(Neo4jClient.class);

    private static final String SYSTEM_DATABASE = "system";
    private static final String DEFAULT_DATABASE = "neo4j";

    private final Driver driver;
    private final Neo4jTypeManager typeManager;

    private final Cache<SchemaTableName, Neo4jRemoteTableName> tableNameCache;
    private final Cache<SchemaTableName, Neo4jTable> tableCache;

    @Inject
    public Neo4jClient(
            Neo4jConnectorConfig config,
            Neo4jTypeManager typeManager)
    {
        this.driver = GraphDatabase.driver(config.getURI(), getAuthToken(config));
        this.typeManager = typeManager;
        this.tableNameCache = EvictableCacheBuilder.newBuilder()
                .expireAfterWrite(Duration.ofMinutes(1))
                .build();

        this.tableCache = EvictableCacheBuilder.newBuilder()
                .expireAfterWrite(Duration.ofMinutes(1))
                .build();
    }

    private static AuthToken getAuthToken(Neo4jConnectorConfig config)
    {
        return switch (config.getAuthType().toLowerCase(ENGLISH)) {
            case "basic" -> AuthTokens.basic(config.getBasicAuthUser(), config.getBasicAuthPassword());
            case "bearer" -> AuthTokens.bearer(config.getBearerAuthToken());
            case "none" -> AuthTokens.none();
            default -> throw new ConfigurationException(ImmutableList.of(new Message("Unknown neo4j.auth.type: %s".formatted(config.getAuthType()))));
        };
    }

    public List<String> listSchemaNames(ConnectorSession connectorSession)
    {
        try (Session session = this.driver.session(SessionConfig.forDatabase(SYSTEM_DATABASE))) {
            return session.executeRead(tx -> {
                return tx.run("SHOW DATABASES YIELD name, type WHERE type <> 'system' RETURN distinct(name)")
                        .stream()
                        .map(r -> r.get(0).asString())
                        .map(s -> s.toLowerCase(ENGLISH))
                        .collect(toImmutableList());
            });
        }
    }

    // Nodes and relationships are mapped to tables.
    public List<SchemaTableName> listTables(ConnectorSession connectorSession, Optional<String> schemaName)
    {
        Statement statement =
                Cypher.union(
                        Cypher.call("db.labels")
                                .yield("label")
                                .returning(
                                        Cypher.literalOf("node").as("type"),
                                        Cypher.name("label").as("name"))
                                .build(),
                        Cypher.call("db.relationshipTypes")
                                .yield("relationshipType")
                                .returning(
                                        Cypher.literalOf("relationship").as("type"),
                                        Cypher.name("relationshipType").as("name"))
                                .build());

        String query = statement.getCypher();
        String databaseName = schemaName.orElseThrow();

        try (Session session = this.driver.session(SessionConfig.forDatabase(databaseName))) {
            return session.executeRead(tx -> {
                logQuery(databaseName, query);
                return tx.run(query)
                        .stream()
                        .map(r -> {
                            String name = r.get("name").asString();
                            String type = r.get("type").asString();
                            return switch (type) {
                                case "node" -> new SchemaTableName(schemaName.orElse(DEFAULT_DATABASE), "(%s)".formatted(name));
                                case "relationship" -> new SchemaTableName(schemaName.orElse(DEFAULT_DATABASE), "[%s]".formatted(name));
                                default -> throw new IllegalStateException("Unknown entity type: '%s'".formatted(type));
                            };
                        })
                        .collect(toImmutableList());
            });
        }
    }

    public Neo4jTable getTable(SchemaTableName schemaTableName)
    {
        try {
            return this.tableCache.get(schemaTableName, () -> loadTable(schemaTableName));
        }
        catch (ExecutionException e) {
            throwIfInstanceOf(e.getCause(), TrinoException.class);
            throw new RuntimeException(e);
        }
    }

    private Neo4jRemoteTableName getRemoteTableName(SchemaTableName schemaTableName)
    {
        try {
            return this.tableNameCache.get(schemaTableName, () -> this.loadRemoteTableName(schemaTableName));
        }
        catch (ExecutionException e) {
            throwIfInstanceOf(e.getCause(), TrinoException.class);
            throw new RuntimeException(e);
        }
    }

    private Neo4jTable loadTable(SchemaTableName schemaTableName)
    {
        Neo4jRemoteTableName remoteTableName = this.getRemoteTableName(schemaTableName);

        // SymbolicName nodeLabels = Cypher.name("nodeLabels");
        // Cypher.use(remoteTableName.getDatabaseName(),
        //         Cypher.call("db.schema.nodeTypeProperties")
        //                 .yield(nodeLabels, "propertyName", "propertyTypes", "mandatory");

        String query = switch (remoteTableName.getType()) {
            case NODE -> """
                      call db.schema.nodeTypeProperties()
                      yield nodeLabels, propertyName, propertyTypes, mandatory
                      where any(label IN nodeLabels WHERE label = $name)
                      return propertyName, propertyTypes, mandatory
                    """;
            case RELATIONSHIP -> """
                      call db.schema.relTypeProperties()
                      yield relType, propertyName, propertyTypes, mandatory
                      where relType = ':`' + $name + '`' and propertyName is not null
                      return propertyName, propertyTypes, mandatory
                    """;
        };

        Map<String, Object> parameters = Map.of(
                "name", remoteTableName.getName()
        );

        String databaseName = remoteTableName.getDatabaseName();

        try (Session session = this.driver.session(SessionConfig.forDatabase(databaseName))) {
            return session.executeRead(tx -> {
                logQuery(databaseName, query, parameters);
                List<Neo4jColumnHandle> columnHandles = tx.run(query, parameters)
                        .stream()
                        .filter(r -> !r.get("propertyName").isNull())
                        .filter(r -> !r.get("propertyTypes").isNull())
                        .map(r -> new Neo4jColumnHandle(
                                r.get("propertyName").asString(),
                                typeManager.propertyTypesToTrinoType(r.get("propertyTypes").asList(Value::asString)),
                                r.get("mandatory").asBoolean()))
                        .collect(toImmutableList());

                //columnHandles.add(Neo4jMetadata.ELEMENT_ID_COLUMN);

                /*ImmutableList<Neo4jColumnHandle> allColumnHandles = ImmutableList.<Neo4jColumnHandle>builder()
                        .add(Neo4jMetadata.ELEMENT_ID_COLUMN)
                        .addAll(columnHandles)
                       .build();*/

                Neo4jTableHandle tableHandle = new Neo4jTableHandle(new Neo4jNamedRelationHandle(schemaTableName, remoteTableName, NODE, OptionalLong.empty()));

                return new Neo4jTable(tableHandle, columnHandles);
            });
        }
    }

    private Neo4jRemoteTableName loadRemoteTableName(SchemaTableName schemaTableName)
    {
        String tableName = schemaTableName.getTableName();

        final Neo4jRemoteTableName.Type type = getTableType(tableName);

        //Cypher.call("db.labels").yield(Cypher.name("label"))
        //        .where(Cypher.call(Cypher.toLower()))

        String query = switch (type) {
            case NODE -> """
                    CALL db.labels() YIELD label
                    WHERE toLower(label) = $tableName
                    RETURN label as name
                    """;
            case RELATIONSHIP -> """
                    CALL db.relationshipTypes() YIELD relationshipType
                    WHERE toLower(relationshipType) = $tableName
                    RETURN relationshipType as name
                    """;
        };

        Map<String, Object> parameters = Map.of(
                "tableName", schemaTableName.getTableName().substring(1, schemaTableName.getTableName().length() - 1)
        );

        String databaseName = schemaTableName.getSchemaName();

        try (Session session = this.driver.session(SessionConfig.forDatabase(databaseName))) {
            return session.executeRead(tx -> {
                logQuery(databaseName, query, parameters);
                return tx.run(query, parameters)
                        .stream()
                        .map(r -> new Neo4jRemoteTableName(
                                schemaTableName.getSchemaName(),
                                r.get("name").asString(),
                                type))
                        .findFirst()
                        .orElseThrow(() -> new TrinoException(TABLE_NOT_FOUND, schemaTableName.toString()));
            });
        }
    }

    private static Neo4jRemoteTableName.Type getTableType(String tableName)
    {
        if (tableName.startsWith("(")) {
            return Neo4jRemoteTableName.Type.NODE;
        }
        else if (tableName.startsWith("[")) {
            return Neo4jRemoteTableName.Type.RELATIONSHIP;
        }
        else {
            throw new IllegalStateException("Bad table name: %s".formatted(tableName));
        }
    }

    public String toCypher(Neo4jTableHandle table, List<Neo4jColumnHandle> columnHandles)
    {
        if (table.getRelationHandle() instanceof Neo4jQueryRelationHandle queryHandle) {
            return queryHandle.getQuery();
        }

        Neo4jNamedRelationHandle namedRelation = table.getRequiredNamedRelation();

        Neo4jRemoteTableName remoteTableName = namedRelation.getRemoteTableName();

        var builder = switch (remoteTableName.getType()) {
            case NODE -> {
                // MATCH (t:$name) return t.col1 as col1, t.col2 as col2, ...
                Node node = Cypher.node(remoteTableName.getName()).named("t");
                List<AliasedExpression> properties = columnHandles.stream().map(c -> node.property(c.getColumnName()).as(c.getColumnName())).toList();

                yield Cypher.match(node).returning(properties);
            }
            case RELATIONSHIP -> {
                // MATCH ()-[r:$name]-() RETURN r.col1 as col1, r.col2 as col2, ...
                Relationship rel = Cypher.anyNode()
                        .relationshipBetween(Cypher.anyNode(), remoteTableName.getName())
                        .named("r");
                List<AliasedExpression> properties = columnHandles.stream().map(c -> rel.property(c.getColumnName()).as(c.getColumnName())).toList();

                yield Cypher.match(rel).returning(properties);
            }
        };

        if (namedRelation.getLimit().isPresent()) {
            return builder.limit(namedRelation.getLimit().getAsLong()).build().getCypher();
        }

        return builder.build().getCypher();
    }

    @Override
    public void close()
            throws Exception
    {
        this.driver.close();
    }

    public Session newSession(Optional<String> databaseName)
    {
        return this.driver.session(databaseName
                .map(SessionConfig::forDatabase)
                .orElse(SessionConfig.defaultConfig()));
    }

    private void logQuery(String databaseName, String cypher)
    {
        logQuery(databaseName, cypher, Map.of());
    }

    private void logQuery(String databaseName, String cypher, Map<String, Object> parameters)
    {
        log.debug("Executing query: database=%s cypher=%s parameters=%s ", databaseName, cypher, parameters);
    }
}
