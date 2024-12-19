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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.SchemaTableName;
import org.neo4j.cypherdsl.core.AliasedExpression;
import org.neo4j.cypherdsl.core.Cypher;
import org.neo4j.cypherdsl.core.Node;
import org.neo4j.cypherdsl.core.Relationship;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class Neo4jNamedRelationHandle
        extends Neo4jRelationHandle
{
    public enum TableType
    {
        NODE, RELATIONSHIP
    }

    private final SchemaTableName schemaTableName;
    private final Neo4jRemoteTableName remoteTableName;
    private final TableType tableType;
    private final OptionalLong limit;

    @JsonCreator
    public Neo4jNamedRelationHandle(
            @JsonProperty("schemaTableName") SchemaTableName schemaTableName,
            @JsonProperty("remoteTableName") Neo4jRemoteTableName remoteTableName,
            @JsonProperty("tableType") TableType tableType,
            @JsonProperty("limit") OptionalLong limit)

    {
        this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
        this.remoteTableName = requireNonNull(remoteTableName, "remoteTableName is null");
        this.tableType = requireNonNull(tableType, "tableType is null");
        this.limit = limit;
    }

    @JsonProperty
    public SchemaTableName getSchemaTableName()
    {
        return this.schemaTableName;
    }

    @JsonProperty
    public Neo4jRemoteTableName getRemoteTableName()
    {
        return this.remoteTableName;
    }

    @JsonProperty
    public TableType getTableType()
    {
        return this.tableType;
    }

    @JsonProperty
    public OptionalLong getLimit()
    {
        return this.limit;
    }

    @Override
    public Optional<String> getDatabaseName()
    {
        return Optional.ofNullable(getRemoteTableName().getDatabaseName());
    }

    @Override
    public String toCypherQuery(List<Neo4jColumnHandle> columnHandles)
    {
        Neo4jRemoteTableName remoteTableName = this.getRemoteTableName();

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

        if (this.getLimit().isPresent()) {
            return builder.limit(this.getLimit().getAsLong()).build().getCypher();
        }

        return builder.build().getCypher();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("schemaTableName", schemaTableName)
                .add("remoteTableName", remoteTableName)
                .add("tableType", tableType)
                .add("limit", limit)
                .toString();
    }
}
