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
package io.trino.execution;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.Session;
import io.trino.connector.ConnectorName;
import io.trino.metadata.CatalogInfo;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.TableProperties;
import io.trino.metadata.TableSchema;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.TupleDomain;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.TableScanNode;

import javax.annotation.concurrent.Immutable;

import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static java.util.Objects.requireNonNull;

@Immutable
public class TableInfo
{
    private final Optional<String> connectorName;
    private final QualifiedObjectName tableName;
    private final TupleDomain<ColumnHandle> predicate;

    @JsonCreator
    public TableInfo(
            @JsonProperty("connectorName") Optional<String> connectorName,
            @JsonProperty("tableName") QualifiedObjectName tableName,
            @JsonProperty("predicate") TupleDomain<ColumnHandle> predicate)
    {
        this.connectorName = requireNonNull(connectorName, "connectorName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.predicate = requireNonNull(predicate, "predicate is null");
    }

    @JsonProperty
    public Optional<String> getConnectorName()
    {
        return connectorName;
    }

    @JsonProperty
    public QualifiedObjectName getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getPredicate()
    {
        return predicate;
    }

    public static Map<PlanNodeId, TableInfo> extract(Session session, Metadata metadata, PlanFragment fragment)
    {
        return searchFrom(fragment.getRoot())
                .where(TableScanNode.class::isInstance)
                .findAll()
                .stream()
                .map(TableScanNode.class::cast)
                .collect(toImmutableMap(PlanNode::getId, node -> extract(session, metadata, node)));
    }

    private static TableInfo extract(Session session, Metadata metadata, TableScanNode node)
    {
        TableSchema tableSchema = metadata.getTableSchema(session, node.getTable());
        TableProperties tableProperties = metadata.getTableProperties(session, node.getTable());
        Optional<String> connectorName = metadata.listCatalogs(session).stream()
                .filter(catalogInfo -> catalogInfo.getCatalogName().equals(tableSchema.getCatalogName()))
                .map(CatalogInfo::getConnectorName)
                .map(ConnectorName::toString)
                .findFirst();
        return new TableInfo(connectorName, tableSchema.getQualifiedName(), tableProperties.getPredicate());
    }
}
