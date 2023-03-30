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
package io.trino.sql.planner.planprinter;

import io.trino.Session;
import io.trino.connector.ConnectorName;
import io.trino.execution.TableInfo;
import io.trino.metadata.CatalogInfo;
import io.trino.metadata.Metadata;
import io.trino.metadata.TableProperties;
import io.trino.metadata.TableSchema;
import io.trino.sql.planner.plan.TableScanNode;

import java.util.Optional;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public class TableInfoSupplier
        implements Function<TableScanNode, TableInfo>
{
    private final Metadata metadata;
    private final Session session;

    public TableInfoSupplier(Metadata metadata, Session session)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.session = requireNonNull(session, "session is null");
    }

    @Override
    public TableInfo apply(TableScanNode node)
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
