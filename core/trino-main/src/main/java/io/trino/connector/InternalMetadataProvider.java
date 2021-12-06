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
package io.trino.connector;

import io.trino.FullConnectorSession;
import io.trino.Session;
import io.trino.metadata.MaterializedViewDefinition;
import io.trino.metadata.MetadataManager;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.TableHandle;
import io.trino.metadata.ViewColumn;
import io.trino.metadata.ViewDefinition;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnSchema;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableSchema;
import io.trino.spi.connector.MetadataProvider;
import io.trino.spi.type.TypeManager;

import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class InternalMetadataProvider
        implements MetadataProvider
{
    private final MetadataManager metadataManager;
    private final TypeManager typeManager;

    public InternalMetadataProvider(MetadataManager metadataManager, TypeManager typeManager)
    {
        this.metadataManager = requireNonNull(metadataManager, "metadataManager is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public Optional<ConnectorTableSchema> getRelationMetadata(ConnectorSession connectorSession, CatalogSchemaTableName tableName)
    {
        Session session = ((FullConnectorSession) connectorSession).getSession();
        QualifiedObjectName qualifiedName = new QualifiedObjectName(tableName.getCatalogName(), tableName.getSchemaTableName().getSchemaName(), tableName.getSchemaTableName().getTableName());

        Optional<MaterializedViewDefinition> materializedView = metadataManager.getMaterializedView(session, qualifiedName);
        if (materializedView.isPresent()) {
            return Optional.of(new ConnectorTableSchema(tableName.getSchemaTableName(), toColumnSchema(materializedView.get().getColumns())));
        }

        Optional<ViewDefinition> view = metadataManager.getView(session, qualifiedName);
        if (view.isPresent()) {
            return Optional.of(new ConnectorTableSchema(tableName.getSchemaTableName(), toColumnSchema(view.get().getColumns())));
        }

        Optional<TableHandle> tableHandle = metadataManager.getTableHandle(session, qualifiedName);
        if (tableHandle.isPresent()) {
            return Optional.of(metadataManager.getTableSchema(session, tableHandle.get()).getTableSchema());
        }

        return Optional.empty();
    }

    private List<ColumnSchema> toColumnSchema(List<ViewColumn> viewColumns)
    {
        return viewColumns.stream()
                .map(viewColumn ->
                        ColumnSchema.builder()
                                .setName(viewColumn.getName())
                                .setType(typeManager.getType(viewColumn.getType()))
                                .build())
                .collect(toImmutableList());
    }
}
