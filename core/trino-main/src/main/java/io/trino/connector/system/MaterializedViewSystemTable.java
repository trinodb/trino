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
package io.trino.connector.system;

import io.trino.FullConnectorSession;
import io.trino.Session;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.QualifiedTablePrefix;
import io.trino.security.AccessControl;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.InMemoryRecordSet;
import io.trino.spi.connector.MaterializedViewFreshness;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.predicate.TupleDomain;

import javax.inject.Inject;

import java.util.Optional;

import static io.trino.connector.system.jdbc.FilterUtil.tablePrefix;
import static io.trino.metadata.MetadataListing.getMaterializedViews;
import static io.trino.metadata.MetadataListing.listCatalogs;
import static io.trino.spi.connector.SystemTable.Distribution.SINGLE_COORDINATOR;
import static java.util.Objects.requireNonNull;

public class MaterializedViewSystemTable
        implements SystemTable
{
    private final Metadata metadata;
    private final AccessControl accessControl;
    private final MaterializedViewsSystemTableAdapter materializedViewAdapter;

    @Inject
    public MaterializedViewSystemTable(
            Metadata metadata,
            AccessControl accessControl,
            MaterializedViewsSystemTableAdapter materializedViewAdapter)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.materializedViewAdapter = requireNonNull(materializedViewAdapter, "materializedViewAdapter is null");
    }

    @Override
    public Distribution getDistribution()
    {
        return SINGLE_COORDINATOR;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata()
    {
        return materializedViewAdapter.getTableMetadata();
    }

    @Override
    public RecordCursor cursor(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession connectorSession,
            TupleDomain<Integer> constraint)
    {
        Session session = ((FullConnectorSession) connectorSession).getSession();
        InMemoryRecordSet.Builder displayTable = InMemoryRecordSet.builder(getTableMetadata());
        getAllMaterializedViews(session, constraint, displayTable);
        return displayTable.build().cursor();
    }

    private void getAllMaterializedViews(Session session, TupleDomain<Integer> constraint, InMemoryRecordSet.Builder displayTable)
    {
        Optional<String> catalogNameFilter = materializedViewAdapter.getCatalogNameFilter(constraint);
        Optional<String> schemaNameFilter = materializedViewAdapter.getSchemaNameFilter(constraint);
        Optional<String> tableNameFilter = materializedViewAdapter.getTableNameFilter(constraint);

        listCatalogs(session, metadata, accessControl, catalogNameFilter).keySet().forEach(catalogName -> {
            QualifiedTablePrefix tablePrefix = tablePrefix(catalogName, schemaNameFilter, tableNameFilter);

            getMaterializedViews(session, metadata, accessControl, tablePrefix).forEach((tableName, definition) -> {
                QualifiedObjectName name = new QualifiedObjectName(
                        tablePrefix.getCatalogName(),
                        tableName.getSchemaName(),
                        tableName.getTableName());
                MaterializedViewFreshness freshness = metadata.getMaterializedViewFreshness(session, name);
                MaterializedViewInfo materializedView = new MaterializedViewInfo(name, definition, freshness);

                displayTable.addRow(materializedViewAdapter.toTableRow(materializedView));
            });
        });
    }
}
