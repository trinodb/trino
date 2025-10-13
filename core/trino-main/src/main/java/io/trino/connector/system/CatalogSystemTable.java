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

import com.google.inject.Inject;
import io.trino.FullConnectorSession;
import io.trino.Session;
import io.trino.metadata.CatalogInfo;
import io.trino.metadata.Metadata;
import io.trino.security.AccessControl;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.InMemoryRecordSet;
import io.trino.spi.connector.InMemoryRecordSet.Builder;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.predicate.TupleDomain;

import static io.trino.metadata.MetadataListing.listCatalogs;
import static io.trino.metadata.MetadataUtil.TableMetadataBuilder.tableMetadataBuilder;
import static io.trino.spi.connector.SystemTable.Distribution.SINGLE_COORDINATOR;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.util.Objects.requireNonNull;

public class CatalogSystemTable
        implements SystemTable
{
    public static final SchemaTableName CATALOG_TABLE_NAME = new SchemaTableName("metadata", "catalogs");

    public static final ConnectorTableMetadata CATALOG_TABLE = tableMetadataBuilder(CATALOG_TABLE_NAME)
            .column("catalog_name", createUnboundedVarcharType())
            .column("connector_id", createUnboundedVarcharType())
            .column("connector_name", createUnboundedVarcharType())
            .build();
    private final Metadata metadata;
    private final AccessControl accessControl;

    @Inject
    public CatalogSystemTable(Metadata metadata, AccessControl accessControl)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
    }

    @Override
    public Distribution getDistribution()
    {
        return SINGLE_COORDINATOR;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata()
    {
        return CATALOG_TABLE;
    }

    @Override
    public RecordCursor cursor(ConnectorTransactionHandle transactionHandle, ConnectorSession connectorSession, TupleDomain<Integer> constraint)
    {
        Session session = ((FullConnectorSession) connectorSession).getSession();
        Builder table = InMemoryRecordSet.builder(CATALOG_TABLE);
        for (CatalogInfo catalogInfo : listCatalogs(session, metadata, accessControl)) {
            table.addRow(
                    catalogInfo.catalogName(),
                    catalogInfo.catalogName(),
                    catalogInfo.connectorName().toString());
        }
        return table.build().cursor();
    }
}
