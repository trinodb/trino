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
package io.trino.connector.system.jdbc;

import io.trino.FullConnectorSession;
import io.trino.Session;
import io.trino.metadata.Metadata;
import io.trino.security.AccessControl;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.InMemoryRecordSet;
import io.trino.spi.connector.InMemoryRecordSet.Builder;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;

import javax.inject.Inject;

import java.util.Optional;

import static io.trino.connector.system.jdbc.FilterUtil.tryGetSingleVarcharValue;
import static io.trino.metadata.MetadataListing.listCatalogNames;
import static io.trino.metadata.MetadataListing.listSchemas;
import static io.trino.metadata.MetadataUtil.TableMetadataBuilder.tableMetadataBuilder;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.util.Objects.requireNonNull;

public class SchemaJdbcTable
        extends JdbcTable
{
    public static final SchemaTableName NAME = new SchemaTableName("jdbc", "schemas");

    public static final ConnectorTableMetadata METADATA = tableMetadataBuilder(NAME)
            .column("table_schem", createUnboundedVarcharType())
            .column("table_catalog", createUnboundedVarcharType())
            .build();

    private final Metadata metadata;
    private final AccessControl accessControl;

    @Inject
    public SchemaJdbcTable(Metadata metadata, AccessControl accessControl)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
    }

    @Override
    public ConnectorTableMetadata getTableMetadata()
    {
        return METADATA;
    }

    @Override
    public RecordCursor cursor(ConnectorTransactionHandle transactionHandle, ConnectorSession connectorSession, TupleDomain<Integer> constraint)
    {
        Session session = ((FullConnectorSession) connectorSession).getSession();
        Optional<String> catalogFilter = tryGetSingleVarcharValue(constraint, 1);

        Builder table = InMemoryRecordSet.builder(METADATA);
        for (String catalog : listCatalogNames(session, metadata, accessControl, catalogFilter)) {
            for (String schema : listSchemas(session, metadata, accessControl, catalog)) {
                table.addRow(schema, catalog);
            }
        }
        return table.build().cursor();
    }
}
