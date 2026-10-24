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
import io.trino.metadata.Metadata;
import io.trino.security.AccessControl;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.InMemoryRecordSet;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;

import java.util.Optional;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.connector.system.jdbc.FilterUtil.isImpossibleObjectName;
import static io.trino.connector.system.jdbc.FilterUtil.tryGetSingleVarcharValue;
import static io.trino.metadata.MetadataListing.listCatalogNames;
import static io.trino.metadata.MetadataListing.listSchemas;
import static io.trino.metadata.MetadataUtil.TableMetadataBuilder.tableMetadataBuilder;
import static io.trino.spi.StandardErrorCode.SCHEMA_NOT_FOUND;
import static io.trino.spi.connector.SystemTable.Distribution.SINGLE_COORDINATOR;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class SchemaCommentSystemTable
        implements SystemTable
{
    private static final ConnectorTableMetadata TABLE = tableMetadataBuilder(new SchemaTableName("metadata", "schema_comments"))
            .column("catalog_name", VARCHAR)
            .column("schema_name", VARCHAR)
            .column("comment", VARCHAR)
            .build();

    private final Metadata metadata;
    private final AccessControl accessControl;

    @Inject
    public SchemaCommentSystemTable(Metadata metadata, AccessControl accessControl)
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
        return TABLE;
    }

    @Override
    public RecordCursor cursor(ConnectorTransactionHandle transactionHandle, ConnectorSession connectorSession, TupleDomain<Integer> constraint)
    {
        InMemoryRecordSet.Builder table = InMemoryRecordSet.builder(TABLE);
        Domain catalogDomain = constraint.getDomain(0, VARCHAR);
        Domain schemaDomain = constraint.getDomain(1, VARCHAR);
        if (constraint.isNone() || isImpossibleObjectName(catalogDomain) || isImpossibleObjectName(schemaDomain)) {
            return table.build().cursor();
        }

        Session session = ((FullConnectorSession) connectorSession).getSession();
        Optional<String> schemaName = tryGetSingleVarcharValue(schemaDomain);
        for (String catalog : listCatalogNames(session, metadata, accessControl, catalogDomain)) {
            for (String schema : listSchemas(session, metadata, accessControl, catalog, schemaName)) {
                if (!schemaDomain.includesNullableValue(utf8Slice(schema))) {
                    continue;
                }

                Optional<String> comment;
                try {
                    comment = metadata.getSchemaComment(session, new CatalogSchemaName(catalog, schema));
                }
                catch (TrinoException e) {
                    if (e instanceof SchemaNotFoundException || e.getErrorCode().equals(SCHEMA_NOT_FOUND.toErrorCode())) {
                        // The schema was dropped after it was listed.
                        continue;
                    }
                    throw e;
                }
                table.addRow(catalog, schema, comment.orElse(null));
            }
        }
        return table.build().cursor();
    }
}
