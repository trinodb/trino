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
package io.prestosql.connector.system;

import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.prestosql.Session;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.QualifiedObjectName;
import io.prestosql.metadata.QualifiedTablePrefix;
import io.prestosql.security.AccessControl;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.InMemoryRecordSet;
import io.prestosql.spi.connector.InMemoryRecordSet.Builder;
import io.prestosql.spi.connector.RecordCursor;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.SystemTable;
import io.prestosql.spi.predicate.TupleDomain;

import javax.inject.Inject;

import java.util.Optional;
import java.util.Set;

import static io.prestosql.connector.system.SystemConnectorSessionUtil.toSession;
import static io.prestosql.connector.system.jdbc.FilterUtil.filter;
import static io.prestosql.connector.system.jdbc.FilterUtil.stringFilter;
import static io.prestosql.connector.system.jdbc.FilterUtil.tablePrefix;
import static io.prestosql.metadata.MetadataListing.listCatalogs;
import static io.prestosql.metadata.MetadataListing.listTables;
import static io.prestosql.metadata.MetadataUtil.TableMetadataBuilder.tableMetadataBuilder;
import static io.prestosql.spi.connector.SystemTable.Distribution.SINGLE_COORDINATOR;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static java.util.Objects.requireNonNull;

public class TableCommentSystemTable
        implements SystemTable
{
    private static final Logger LOG = Logger.get(TableCommentSystemTable.class);

    private static final SchemaTableName COMMENT_TABLE_NAME = new SchemaTableName("metadata", "table_comments");

    private static final ConnectorTableMetadata COMMENT_TABLE = tableMetadataBuilder(COMMENT_TABLE_NAME)
            .column("catalog_name", createUnboundedVarcharType())
            .column("schema_name", createUnboundedVarcharType())
            .column("table_name", createUnboundedVarcharType())
            .column("comment", createUnboundedVarcharType())
            .build();

    private final Metadata metadata;
    private final AccessControl accessControl;

    @Inject
    public TableCommentSystemTable(Metadata metadata, AccessControl accessControl)
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
        return COMMENT_TABLE;
    }

    @Override
    public RecordCursor cursor(ConnectorTransactionHandle transactionHandle, ConnectorSession connectorSession, TupleDomain<Integer> constraint)
    {
        Optional<String> catalogFilter = stringFilter(constraint, 0);
        Optional<String> schemaFilter = stringFilter(constraint, 1);
        Optional<String> tableFilter = stringFilter(constraint, 2);

        Session session = toSession(transactionHandle, connectorSession);
        Builder table = InMemoryRecordSet.builder(COMMENT_TABLE);

        for (String catalog : filter(listCatalogs(session, metadata, accessControl).keySet(), catalogFilter)) {
            QualifiedTablePrefix prefix = tablePrefix(catalog, schemaFilter, tableFilter);

            Set<SchemaTableName> names = ImmutableSet.of();
            try {
                names = listTables(session, metadata, accessControl, prefix);
            }
            catch (PrestoException e) {
                // listTables throws an exception if cannot connect the database
                LOG.debug(e, "Failed to get tables for catalog: %s", catalog);
            }

            for (SchemaTableName name : names) {
                QualifiedObjectName tableName = new QualifiedObjectName(prefix.getCatalogName(), name.getSchemaName(), name.getTableName());
                Optional<String> comment = Optional.empty();
                try {
                    comment = metadata.getTableHandle(session, tableName)
                            .map(handle -> metadata.getTableMetadata(session, handle))
                            .map(metadata -> metadata.getMetadata().getComment())
                            .get();
                }
                catch (PrestoException e) {
                    // getTableHandle may throw an exception (e.g. Cassandra connector doesn't allow case insensitive column names)
                    LOG.debug(e, "Failed to get metadata for table: %s", name);
                }
                table.addRow(prefix.getCatalogName(), name.getSchemaName(), name.getTableName(), comment.orElse(null));
            }
        }

        return table.build().cursor();
    }
}
