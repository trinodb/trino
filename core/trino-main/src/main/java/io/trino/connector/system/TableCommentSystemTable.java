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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.FullConnectorSession;
import io.trino.Session;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.QualifiedTablePrefix;
import io.trino.metadata.ViewInfo;
import io.trino.security.AccessControl;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.InMemoryRecordSet;
import io.trino.spi.connector.InMemoryRecordSet.Builder;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.predicate.TupleDomain;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.Sets.union;
import static io.trino.connector.system.jdbc.FilterUtil.tablePrefix;
import static io.trino.connector.system.jdbc.FilterUtil.tryGetSingleVarcharValue;
import static io.trino.metadata.MetadataListing.getMaterializedViews;
import static io.trino.metadata.MetadataListing.getViews;
import static io.trino.metadata.MetadataListing.listCatalogNames;
import static io.trino.metadata.MetadataListing.listTables;
import static io.trino.metadata.MetadataUtil.TableMetadataBuilder.tableMetadataBuilder;
import static io.trino.spi.connector.SystemTable.Distribution.SINGLE_COORDINATOR;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
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
        Optional<String> catalogFilter = tryGetSingleVarcharValue(constraint, 0);
        Optional<String> schemaFilter = tryGetSingleVarcharValue(constraint, 1);
        Optional<String> tableFilter = tryGetSingleVarcharValue(constraint, 2);

        Session session = ((FullConnectorSession) connectorSession).getSession();
        Builder table = InMemoryRecordSet.builder(COMMENT_TABLE);

        for (String catalog : listCatalogNames(session, metadata, accessControl, catalogFilter)) {
            QualifiedTablePrefix prefix = tablePrefix(catalog, schemaFilter, tableFilter);

            Set<SchemaTableName> names = ImmutableSet.of();
            Map<SchemaTableName, ViewInfo> views = ImmutableMap.of();
            Map<SchemaTableName, ViewInfo> materializedViews = ImmutableMap.of();
            try {
                materializedViews = getMaterializedViews(session, metadata, accessControl, prefix);
                views = getViews(session, metadata, accessControl, prefix);
                // Some connectors like blackhole, accumulo and raptor don't return views in listTables
                // Materialized views are consistently returned in listTables by the relevant connectors
                names = union(listTables(session, metadata, accessControl, prefix), views.keySet());
            }
            catch (TrinoException e) {
                // listTables throws an exception if cannot connect the database
                LOG.warn(e, "Failed to get tables for catalog: %s", catalog);
            }

            for (SchemaTableName name : names) {
                Optional<String> comment = Optional.empty();
                try {
                    comment = getComment(session, prefix, name, views, materializedViews);
                }
                catch (RuntimeException e) {
                    // getTableHandle may throw an exception (e.g. Cassandra connector doesn't allow case insensitive column names)
                    LOG.warn(e, "Failed to get metadata for table: %s", name);
                }
                table.addRow(prefix.getCatalogName(), name.getSchemaName(), name.getTableName(), comment.orElse(null));
            }
        }

        return table.build().cursor();
    }

    private Optional<String> getComment(
            Session session,
            QualifiedTablePrefix prefix,
            SchemaTableName name,
            Map<SchemaTableName, ViewInfo> views,
            Map<SchemaTableName, ViewInfo> materializedViews)
    {
        ViewInfo materializedViewDefinition = materializedViews.get(name);
        if (materializedViewDefinition != null) {
            return materializedViewDefinition.getComment();
        }
        ViewInfo viewInfo = views.get(name);
        if (viewInfo != null) {
            return viewInfo.getComment();
        }
        QualifiedObjectName tableName = new QualifiedObjectName(prefix.getCatalogName(), name.getSchemaName(), name.getTableName());
        return metadata.getRedirectionAwareTableHandle(session, tableName).getTableHandle()
                .map(handle -> metadata.getTableMetadata(session, handle))
                .map(metadata -> metadata.getMetadata().getComment())
                .orElseGet(() -> {
                    // A previously listed table might have been dropped concurrently
                    LOG.warn("Failed to get metadata for table: %s", name);
                    return Optional.empty();
                });
    }
}
