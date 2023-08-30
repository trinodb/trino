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

import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.FullConnectorSession;
import io.trino.Session;
import io.trino.metadata.MaterializedViewDefinition;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.QualifiedTablePrefix;
import io.trino.metadata.RedirectionAwareTableHandle;
import io.trino.metadata.ViewDefinition;
import io.trino.security.AccessControl;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.InMemoryRecordSet;
import io.trino.spi.connector.InMemoryRecordSet.Builder;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.RelationCommentMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.predicate.TupleDomain;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.connector.system.jdbc.FilterUtil.tablePrefix;
import static io.trino.connector.system.jdbc.FilterUtil.tryGetSingleVarcharValue;
import static io.trino.metadata.MetadataListing.listCatalogNames;
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

            addTableCommentForCatalog(session, table, catalog, prefix);
        }

        return table.build().cursor();
    }

    private void addTableCommentForCatalog(Session session, Builder table, String catalog, QualifiedTablePrefix prefix)
    {
        if (prefix.getTableName().isPresent()) {
            QualifiedObjectName relationName = new QualifiedObjectName(catalog, prefix.getSchemaName().orElseThrow(), prefix.getTableName().get());
            RelationComment relationComment;
            try {
                relationComment = getRelationComment(session, relationName);
            }
            catch (RuntimeException e) {
                LOG.warn(e, "Failed to get comment for relation: %s", relationName);
                relationComment = new RelationComment(false, Optional.empty());
            }
            if (relationComment.found()) {
                SchemaTableName schemaTableName = relationName.asSchemaTableName();
                // Consulting accessControl first would be simpler but some AccessControl implementations may have issues when asked for a relation that does not exist.
                if (accessControl.filterTables(session.toSecurityContext(), catalog, ImmutableSet.of(schemaTableName)).contains(schemaTableName)) {
                    table.addRow(catalog, schemaTableName.getSchemaName(), schemaTableName.getTableName(), relationComment.comment().orElse(null));
                }
            }
        }
        else {
            List<RelationCommentMetadata> relationComments = metadata.listRelationComments(
                    session,
                    prefix.getCatalogName(),
                    prefix.getSchemaName(),
                    relationNames -> accessControl.filterTables(session.toSecurityContext(), catalog, relationNames));

            for (RelationCommentMetadata commentMetadata : relationComments) {
                SchemaTableName name = commentMetadata.name();
                if (!commentMetadata.tableRedirected()) {
                    table.addRow(catalog, name.getSchemaName(), name.getTableName(), commentMetadata.comment().orElse(null));
                }
                else {
                    try {
                        // TODO (https://github.com/trinodb/trino/issues/18514) this should consult accessControl on redirected name. Leaving for now as-is.
                        metadata.getRedirectionAwareTableHandle(session, new QualifiedObjectName(catalog, name.getSchemaName(), name.getTableName()))
                                .tableHandle().ifPresent(tableHandle -> {
                                    Optional<String> comment = metadata.getTableMetadata(session, tableHandle).getMetadata().getComment();
                                    table.addRow(catalog, name.getSchemaName(), name.getTableName(), comment.orElse(null));
                                });
                    }
                    catch (RuntimeException e) {
                        LOG.warn(e, "Failed to get metadata for table: %s", name);
                    }
                }
            }
        }
    }

    private RelationComment getRelationComment(Session session, QualifiedObjectName relationName)
    {
        Optional<MaterializedViewDefinition> materializedView = metadata.getMaterializedView(session, relationName);
        if (materializedView.isPresent()) {
            return new RelationComment(true, materializedView.get().getComment());
        }

        Optional<ViewDefinition> view = metadata.getView(session, relationName);
        if (view.isPresent()) {
            return new RelationComment(true, view.get().getComment());
        }

        RedirectionAwareTableHandle redirectionAware = metadata.getRedirectionAwareTableHandle(session, relationName);
        if (redirectionAware.tableHandle().isPresent()) {
            // TODO (https://github.com/trinodb/trino/issues/18514) this should consult accessControl on redirected name. Leaving for now as-is.
            return new RelationComment(true, metadata.getTableMetadata(session, redirectionAware.tableHandle().get()).getMetadata().getComment());
        }

        return new RelationComment(false, Optional.empty());
    }

    private record RelationComment(boolean found, Optional<String> comment)
    {
        RelationComment
        {
            requireNonNull(comment, "comment is null");
            checkArgument(found || comment.isEmpty(), "Unexpected comment for a relation that is not found");
        }
    }
}
