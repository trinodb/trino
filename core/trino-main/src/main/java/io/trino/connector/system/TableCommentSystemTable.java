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
import io.airlift.slice.Slice;
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
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.connector.system.jdbc.FilterUtil.isImpossibleObjectName;
import static io.trino.connector.system.jdbc.FilterUtil.tablePrefix;
import static io.trino.connector.system.jdbc.FilterUtil.tryGetSingleVarcharValue;
import static io.trino.metadata.MetadataListing.listCatalogNames;
import static io.trino.metadata.MetadataUtil.TableMetadataBuilder.tableMetadataBuilder;
import static io.trino.spi.connector.SystemTable.Distribution.SINGLE_COORDINATOR;
import static io.trino.spi.type.VarcharType.VARCHAR;
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
        Builder table = InMemoryRecordSet.builder(COMMENT_TABLE);

        Domain catalogDomain = constraint.getDomain(0, VARCHAR);
        Domain schemaDomain = constraint.getDomain(1, VARCHAR);
        Domain tableDomain = constraint.getDomain(2, VARCHAR);

        if (isImpossibleObjectName(catalogDomain) || isImpossibleObjectName(schemaDomain) || isImpossibleObjectName(tableDomain)) {
            return table.build().cursor();
        }

        Optional<String> tableFilter = tryGetSingleVarcharValue(tableDomain);

        Session session = ((FullConnectorSession) connectorSession).getSession();

        for (String catalog : listCatalogNames(session, metadata, accessControl, catalogDomain)) {
            // TODO A connector may be able to pull information from multiple schemas at once, so pass the schema filter to the connector instead.
            // TODO Support LIKE predicates on schema name (or any other functional predicates), so pass the schema filter as Constraint-like to the connector.
            if (schemaDomain.isNullableDiscreteSet()) {
                for (Object slice : schemaDomain.getNullableDiscreteSet().getNonNullValues()) {
                    String schemaName = ((Slice) slice).toStringUtf8();
                    if (isImpossibleObjectName(schemaName)) {
                        continue;
                    }
                    addTableCommentForCatalog(session, table, catalog, tablePrefix(catalog, Optional.of(schemaName), tableFilter));
                }
            }
            else {
                addTableCommentForCatalog(session, table, catalog, tablePrefix(catalog, Optional.empty(), tableFilter));
            }
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
            AtomicInteger filteredCount = new AtomicInteger();
            List<RelationCommentMetadata> relationComments = metadata.listRelationComments(
                    session,
                    prefix.getCatalogName(),
                    prefix.getSchemaName(),
                    relationNames -> {
                        Set<SchemaTableName> filtered = accessControl.filterTables(session.toSecurityContext(), catalog, relationNames);
                        filteredCount.addAndGet(filtered.size());
                        return filtered;
                    });
            checkState(
                    // Inequality because relationFilter can be invoked more than once on a set of names.
                    filteredCount.get() >= relationComments.size(),
                    "relationFilter is mandatory, but it has not been called for some of returned relations: returned %s relations, %s passed the filter",
                    relationComments.size(),
                    filteredCount.get());

            for (RelationCommentMetadata commentMetadata : relationComments) {
                SchemaTableName name = commentMetadata.name();
                if (!commentMetadata.tableRedirected()) {
                    table.addRow(catalog, name.getSchemaName(), name.getTableName(), commentMetadata.comment().orElse(null));
                }
                else {
                    try {
                        RelationComment relationComment = getTableCommentRedirectionAware(session, new QualifiedObjectName(catalog, name.getSchemaName(), name.getTableName()));
                        if (relationComment.found()) {
                            table.addRow(catalog, name.getSchemaName(), name.getTableName(), relationComment.comment().orElse(null));
                        }
                    }
                    catch (RuntimeException e) {
                        LOG.warn(e, "Failed to get metadata for redirected table: %s", name);
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

        return getTableCommentRedirectionAware(session, relationName);
    }

    private RelationComment getTableCommentRedirectionAware(Session session, QualifiedObjectName relationName)
    {
        RedirectionAwareTableHandle redirectionAware = metadata.getRedirectionAwareTableHandle(session, relationName);

        if (redirectionAware.tableHandle().isEmpty()) {
            return new RelationComment(false, Optional.empty());
        }

        if (redirectionAware.redirectedTableName().isPresent()) {
            QualifiedObjectName redirectedRelationName = redirectionAware.redirectedTableName().get();
            SchemaTableName redirectedTableName = redirectedRelationName.asSchemaTableName();
            if (!accessControl.filterTables(session.toSecurityContext(), redirectedRelationName.catalogName(), ImmutableSet.of(redirectedTableName)).contains(redirectedTableName)) {
                return new RelationComment(false, Optional.empty());
            }
        }

        return new RelationComment(true, metadata.getTableMetadata(session, redirectionAware.tableHandle().get()).metadata().getComment());
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
