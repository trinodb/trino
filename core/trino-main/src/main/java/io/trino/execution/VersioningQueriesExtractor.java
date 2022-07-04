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
package io.trino.execution;

import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.metadata.Metadata;
import io.trino.metadata.TableHandle;
import io.trino.metadata.TableVersion;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableVersioningLayout;
import io.trino.spi.predicate.TupleDomain;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.OutputNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanVisitor;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.tree.NullLiteral;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.StandardErrorCode.INVALID_VIEW;
import static java.util.Objects.requireNonNull;

public class VersioningQueriesExtractor
{
    private final Metadata metadata;

    public VersioningQueriesExtractor(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    public Optional<PlanWithVersioningSymbols> extractInsertQuery(
            Session session,
            SymbolAllocator symbolAllocator,
            PlanNode root,
            Optional<Map<TableHandle, TableVersion>> sourceTableVersions)
    {
        Map<TableHandle, TableVersion> versions = new HashMap<>();
        sourceTableVersions.ifPresent(versions::putAll);
        Optional<PlanWithVersioningSymbols> planWithVersioningSymbols = root.accept(new InsertQueryExtractor(session, symbolAllocator, versions), null);
        if (!versions.isEmpty()) {
            throw new TrinoException(INVALID_VIEW, "Not all source table versions were propagated");
        }
        return planWithVersioningSymbols;
    }

    public PlanNode extractDeleteQuery(
            Session session,
            PlanNodeIdAllocator idAllocator,
            SymbolAllocator symbolAllocator,
            PlanNode root,
            Map<TableHandle, TableVersion> sourceTableVersions)
    {
        checkArgument(extractInsertQuery(session, symbolAllocator, root, Optional.empty()).isPresent(), "Query does not support versioning");
        Map<TableHandle, TableVersion> versions = new HashMap<>(sourceTableVersions);
        return root.accept(new DeleteQueryExtractor(session, idAllocator, symbolAllocator, versions), null);
    }

    public static class PlanWithVersioningSymbols
    {
        private final PlanNode root;
        private final Set<Symbol> versioningSymbols;
        private final boolean unique;

        public PlanWithVersioningSymbols(PlanNode root, Set<Symbol> versioningSymbols, boolean unique)
        {
            this.root = requireNonNull(root, "root is null");
            this.versioningSymbols = requireNonNull(versioningSymbols, "versioningSymbols is null");
            this.unique = unique;
        }

        public PlanNode getRoot()
        {
            return root;
        }

        public Set<Symbol> getVersioningSymbols()
        {
            return versioningSymbols;
        }

        public boolean isUnique()
        {
            return unique;
        }
    }

    private class InsertQueryExtractor
            extends PlanVisitor<Optional<PlanWithVersioningSymbols>, Void>
    {
        private final Session session;
        private final SymbolAllocator symbolAllocator;
        private final Map<TableHandle, TableVersion> versions;

        public InsertQueryExtractor(Session session, SymbolAllocator symbolAllocator, Map<TableHandle, TableVersion> versions)
        {
            this.session = requireNonNull(session, "session is null");
            this.symbolAllocator = requireNonNull(symbolAllocator);
            this.versions = requireNonNull(versions, "versions is null");
        }

        @Override
        protected Optional<PlanWithVersioningSymbols> visitPlan(PlanNode node, Void context)
        {
            return Optional.empty();
        }

        @Override
        public Optional<PlanWithVersioningSymbols> visitOutput(OutputNode node, Void context)
        {
            return node.getSource().accept(this, null)
                    .map(plan -> {
                        ImmutableList.Builder<String> newColumnNames = ImmutableList.builder();
                        ImmutableList.Builder<Symbol> newSymbols = ImmutableList.builder();
                        newColumnNames.addAll(node.getColumnNames());
                        newSymbols.addAll(node.getOutputSymbols());
                        plan.getVersioningSymbols().stream()
                                .filter(symbol -> !node.getOutputSymbols().contains(symbol))
                                .forEach(symbol -> {
                                    // TODO: KS handle name clashes with exiting columns
                                    newColumnNames.add(symbol.getName());
                                    newSymbols.add(symbol);
                                });
                        return new PlanWithVersioningSymbols(
                                new OutputNode(
                                        node.getId(),
                                        plan.getRoot(),
                                        newColumnNames.build(),
                                        newSymbols.build()),
                                plan.getVersioningSymbols(),
                                plan.isUnique());
                    });
        }

        @Override
        public Optional<PlanWithVersioningSymbols> visitProject(ProjectNode node, Void context)
        {
            return node.getSource().accept(this, null)
                    .map(plan -> new PlanWithVersioningSymbols(
                            new ProjectNode(
                                    node.getId(),
                                    plan.getRoot(),
                                    Assignments.builder()
                                            .putAll(node.getAssignments())
                                            .putIdentities(plan.getVersioningSymbols().stream()
                                                    .filter(symbol -> !node.getOutputSymbols().contains(symbol))
                                                    .collect(toImmutableList()))
                                            .build()),
                            plan.getVersioningSymbols(),
                            plan.isUnique()));
        }

        @Override
        public Optional<PlanWithVersioningSymbols> visitTableScan(TableScanNode node, Void context)
        {
            Optional<ConnectorTableVersioningLayout> versioningLayout = metadata.getTableVersioningLayout(session, node.getTable());
            if (versioningLayout.isEmpty()) {
                return Optional.empty();
            }

            Map<ColumnHandle, Symbol> assignments = HashBiMap.create(node.getAssignments()).inverse();
            ImmutableMap.Builder<Symbol, ColumnHandle> newVersioningColumnsBuilder = ImmutableMap.builder();
            ImmutableSet.Builder<Symbol> versioningSymbols = ImmutableSet.builder();
            for (ColumnHandle versioningColumn : versioningLayout.get().getVersioningColumns()) {
                if (assignments.containsKey(versioningColumn)) {
                    // versioning column already present
                    versioningSymbols.add(assignments.get(versioningColumn));
                }
                else {
                    throw new IllegalStateException("All columns should be present in unpruned table scan");
                    // TODO: KS remove
                    //ColumnMetadata columnMetadata = metadata.getColumnMetadata(session, node.getTable(), versioningColumn);
                    //Symbol symbol = symbolAllocator.newSymbol(columnMetadata.getName(), columnMetadata.getType());
                    //newVersioningColumnsBuilder.put(symbol, versioningColumn);
                    //versioningSymbols.add(symbol);
                }
            }

            TableHandle handle = node.getTable();
            if (versions.containsKey(handle)) {
                handle = metadata.getInsertedOrUpdatedRows(session, handle, versions.remove(handle))
                        .orElseThrow(() -> new IllegalStateException("Query does not support versioning"));
            }

            Map<Symbol, ColumnHandle> newVersioningColumns = newVersioningColumnsBuilder.buildOrThrow();
            return Optional.of(new PlanWithVersioningSymbols(
                    new TableScanNode(
                            node.getId(),
                            handle,
                            ImmutableList.<Symbol>builder()
                                    .addAll(node.getOutputSymbols())
                                    .addAll(newVersioningColumns.keySet())
                                    .build(),
                            ImmutableMap.<Symbol, ColumnHandle>builder()
                                    .putAll(node.getAssignments())
                                    .putAll(newVersioningColumns)
                                    .buildOrThrow(),
                            node.getEnforcedConstraint(),
                            node.getStatistics(),
                            node.isUpdateTarget(),
                            node.getUseConnectorNodePartitioning()),
                    versioningSymbols.build(),
                    versioningLayout.get().isUnique()));
        }
    }

    private class DeleteQueryExtractor
            extends PlanVisitor<PlanNode, Void>
    {
        private final Session session;
        private final PlanNodeIdAllocator idAllocator;
        private final SymbolAllocator symbolAllocator;
        private final Map<TableHandle, TableVersion> versions;
        private final Map<Symbol, Symbol> versioningSymbolMapping = new HashMap<>();

        public DeleteQueryExtractor(Session session, PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator, Map<TableHandle, TableVersion> versions)
        {
            this.session = requireNonNull(session, "session is null");
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.symbolAllocator = requireNonNull(symbolAllocator, "symbolAllocator is null");
            this.versions = requireNonNull(versions, "versions is null");
        }

        @Override
        protected PlanNode visitPlan(PlanNode node, Void context)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public PlanNode visitProject(ProjectNode node, Void context)
        {
            PlanNode source = node.getSource().accept(this, null);
            Assignments.Builder assignments = Assignments.builder();
            Set<Symbol> addedVersioningSymbols = new HashSet<>();
            for (Symbol symbol : node.getOutputSymbols()) {
                Symbol versioningSymbol = versioningSymbolMapping.get(symbol);
                if (versioningSymbol == null) {
                    // put null for each non-versioning symbol
                    assignments.put(symbolAllocator.newSymbol(symbol), new NullLiteral());
                }
                else {
                    assignments.putIdentity(versioningSymbol);
                    addedVersioningSymbols.add(versioningSymbol);
                }
            }
            // versioning symbols are added last
            versioningSymbolMapping.values().stream()
                    .filter(symbol -> !addedVersioningSymbols.contains(symbol))
                    .forEach(assignments::putIdentity);
            return new ProjectNode(
                    // use unique node ids
                    idAllocator.getNextId(),
                    source,
                    assignments.build());
        }

        @Override
        public PlanNode visitTableScan(TableScanNode node, Void context)
        {
            ConnectorTableVersioningLayout versioningLayout = metadata.getTableVersioningLayout(session, node.getTable())
                    .orElseThrow(() -> new IllegalStateException("Query does not support versioning"));
            TableHandle deleteHandle = metadata.getDeletedRows(
                            session,
                            node.getTable(),
                            requireNonNull(versions.remove(node.getTable()), "Missing version for table handle"))
                    .orElseThrow(() -> new IllegalStateException("Query does not support versioning"));

            ImmutableMap.Builder<Symbol, ColumnHandle> scanAssignmentsBuilder = ImmutableMap.builder();
            Assignments.Builder projectionAssignments = Assignments.builder();
            ImmutableSet.Builder<Symbol> versioningSymbols = ImmutableSet.builder();
            for (Symbol symbol : node.getOutputSymbols()) {
                Symbol newSymbol = symbolAllocator.newSymbol(symbol);
                ColumnHandle columnHandle = node.getAssignments().get(symbol);
                if (versioningLayout.getVersioningColumns().contains(columnHandle)) {
                    scanAssignmentsBuilder.put(newSymbol, columnHandle);
                    projectionAssignments.putIdentity(newSymbol);
                    versioningSymbols.add(newSymbol);
                    versioningSymbolMapping.put(symbol, newSymbol);
                }
                else {
                    projectionAssignments.put(newSymbol, new NullLiteral());
                }
            }
            Map<Symbol, ColumnHandle> scanAssignments = scanAssignmentsBuilder.buildOrThrow();

            return new ProjectNode(
                    idAllocator.getNextId(),
                    new TableScanNode(
                            idAllocator.getNextId(),
                            deleteHandle,
                            ImmutableList.copyOf(scanAssignments.keySet()),
                            scanAssignments,
                            TupleDomain.all(),
                            Optional.empty(),
                            false,
                            Optional.empty()),
                    projectionAssignments.build());
        }
    }
}
