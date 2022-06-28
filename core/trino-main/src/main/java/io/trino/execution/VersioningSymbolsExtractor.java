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
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableVersioningLayout;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.OutputNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanVisitor;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TableScanNode;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class VersioningSymbolsExtractor
{
    private final Metadata metadata;

    public VersioningSymbolsExtractor(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    public Optional<PlanWithVersioningSymbols> extractVersioningSymbols(Session session, SymbolAllocator symbolAllocator, PlanNode root)
    {
        return root.accept(new Rewriter(session, symbolAllocator), null);
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

    private class Rewriter
            extends PlanVisitor<Optional<PlanWithVersioningSymbols>, Void>
    {
        private final Session session;
        private final SymbolAllocator symbolAllocator;

        public Rewriter(Session session, SymbolAllocator symbolAllocator)
        {
            this.session = requireNonNull(session, "session is null");
            this.symbolAllocator = requireNonNull(symbolAllocator);
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
                                            .putIdentities(plan.getVersioningSymbols())
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

            Map<Symbol, ColumnHandle> newVersioningColumns = newVersioningColumnsBuilder.buildOrThrow();
            return Optional.of(new PlanWithVersioningSymbols(
                    new TableScanNode(
                            node.getId(),
                            node.getTable(),
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
}
