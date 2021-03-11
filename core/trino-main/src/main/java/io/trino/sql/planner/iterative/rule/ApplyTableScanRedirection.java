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
package io.trino.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.Metadata;
import io.trino.metadata.TableHandle;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.TableScanRedirectApplicationResult;
import io.trino.spi.connector.TableScanRedirectApplicationResult.Redirection;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import io.trino.sql.planner.DomainTranslator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TableScanNode;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.metadata.QualifiedObjectName.convertFromSchemaTableName;
import static io.trino.spi.StandardErrorCode.COLUMN_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.TYPE_MISMATCH;
import static io.trino.sql.planner.plan.Patterns.tableScan;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ApplyTableScanRedirection
        implements Rule<TableScanNode>
{
    private static final Pattern<TableScanNode> PATTERN = tableScan()
            .matching(node -> !node.isForDelete());

    private final Metadata metadata;
    private final DomainTranslator domainTranslator;

    public ApplyTableScanRedirection(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.domainTranslator = new DomainTranslator(metadata);
    }

    @Override
    public Pattern<TableScanNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(TableScanNode scanNode, Captures captures, Context context)
    {
        Optional<TableScanRedirectApplicationResult> tableScanRedirectApplicationResult = metadata.applyTableScanRedirect(context.getSession(), scanNode.getTable());
        if (tableScanRedirectApplicationResult.isEmpty()) {
            return Result.empty();
        }

        List<Redirection> redirections = tableScanRedirectApplicationResult.get().getRedirections();
        if (redirections.size() != 1) {
            throw new TrinoException(NOT_SUPPORTED, "UNION ALL redirection type is not supported");
        }

        Redirection redirection = getOnlyElement(redirections);
        CatalogSchemaTableName destinationTable = redirection.getDestinationTable();
        Optional<TableHandle> destinationTableHandle = metadata.getTableHandle(
                context.getSession(),
                convertFromSchemaTableName(destinationTable.getCatalogName()).apply(destinationTable.getSchemaTableName()));
        if (destinationTableHandle.isEmpty()) {
            throw new TrinoException(TABLE_NOT_FOUND, format("Destination table %s from table scan redirection not found", destinationTable));
        }
        if (destinationTableHandle.get().equals(scanNode.getTable())) {
            return Result.empty();
        }

        Map<ColumnHandle, String> columnMapping = redirection.getDestinationColumns();
        Map<String, ColumnHandle> destinationColumnHandles = metadata.getColumnHandles(context.getSession(), destinationTableHandle.get());
        Map<Symbol, ColumnHandle> newAssignments = scanNode.getAssignments().entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, entry -> {
                    String destinationColumn = columnMapping.get(entry.getValue());
                    if (destinationColumn == null) {
                        throw new TrinoException(COLUMN_NOT_FOUND, format("Did not find mapping for source column %s in table scan redirection", entry.getValue()));
                    }
                    ColumnHandle destinationColumnHandle = destinationColumnHandles.get(destinationColumn);
                    if (destinationColumnHandle == null) {
                        throw new TrinoException(COLUMN_NOT_FOUND, format("Did not find handle for column %s in destination table %s", destinationColumn, destinationTable));
                    }

                    // validate that redirected types match source types
                    Type sourceType = context.getSymbolAllocator().getTypes().get(entry.getKey());
                    Type redirectedType = metadata.getColumnMetadata(context.getSession(), destinationTableHandle.get(), destinationColumnHandle).getType();
                    if (!sourceType.equals(redirectedType)) {
                        throwTypeMismatchException(
                                destinationTable,
                                destinationColumn,
                                redirectedType,
                                scanNode.getTable(),
                                entry.getValue(),
                                sourceType);
                    }

                    return destinationColumnHandle;
                }));

        TupleDomain<String> requiredFilter = redirection.getFilter();
        if (requiredFilter.isAll()) {
            return Result.ofPlanNode(
                    new TableScanNode(
                            scanNode.getId(),
                            destinationTableHandle.get(),
                            scanNode.getOutputSymbols(),
                            newAssignments,
                            TupleDomain.all(),
                            scanNode.isForDelete()));
        }

        ImmutableMap.Builder<Symbol, ColumnHandle> newAssignmentsBuilder = ImmutableMap.<Symbol, ColumnHandle>builder()
                .putAll(newAssignments);
        ImmutableList.Builder<Symbol> newOutputSymbolsBuilder = ImmutableList.<Symbol>builder()
                .addAll(scanNode.getOutputSymbols());
        Map<ColumnHandle, Symbol> inverseAssignments = ImmutableBiMap.copyOf(scanNode.getAssignments()).inverse();
        Map<String, ColumnHandle> inverseColumnsMapping = ImmutableBiMap.copyOf(columnMapping).inverse();
        TupleDomain<Symbol> transformedConstraint = requiredFilter.transform(destinationColumn -> {
            ColumnHandle sourceColumnHandle = inverseColumnsMapping.get(destinationColumn);
            if (sourceColumnHandle == null) {
                throw new TrinoException(COLUMN_NOT_FOUND, format("Did not find mapping for destination column %s in table scan redirection", destinationColumn));
            }
            Symbol symbol = inverseAssignments.get(sourceColumnHandle);
            if (symbol != null) {
                // symbol should be mapped in redirected table scan
                return symbol;
            }

            // validate that redirected types match source types
            Type domainType = requiredFilter.getDomains().get().get(destinationColumn).getType();
            ColumnHandle destinationColumnHandle = destinationColumnHandles.get(destinationColumn);
            Type redirectedType = metadata.getColumnMetadata(context.getSession(), destinationTableHandle.get(), destinationColumnHandle).getType();
            if (!domainType.equals(redirectedType)) {
                throwTypeMismatchException(
                        destinationTable,
                        destinationColumn,
                        redirectedType,
                        scanNode.getTable(),
                        sourceColumnHandle,
                        domainType);
            }

            // Column pruning after predicate is pushed into table scan can remove assignments for filter columns from the scan node
            symbol = context.getSymbolAllocator().newSymbol(destinationColumn, domainType);
            if (destinationColumnHandle == null) {
                throw new TrinoException(COLUMN_NOT_FOUND, format("Did not find handle for column %s in destination table %s", destinationColumn, destinationTable));
            }
            newAssignmentsBuilder.put(symbol, destinationColumnHandle);
            newOutputSymbolsBuilder.add(symbol);
            return symbol;
        });

        List<Symbol> newOutputSymbols = newOutputSymbolsBuilder.build();
        TableScanNode newScanNode = new TableScanNode(
                scanNode.getId(),
                destinationTableHandle.get(),
                newOutputSymbols,
                newAssignmentsBuilder.build(),
                TupleDomain.all(),
                scanNode.isForDelete());

        FilterNode filterNode = new FilterNode(
                context.getIdAllocator().getNextId(),
                newScanNode,
                domainTranslator.toPredicate(transformedConstraint));
        if (newOutputSymbols.size() == scanNode.getOutputSymbols().size()) {
            return Result.ofPlanNode(filterNode);
        }

        return Result.ofPlanNode(
                new ProjectNode(
                        context.getIdAllocator().getNextId(),
                        filterNode,
                        Assignments.identity(scanNode.getOutputSymbols())));
    }

    private static void throwTypeMismatchException(
            CatalogSchemaTableName destinationTable,
            String destinationColumn,
            Type destinationType,
            TableHandle sourceTable,
            ColumnHandle sourceColumnHandle,
            Type sourceType)
    {
        throw new TrinoException(TYPE_MISMATCH, format(
                "Redirected column %s.%s has type %s, different from source column %s.%s type: %s",
                destinationTable,
                destinationColumn,
                destinationType,
                // TODO report source table and column name instead of ConnectorTableHandle, ConnectorColumnHandle toString
                sourceTable,
                sourceColumnHandle,
                sourceType));
    }
}
