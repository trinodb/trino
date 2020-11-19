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
package io.prestosql.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.TableHandle;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.CatalogSchemaTableName;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.TableScanRedirectApplicationResult;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.planner.DomainTranslator;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.plan.Assignments;
import io.prestosql.sql.planner.plan.FilterNode;
import io.prestosql.sql.planner.plan.ProjectNode;
import io.prestosql.sql.planner.plan.TableScanNode;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.prestosql.metadata.QualifiedObjectName.convertFromSchemaTableName;
import static io.prestosql.spi.StandardErrorCode.COLUMN_NOT_FOUND;
import static io.prestosql.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.prestosql.spi.StandardErrorCode.TYPE_MISMATCH;
import static io.prestosql.sql.planner.plan.Patterns.tableScan;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ApplyTableScanRedirection
        implements Rule<TableScanNode>
{
    private static final Pattern<TableScanNode> PATTERN = tableScan();

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

        CatalogSchemaTableName destinationTable = tableScanRedirectApplicationResult.get().getDestinationTable();
        Optional<TableHandle> destinationTableHandle = metadata.getTableHandle(
                context.getSession(),
                convertFromSchemaTableName(destinationTable.getCatalogName()).apply(destinationTable.getSchemaTableName()));
        if (destinationTableHandle.isEmpty()) {
            throw new PrestoException(TABLE_NOT_FOUND, format("Destination table %s from table scan redirection not found", destinationTable));
        }
        if (destinationTableHandle.get().equals(scanNode.getTable())) {
            return Result.empty();
        }

        Map<ColumnHandle, String> columnMapping = tableScanRedirectApplicationResult.get().getDestinationColumns();
        Map<String, ColumnHandle> destinationColumnHandles = metadata.getColumnHandles(context.getSession(), destinationTableHandle.get());
        Map<Symbol, ColumnHandle> newAssignments = scanNode.getAssignments().entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, entry -> {
                    String destinationColumn = columnMapping.get(entry.getValue());
                    if (destinationColumn == null) {
                        throw new PrestoException(COLUMN_NOT_FOUND, format("Did not find mapping for source column %s in table scan redirection", entry.getValue()));
                    }
                    ColumnHandle destinationColumnHandle = destinationColumnHandles.get(destinationColumn);
                    if (destinationColumnHandle == null) {
                        throw new PrestoException(COLUMN_NOT_FOUND, format("Did not find handle for column %s in destination table %s", destinationColumn, destinationTable));
                    }

                    // validate that redirected types match source types
                    Type sourceType = context.getSymbolAllocator().getTypes().get(entry.getKey());
                    Type redirectedType = metadata.getColumnMetadata(context.getSession(), destinationTableHandle.get(), destinationColumnHandle).getType();
                    if (!sourceType.equals(redirectedType)) {
                        throw new PrestoException(
                                TYPE_MISMATCH,
                                format("Redirected type (%s) for column %s and table %s does not match source type (%s) for source column %s and table %s",
                                        redirectedType,
                                        destinationColumn,
                                        destinationTable,
                                        sourceType,
                                        entry.getValue(),
                                        scanNode.getTable()));
                    }

                    return destinationColumnHandle;
                }));

        TupleDomain<String> requiredFilter = tableScanRedirectApplicationResult.get().getFilter();
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
                throw new PrestoException(COLUMN_NOT_FOUND, format("Did not find mapping for destination column %s in table scan redirection", destinationColumn));
            }
            Symbol symbol = inverseAssignments.get(sourceColumnHandle);
            if (symbol != null) {
                // symbol should be mapped in redirected table scan
                return symbol;
            }
            // Column pruning after PPD into table scan can remove assignments for filter columns from the scan node
            Type domainType = requiredFilter.getDomains().get().get(destinationColumn).getType();
            symbol = context.getSymbolAllocator().newSymbol(destinationColumn, domainType);
            ColumnHandle destinationColumnHandle = destinationColumnHandles.get(destinationColumn);
            if (destinationColumnHandle == null) {
                throw new PrestoException(COLUMN_NOT_FOUND, format("Did not find handle for column %s in destination table %s", destinationColumn, destinationTable));
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
}
