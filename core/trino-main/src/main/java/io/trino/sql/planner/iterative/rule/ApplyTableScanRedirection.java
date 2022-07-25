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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.trino.Session;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.TableHandle;
import io.trino.metadata.TableMetadata;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.TableScanRedirectApplicationResult;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.DomainTranslator;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.tree.Cast;
import io.trino.type.TypeCoercion;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.trino.metadata.QualifiedObjectName.convertFromSchemaTableName;
import static io.trino.spi.StandardErrorCode.COLUMN_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.FUNCTION_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.trino.sql.analyzer.TypeSignatureTranslator.toSqlType;
import static io.trino.sql.planner.plan.Patterns.tableScan;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ApplyTableScanRedirection
        implements Rule<TableScanNode>
{
    private static final Pattern<TableScanNode> PATTERN = tableScan()
            .matching(node -> !node.isUpdateTarget());

    private final PlannerContext plannerContext;
    private final TypeCoercion typeCoercion;

    public ApplyTableScanRedirection(PlannerContext plannerContext)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        this.typeCoercion = new TypeCoercion(plannerContext.getTypeManager()::getType);
    }

    @Override
    public Pattern<TableScanNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(TableScanNode scanNode, Captures captures, Context context)
    {
        Optional<TableScanRedirectApplicationResult> tableScanRedirectApplicationResult = plannerContext.getMetadata().applyTableScanRedirect(context.getSession(), scanNode.getTable());
        if (tableScanRedirectApplicationResult.isEmpty()) {
            return Result.empty();
        }

        CatalogSchemaTableName destinationTable = tableScanRedirectApplicationResult.get().getDestinationTable();

        QualifiedObjectName destinationObjectName = convertFromSchemaTableName(destinationTable.getCatalogName()).apply(destinationTable.getSchemaTableName());
        Optional<QualifiedObjectName> redirectedObjectName = plannerContext.getMetadata().getRedirectionAwareTableHandle(context.getSession(), destinationObjectName).getRedirectedTableName();

        redirectedObjectName.ifPresent(name -> {
            throw new TrinoException(NOT_SUPPORTED, format("Further redirection of destination table '%s' to '%s' is not supported", destinationObjectName, name));
        });

        TableMetadata tableMetadata = plannerContext.getMetadata().getTableMetadata(context.getSession(), scanNode.getTable());
        CatalogSchemaTableName sourceTable = new CatalogSchemaTableName(tableMetadata.getCatalogName().getCatalogName(), tableMetadata.getTable());
        if (destinationTable.equals(sourceTable)) {
            return Result.empty();
        }

        Optional<TableHandle> destinationTableHandle = plannerContext.getMetadata().getTableHandle(
                context.getSession(),
                convertFromSchemaTableName(destinationTable.getCatalogName()).apply(destinationTable.getSchemaTableName()));
        if (destinationTableHandle.isEmpty()) {
            throw new TrinoException(TABLE_NOT_FOUND, format("Destination table %s from table scan redirection not found", destinationTable));
        }

        Map<ColumnHandle, String> columnMapping = tableScanRedirectApplicationResult.get().getDestinationColumns();
        Map<String, ColumnHandle> destinationColumnHandles = plannerContext.getMetadata().getColumnHandles(context.getSession(), destinationTableHandle.get());
        ImmutableMap.Builder<Symbol, Cast> casts = ImmutableMap.builder();
        ImmutableMap.Builder<Symbol, ColumnHandle> newAssignmentsBuilder = ImmutableMap.builder();
        for (Map.Entry<Symbol, ColumnHandle> assignment : scanNode.getAssignments().entrySet()) {
            String destinationColumn = columnMapping.get(assignment.getValue());
            if (destinationColumn == null) {
                throw new TrinoException(COLUMN_NOT_FOUND, format("Did not find mapping for source column %s in table scan redirection", assignment.getValue()));
            }
            ColumnHandle destinationColumnHandle = destinationColumnHandles.get(destinationColumn);
            if (destinationColumnHandle == null) {
                throw new TrinoException(COLUMN_NOT_FOUND, format("Did not find handle for column %s in destination table %s", destinationColumn, destinationTable));
            }

            // insert ts if redirected types don't match source types
            Type sourceType = context.getSymbolAllocator().getTypes().get(assignment.getKey());
            Type redirectedType = plannerContext.getMetadata().getColumnMetadata(context.getSession(), destinationTableHandle.get(), destinationColumnHandle).getType();
            if (!sourceType.equals(redirectedType)) {
                Symbol redirectedSymbol = context.getSymbolAllocator().newSymbol(destinationColumn, redirectedType);
                Cast cast = getCast(
                        context.getSession(),
                        destinationTable,
                        destinationColumn,
                        redirectedType,
                        redirectedSymbol,
                        sourceTable,
                        assignment.getValue(),
                        sourceType);
                casts.put(assignment.getKey(), cast);
                newAssignmentsBuilder.put(redirectedSymbol, destinationColumnHandle);
            }
            else {
                newAssignmentsBuilder.put(assignment.getKey(), destinationColumnHandle);
            }
        }

        TupleDomain<String> requiredFilter = tableScanRedirectApplicationResult.get().getFilter();
        if (requiredFilter.isAll()) {
            ImmutableMap<Symbol, ColumnHandle> newAssignments = newAssignmentsBuilder.buildOrThrow();
            return Result.ofPlanNode(applyProjection(
                    context.getIdAllocator(),
                    ImmutableSet.copyOf(scanNode.getOutputSymbols()),
                    casts.buildOrThrow(),
                    new TableScanNode(
                            scanNode.getId(),
                            destinationTableHandle.get(),
                            ImmutableList.copyOf(newAssignments.keySet()),
                            newAssignments,
                            TupleDomain.all(),
                            Optional.empty(), // Use table statistics from destination table
                            scanNode.isUpdateTarget(),
                            Optional.empty())));
        }

        Map<ColumnHandle, Symbol> inverseAssignments = ImmutableBiMap.copyOf(scanNode.getAssignments()).inverse();
        Map<String, ColumnHandle> inverseColumnsMapping = ImmutableBiMap.copyOf(columnMapping).inverse();
        TupleDomain<Symbol> transformedConstraint = requiredFilter.transformKeys(destinationColumn -> {
            ColumnHandle sourceColumnHandle = inverseColumnsMapping.get(destinationColumn);
            if (sourceColumnHandle == null) {
                throw new TrinoException(COLUMN_NOT_FOUND, format("Did not find mapping for destination column %s in table scan redirection", destinationColumn));
            }
            Symbol symbol = inverseAssignments.get(sourceColumnHandle);
            if (symbol != null) {
                // domain symbol should already be mapped in redirected table scan
                return symbol;
            }

            // Column pruning after predicate is pushed into table scan can remove assignments for filter columns from the scan node
            Type domainType = requiredFilter.getDomains().get().get(destinationColumn).getType();
            symbol = context.getSymbolAllocator().newSymbol(destinationColumn, domainType);

            ColumnHandle destinationColumnHandle = destinationColumnHandles.get(destinationColumn);
            if (destinationColumnHandle == null) {
                throw new TrinoException(COLUMN_NOT_FOUND, format("Did not find handle for column %s in destination table %s", destinationColumn, destinationTable));
            }

            // insert casts if redirected types don't match domain types
            Type redirectedType = plannerContext.getMetadata().getColumnMetadata(context.getSession(), destinationTableHandle.get(), destinationColumnHandle).getType();
            if (!domainType.equals(redirectedType)) {
                Symbol redirectedSymbol = context.getSymbolAllocator().newSymbol(destinationColumn, redirectedType);
                Cast cast = getCast(
                        context.getSession(),
                        destinationTable,
                        destinationColumn,
                        redirectedType,
                        redirectedSymbol,
                        sourceTable,
                        sourceColumnHandle,
                        domainType);
                casts.put(symbol, cast);
                newAssignmentsBuilder.put(redirectedSymbol, destinationColumnHandle);
            }
            else {
                newAssignmentsBuilder.put(symbol, destinationColumnHandle);
            }

            return symbol;
        });

        Map<Symbol, ColumnHandle> newAssignments = newAssignmentsBuilder.buildOrThrow();
        TableScanNode newScanNode = new TableScanNode(
                scanNode.getId(),
                destinationTableHandle.get(),
                ImmutableList.copyOf(newAssignments.keySet()),
                newAssignments,
                TupleDomain.all(),
                Optional.empty(), // Use table statistics from destination table
                scanNode.isUpdateTarget(),
                Optional.empty());

        DomainTranslator domainTranslator = new DomainTranslator(plannerContext);
        FilterNode filterNode = new FilterNode(
                context.getIdAllocator().getNextId(),
                applyProjection(
                        context.getIdAllocator(),
                        newAssignments.keySet(),
                        casts.buildOrThrow(),
                        newScanNode),
                domainTranslator.toPredicate(context.getSession(), transformedConstraint));

        return Result.ofPlanNode(applyProjection(
                context.getIdAllocator(),
                ImmutableSet.copyOf(scanNode.getOutputSymbols()),
                ImmutableMap.of(),
                filterNode));
    }

    private PlanNode applyProjection(
            PlanNodeIdAllocator idAllocator,
            Set<Symbol> requiredSymbols,
            Map<Symbol, Cast> casts,
            PlanNode source)
    {
        if (casts.isEmpty() && requiredSymbols.equals(ImmutableSet.copyOf(source.getOutputSymbols()))) {
            return source;
        }

        return new ProjectNode(
                idAllocator.getNextId(),
                source,
                Assignments.builder()
                        .putIdentities(Sets.difference(requiredSymbols, casts.keySet()))
                        .putAll(casts)
                        .build());
    }

    private Cast getCast(
            Session session,
            CatalogSchemaTableName destinationTable,
            String destinationColumn,
            Type destinationType,
            Symbol destinationSymbol,
            CatalogSchemaTableName sourceTable,
            ColumnHandle sourceColumnHandle,
            Type sourceType)
    {
        try {
            plannerContext.getMetadata().getCoercion(session, destinationType, sourceType);
        }
        catch (TrinoException e) {
            throw new TrinoException(FUNCTION_NOT_FOUND, format(
                    "Cast not possible from redirected column %s.%s with type %s to source column %s.%s with type: %s",
                    destinationTable,
                    destinationColumn,
                    destinationType,
                    sourceTable,
                    // TODO report source column name instead of ColumnHandle toString
                    sourceColumnHandle,
                    sourceType));
        }

        return new Cast(
                destinationSymbol.toSymbolReference(),
                toSqlType(sourceType),
                false,
                typeCoercion.isTypeOnlyCoercion(destinationType, sourceType));
    }
}
