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
package io.trino.sql.planner.optimizations.ctereuse;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import io.trino.Session;
import io.trino.metadata.Metadata;
import io.trino.metadata.TableHandle;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.UnificationResult;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.sql.PlannerContext;
import io.trino.sql.dialect.trino.Attributes;
import io.trino.sql.dialect.trino.Context;
import io.trino.sql.dialect.trino.ProgramBuilder;
import io.trino.sql.dialect.trino.ScalarProgramBuilder;
import io.trino.sql.dialect.trino.operation.Exchange;
import io.trino.sql.dialect.trino.operation.Filter;
import io.trino.sql.dialect.trino.operation.Output;
import io.trino.sql.dialect.trino.operation.Project;
import io.trino.sql.dialect.trino.operation.Query;
import io.trino.sql.dialect.trino.operation.Return;
import io.trino.sql.dialect.trino.operation.TableScan;
import io.trino.sql.dialect.trino.operation.TrinoOperation;
import io.trino.sql.ir.Expression;
import io.trino.sql.newir.Block;
import io.trino.sql.newir.FormatOptions;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Program;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.SourceNode;
import io.trino.sql.newir.Value;
import io.trino.sql.planner.DomainTranslator;
import io.trino.sql.planner.Plan;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Multimaps.toMultimap;
import static io.trino.spi.StandardErrorCode.IR_ERROR;
import static io.trino.spi.type.EmptyRowType.EMPTY_ROW;
import static io.trino.sql.dialect.trino.Attributes.COLUMN_HANDLES;
import static io.trino.sql.dialect.trino.Attributes.EXCHANGE_SCOPE;
import static io.trino.sql.dialect.trino.Attributes.ExchangeScope.LOCAL;
import static io.trino.sql.dialect.trino.Attributes.ExchangeScope.REMOTE;
import static io.trino.sql.dialect.trino.Attributes.ExchangeType.REPARTITION;
import static io.trino.sql.dialect.trino.Attributes.TABLE_HANDLE;
import static io.trino.sql.dialect.trino.Attributes.UPDATE_TARGET;
import static io.trino.sql.dialect.trino.Attributes.USE_CONNECTOR_NODE_PARTITIONING;
import static io.trino.sql.dialect.trino.RelationalProgramBuilder.relationRowType;
import static io.trino.sql.dialect.trino.TrinoDialect.irType;
import static io.trino.sql.dialect.trino.TrinoDialect.trinoType;
import static io.trino.sql.newir.Region.singleBlockRegion;
import static io.trino.sql.planner.SystemPartitioningHandle.ARBITRARY_DISTRIBUTION;
import static io.trino.sql.planner.optimizations.ctereuse.AssignmentsUtils.getEmptyFieldSelector;
import static io.trino.sql.planner.optimizations.ctereuse.AssignmentsUtils.getFullPassthroughFieldSelector;
import static io.trino.sql.planner.optimizations.ctereuse.AssignmentsUtils.getPassthroughMapping;
import static io.trino.sql.planner.optimizations.ctereuse.AssignmentsUtils.getPrunedFields;
import static io.trino.sql.planner.optimizations.ctereuse.AssignmentsUtils.getPruningAssignments;
import static io.trino.sql.planner.optimizations.ctereuse.AssignmentsUtils.getReorderingAssignments;
import static io.trino.sql.planner.optimizations.ctereuse.PredicateUtils.conjunction;
import static io.trino.sql.planner.optimizations.ctereuse.PredicateUtils.disjunction;
import static io.trino.sql.planner.optimizations.ctereuse.PredicateUtils.extractConjuncts;
import static io.trino.sql.planner.optimizations.ctereuse.PredicateUtils.isTrue;
import static io.trino.sql.planner.optimizations.ctereuse.PredicateUtils.optimizeLogicalOperations;
import static io.trino.sql.planner.optimizations.ctereuse.PredicateUtils.removeConjuncts;
import static io.trino.sql.planner.optimizations.ctereuse.PredicateUtils.truePredicate;
import static io.trino.sql.planner.optimizations.ctereuse.RewriteUtils.extractReferencedFields;
import static io.trino.sql.planner.optimizations.ctereuse.RewriteUtils.rebaseBlock;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

public class CteReuse
{
    private CteReuse()
    {}

    public static Optional<Program> reuseCommonSubqueries(Plan plan, PlannerContext plannerContext, Session session, FormatOptions formatOptions)
    {
        // rewrite to new IR
        Program program;
        try {
            program = ProgramBuilder.buildProgram(plan.getRoot());
        }
        catch (UnsupportedOperationException | TrinoException e) {
            return Optional.empty();
        }

        // proceed only if this is a SELECT statement. This is true if all tables have updateTarget == false
        if (hasUpdateTarget(program)) {
            return Optional.empty();
        }

        Set<TableScan> topLevelTableScanOperations = getTopLevelTableScanOperations(program);

        // group TableScan operations by catalog
        Multimap<CatalogHandle, TableScan> tableScansByCatalog = topLevelTableScanOperations.stream()
                .collect(toMultimap(
                        tableScan -> TABLE_HANDLE.getAttribute(tableScan.attributes()).catalogHandle(),
                        identity(),
                        ArrayListMultimap::create));

        // find compatible subgroups for each connector. singleton subgroups are excluded
        List<UnifiedGroup> unifiedGroups = tableScansByCatalog.asMap().values().stream()
                .flatMap(catalogTables -> unifyTableSubgroups(catalogTables, plannerContext.getMetadata(), session).stream())
                .collect(toImmutableList());

        if (unifiedGroups.isEmpty()) {
            return Optional.empty();
        }

        // initialize the (operation -> downstream operations) map for the original program
        // it will be used to traverse the original program and will not be updated when we change parts of the program
        Multimap<Operation, Operation> usesMap = buildUsesMap(program);

        // initialize a ValueNameAllocator compatible with the original program
        // when we create new values using this allocator, they will be ready to incorporate
        // in the original program without causing duplicate name issues
        ProgramBuilder.ValueNameAllocator nameAllocator = initializeNameAllocator(program);

        // merge each group
        ImmutableSet.Builder<Operation> newOperations = ImmutableSet.builder();
        for (UnifiedGroup unifiedGroup : unifiedGroups) {
            // unified TableScan is the first unified operation
            TableScan unifiedTableScan = getUnifiedTableScan(unifiedGroup, nameAllocator);
            newOperations.add(unifiedTableScan);

            // initialize traversal context for each branch on top of the unified TableScan
            List<TraversalContext> traversalContexts = initializeTraversalContexts(unifiedGroup, unifiedTableScan, plannerContext.getMetadata(), nameAllocator);

            // initialize traversal state for each branch by combining the TraversalContext with the next downstream operation
            ImmutableList.Builder<TraversalState> traversalStates = ImmutableList.builder();
            for (int i = 0; i < unifiedGroup.tableScans().size(); i++) {
                traversalStates.add(new TraversalState(traversalContexts.get(i), getNextOperation(unifiedGroup.tableScans().get(i), usesMap).orElseThrow()));
            }

            mergeGroupRecursively(
                    unifiedTableScan,
                    traversalStates.build(),
                    null,
                    USE_CONNECTOR_NODE_PARTITIONING.getAttribute(unifiedTableScan.attributes()),
                    usesMap,
                    nameAllocator,
                    newOperations);
        }

        // create the new plan consisting of old and new operations
        Block oldMainBlock = ((Query) program.getRoot()).query();
        Block newMainBlock = layoutOperations(oldMainBlock, newOperations.build());
        Program newProgram = new Program(((Query) program.getRoot()).withRegions(ImmutableList.of(singleBlockRegion(newMainBlock))), ImmutableMap.of());

        System.out.println("\n\n\nBEFORE\n" + program.print(1, formatOptions) + "\n\n\n");
        System.out.println("\n\n\nAFTER\n" + newProgram.print(1, formatOptions) + "\n\n\n");

        return Optional.empty();
    }

    private static boolean hasUpdateTarget(Program program)
    {
        Operation root = program.getRoot();
        return hasUpdateTarget(root);
    }

    private static boolean hasUpdateTarget(Operation operation)
    {
        if (operation instanceof TableScan tableScan && UPDATE_TARGET.getAttribute(tableScan.attributes())) {
            return true;
        }
        for (Region region : operation.regions()) {
            for (Block block : region.blocks()) {
                for (Operation nested : block.operations()) {
                    if (hasUpdateTarget(nested)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /**
     * We only search for TableScan operations on the top level of the program. We don't search recursively in nested Blocks.
     * <p>
     * Correlated subqueries are represented as nested Blocks. Currently, a valid optimized plan does not contain any correlation,
     * and Trino cannot execute correlated queries without de-correlating them first.
     * As soon as Trino can execute correlated queries, the correlated TableScan operations
     * will be very good candidates for CTE reuse because they are executed multiple times, once for each input row.
     */
    private static Set<TableScan> getTopLevelTableScanOperations(Program program)
    {
        Block topLevelBlock = ((Query) program.getRoot()).query();

        return topLevelBlock.operations().stream()
                .filter(TableScan.class::isInstance)
                .map(TableScan.class::cast)
                .collect(toImmutableSet());
    }

    /**
     * Given a collection of tables: [T1, T2, T3, T4, T5, T6], the algorithm identifies groups of compatible tables,
     * and unifies each group in a sequence of `unifyTables()` operations.
     * <p>
     * For example, if T1, T2, and T5 are compatible, they are unified in two steps:
     * - unifyTables(T1, T2) --> Result1(Unified1, Compensation1, Compensation2)
     * - unifyTables(Unified1, T5) --> Result2(Unified2, Compensation3, Compensation4)
     * <p>
     * Result2 is the final unification result for the whole group, and Unified2 is the unified table handle.
     * To restore T1 from unified2, we must compose Compensation3 with Compensation1.
     * To restore T2 from unified2, we must compose Compensation3 with Compensation2.
     * To restore T5 from unified2, we must apply Compensation4.
     * <p>
     * After each successful `unifyTables()` call, the unification result is recorded
     * under the second one of the unified tables:
     * - Result1 is recorded under "2",
     * - Result2 is recorded under "5".
     * <p>
     * The final unification result, recorded under "5", has the unified table handle
     * for the whole group.
     * To get the right compensations for all tables in the group, we must visit
     * the final unification result as well as the intermediate results.
     * <p>
     * Note: the first table of a group has no unification result recorded.
     * It also applies to singleton groups, but those are filtered out from the result.
     * <p>
     * Note: generally, TableScan operations may have duplicates in their ColumnHandle list,
     * which means that one column handle backs multiple output fields. We exclude those TableScans.
     * This is for the purpose of having a clear 1-1 correspondence between the columns of the unified TableScan
     * and the column of the component TableScans. It is important when handling the pruning projections.
     */
    private static List<UnifiedGroup> unifyTableSubgroups(Collection<TableScan> tableScans, Metadata metadata, Session session)
    {
        // exclude tables that have duplicates in the column handles list
        // TODO also exclude tables where scans are non-deterministic
        List<TableScan> tables = ImmutableList.copyOf(tableScans).stream()
                .filter(table -> {
                    List<ColumnHandle> columnHandles = COLUMN_HANDLES.getAttribute(table.attributes());
                    return columnHandles.stream().distinct().count() == columnHandles.size();
                })
                .collect(toImmutableList());

        // group id is the index of the first table in the group
        int[] groupIds = new int[tables.size()];
        Arrays.fill(groupIds, -1);
        // attach the unification result to the second one of the unified tables
        ImmutableMap.Builder<Integer, UnificationResult<TableHandle>> unificationResultsBuilder = ImmutableMap.builder();

        for (int i = 0; i < tables.size(); i++) {
            if (groupIds[i] == -1) {
                // this table does not belong to any group yet, start a new group
                groupIds[i] = i;
                TableHandle first = TABLE_HANDLE.getAttribute(tables.get(i).attributes());
                for (int j = i + 1; j < tables.size(); j++) {
                    if (groupIds[j] == -1 && Objects.equals(USE_CONNECTOR_NODE_PARTITIONING.getAttribute(tables.get(i).attributes()), USE_CONNECTOR_NODE_PARTITIONING.getAttribute(tables.get(j).attributes()))) {
                        TableHandle second = TABLE_HANDLE.getAttribute(tables.get(j).attributes());
                        Optional<UnificationResult<TableHandle>> result = metadata.unifyTables(session, first, second);
                        if (result.isPresent()) {
                            // add the table to the group
                            groupIds[j] = i;
                            first = result.orElseThrow().unifiedHandle();
                            unificationResultsBuilder.put(j, result.orElseThrow());
                        }
                    }
                }
            }
        }
        Map<Integer, UnificationResult<TableHandle>> unificationResults = unificationResultsBuilder.buildOrThrow();

        ImmutableListMultimap.Builder<Integer, Integer> groupIndexesBuilder = ImmutableListMultimap.builder();
        for (int i = 0; i < groupIds.length; i++) {
            groupIndexesBuilder.put(groupIds[i], i);
        }

        return groupIndexesBuilder.build().asMap().values().stream()
                .filter(indexCollection -> indexCollection.size() > 1)
                .map(indexCollection -> {
                    List<Integer> indexes = ImmutableList.copyOf(indexCollection);
                    return new UnifiedGroup(
                            indexes.stream()
                                    .map(tables::get)
                                    .collect(toImmutableList()),
                            indexes.subList(1, indexes.size()).stream()
                                    .map(unificationResults::get)
                                    .collect(toImmutableList()));
                })
                .collect(toImmutableList());
    }

    /**
     * Map each operation to all operations that use this operation's result as an argument.
     */
    private static Multimap<Operation, Operation> buildUsesMap(Program program)
    {
        ImmutableListMultimap.Builder<Operation, Operation> usesMap = ImmutableListMultimap.builder();
        buildUsesMap(program.getRoot(), program.getValueMap(), usesMap);
        return usesMap.build();
    }

    private static void buildUsesMap(Operation operation, Map<Value, SourceNode> valueMap, ImmutableListMultimap.Builder<Operation, Operation> builder)
    {
        operation.arguments().stream()
                .forEach(argument -> {
                    SourceNode source = valueMap.get(argument);
                    if (source instanceof Operation sourceOperation) {
                        builder.put(sourceOperation, operation);
                    }
                });
        for (Region region : operation.regions()) {
            for (Block block : region.blocks()) {
                for (Operation nested : block.operations()) {
                    buildUsesMap(nested, valueMap, builder);
                }
            }
        }
    }

    private static ProgramBuilder.ValueNameAllocator initializeNameAllocator(Program program)
    {
        return new ProgramBuilder.ValueNameAllocator(program.getValueMap().keySet().stream()
                .map(Value::name)
                .map(name -> name.substring(1))
                .map(unprefixed -> {
                    try {
                        return OptionalInt.of(Integer.parseInt(unprefixed));
                    }
                    catch (NumberFormatException e) {
                        return OptionalInt.empty();
                    }
                })
                .filter(OptionalInt::isPresent)
                .map(OptionalInt::getAsInt)
                .max(Integer::compare)
                .map(maxLabel -> maxLabel + 1)
                .orElse(0));
    }

    /**
     * Create the unified TableScan operation that will replace the TableScan operations of all the unified branches.
     * <p>
     * Exposed columns:
     * The unified TableScan must expose all columns referenced by either of the component tables.
     * Additionally, it must expose all columns used by the compensation predicates of all the component tables.
     * It is not guaranteed that the compensation predicates use only the referenced columns.
     * Example:
     * Table T1 [a, b, c]
     * -- a predicate (c > 0) is pushed down --> Table T1 [a, b, c] enforcedPredicate = (c > 0)
     * -- a pruning projection for column c is pushed down --> Table T1 [a, b] enforcedPredicate = (c > 0)
     * Table T2 [d, e] enforcedPredicate = (d < 0)
     * unifyTables(T1, T2) --> Table T3 enforcedPredicate = (c > 0 OR d < 0); compensation1 = (c > 0); compensation2 = (d < 0)
     * In order to restore semantics of T1, we must be able to apply compensation1 = (c > 0) on top of unified table T3.
     * For that purpose, we must expose column c, even though it was pruned from Table T1.
     * <p>
     * Note: we do not guarantee to expose all columns used by the enforced predicates.
     */
    private static TableScan getUnifiedTableScan(UnifiedGroup unifiedGroup, ProgramBuilder.ValueNameAllocator nameAllocator)
    {
        // compute a set of referenced column handles from all component tables
        // and determine the output field type for each column handle
        Map<ColumnHandle, Type> typesMap = new HashMap<>();
        for (TableScan tableScan : unifiedGroup.tableScans()) {
            Type outputRowType = relationRowType(trinoType(tableScan.result().type()));
            if (outputRowType instanceof RowType rowType) {
                List<ColumnHandle> columnHandles = COLUMN_HANDLES.getAttribute(tableScan.attributes());
                for (int i = 0; i < rowType.getTypeParameters().size(); i++) {
                    Type previous = typesMap.put(columnHandles.get(i), rowType.getTypeParameters().get(i));
                    if (previous != null && !rowType.getTypeParameters().get(i).equals(previous)) {
                        throw new TrinoException(IR_ERROR, "different types for the same column handle");
                    }
                }
            }
        }
        // compute a set of columns used in compensation predicates
        Multimap<ColumnHandle, Type> predicateHandles = unifiedGroup.unificationResults().stream()
                .flatMap(unificationResult -> Stream.of(
                        unificationResult.firstCompensationFilter(),
                        unificationResult.secondCompensationFilter()))
                .filter(tupleDomain -> !tupleDomain.isNone())
                .flatMap(tupleDomain -> tupleDomain.getDomains().orElseThrow().entrySet().stream())
                .collect(toMultimap(
                        Map.Entry::getKey,
                        entry -> entry.getValue().getType(),
                        HashMultimap::create));
        if (predicateHandles.asMap().values().stream()
                .mapToInt(Collection::size)
                .max()
                .orElse(0) > 1) {
            throw new TrinoException(IR_ERROR, "different types for the same column handle");
        }
        predicateHandles.entries().stream()
                .forEach(entry -> {
                    Type previous = typesMap.put(entry.getKey(), entry.getValue());
                    if (previous != null && !entry.getValue().equals(previous)) {
                        throw new TrinoException(IR_ERROR, "different types for the same column handle");
                    }
                });

        List<Map.Entry<ColumnHandle, Type>> columnsList = typesMap.entrySet().stream().collect(toImmutableList());

        UnificationResult<TableHandle> unificationResult = unifiedGroup.unificationResults().getLast();

        return new TableScan(
                nameAllocator.newName(),
                columnsList.isEmpty() ? EMPTY_ROW : RowType.anonymous(columnsList.stream()
                        .map(Map.Entry::getValue)
                        .collect(toImmutableList())),
                unificationResult.unifiedHandle(),
                columnsList.stream()
                        .map(Map.Entry::getKey)
                        .collect(toImmutableList()),
                unificationResult.enforcedProperties().filter(),
                // TODO use the method deriveTableStatisticsForPushdown() to get the statistics for the unified TableScan
                Optional.empty(),
                false,
                Optional.ofNullable(USE_CONNECTOR_NODE_PARTITIONING.getAttribute(unifiedGroup.tableScans().getFirst().attributes())));
    }

    /**
     * Initialize traversal contexts for a group of branches after their initial TableScan operations were unified.
     * <p>
     * In each of the branches, the initial TableScan operation will be replaced with the unified TableScan.
     * For each branch, the TraversalContext carries whatever differences there are between the original TableScan
     * and the unified TableScan, so that we can restore the original semantics later.
     * Additionally, the TraversalContext carries the properties of the unified TableScan. It will help us to avoid
     * repetition when we apply filter or limit operations.
     */
    private static List<TraversalContext> initializeTraversalContexts(UnifiedGroup unifiedGroup, TableScan unifiedTableScan, Metadata metadata, ProgramBuilder.ValueNameAllocator nameAllocator)
    {
        ImmutableList.Builder<TraversalContext> resultBuilder = ImmutableList.builder();

        // the enforced properties of the unified table scan are common to all the unified branches
        UnificationResult<TableHandle> finalUnificationResult = unifiedGroup.unificationResults().getLast();
        // prune the parts of enforced filter which are not supported by the exposed columns
        Set<ColumnHandle> unifiedHandlesSet = ImmutableSet.copyOf(COLUMN_HANDLES.getAttribute(unifiedTableScan.attributes()));
        TupleDomain<ColumnHandle> prunedEnforcedFilter = finalUnificationResult.enforcedProperties().filter()
                .filter((columnHandle, domain) -> unifiedHandlesSet.contains(columnHandle));
        Block enforcedPredicate = translateToBlock(unifiedTableScan, prunedEnforcedFilter, metadata, nameAllocator);
        OptionalLong enforcedLimit = finalUnificationResult.enforcedProperties().limit();

        // map ColumnHandles to field indexes in the unifiedTableScan
        ImmutableMap.Builder<ColumnHandle, Integer> unifiedIndexesBuilder = ImmutableMap.builder();
        List<ColumnHandle> unifiedHandles = COLUMN_HANDLES.getAttribute(unifiedTableScan.attributes());
        for (int i = 0; i < unifiedHandles.size(); i++) {
            // the unified TableScan has no duplicate ColumnHandles, as they were de-duplicated in the `getUnifiedTableScan()` method
            unifiedIndexesBuilder.put(unifiedHandles.get(i), i);
        }
        Map<ColumnHandle, Integer> unifiedIndexes = unifiedIndexesBuilder.buildOrThrow();

        // process component tables in reverse order to compose compensations
        TupleDomain<ColumnHandle> currentCompensationPredicate = TupleDomain.all();
        for (int i = unifiedGroup.tableScans().size() - 1; i >= 1; i--) {
            TableScan tableScan = unifiedGroup.tableScans().get(i);
            UnificationResult<TableHandle> unificationResult = unifiedGroup.unificationResults().get(i - 1);
            FieldMapping fieldMapping = computeMapping(tableScan, unifiedIndexes);
            Set<Integer> fieldsToPrune = computeFieldsToPrune(tableScan, unifiedIndexes);
            Block compensationPredicate = translateToBlock(unifiedTableScan, currentCompensationPredicate.intersect(unificationResult.secondCompensationFilter()), metadata, nameAllocator);
            currentCompensationPredicate = currentCompensationPredicate.intersect(unificationResult.firstCompensationFilter());
            resultBuilder.add(new TraversalContext(fieldMapping, fieldsToPrune, compensationPredicate, enforcedPredicate, enforcedLimit));
        }

        // process the first component table
        TableScan tableScan = unifiedGroup.tableScans().get(0);
        FieldMapping fieldMapping = computeMapping(tableScan, unifiedIndexes);
        Set<Integer> fieldsToPrune = computeFieldsToPrune(tableScan, unifiedIndexes);
        Block compensationPredicate = translateToBlock(unifiedTableScan, currentCompensationPredicate, metadata, nameAllocator);
        resultBuilder.add(new TraversalContext(fieldMapping, fieldsToPrune, compensationPredicate, enforcedPredicate, enforcedLimit));

        return resultBuilder.build().reverse();
    }

    /**
     * Translate TupleDomain to Block based on the output type of the provided TableScan.
     * <p>
     * This method uses a temporary hack. It invokes the DomainTranslator to translate the TupleDomain to the old IR: TupleDomain -> Expression.
     * Then we translate from the old IR to the new IR: Expression -> Block. We should translate directly: TupleDomain -> Block.
     * However, the translation is not trivial, and it involves optimization of the created predicate. It will not be migrated to the new IR
     * as part of the CTE reuse POC.
     * TODO rewrite DomainTranslator to new IR
     */
    private static Block translateToBlock(TableScan tableScan, TupleDomain<ColumnHandle> tupleDomain, Metadata metadata, ProgramBuilder.ValueNameAllocator nameAllocator)
    {
        SymbolAllocator symbolAllocator = new SymbolAllocator();
        List<ColumnHandle> columnHandles = COLUMN_HANDLES.getAttribute(tableScan.attributes());
        Type relationRowType = relationRowType(trinoType(tableScan.result().type()));
        ImmutableMap.Builder<ColumnHandle, Symbol> columnMapBuilder = ImmutableMap.builder();
        ImmutableList.Builder<Symbol> symbolListBuilder = ImmutableList.builder();
        for (int i = 0; i < columnHandles.size(); i++) {
            Symbol symbol = symbolAllocator.newSymbol("tmp_hack", relationRowType.getTypeParameters().get(i));
            columnMapBuilder.put(columnHandles.get(i), symbol);
            symbolListBuilder.add(symbol);
        }
        Map<ColumnHandle, Symbol> columnMap = columnMapBuilder.buildOrThrow();
        List<Symbol> symbolList = symbolListBuilder.build();
        TupleDomain<Symbol> symbolTupleDomain = tupleDomain.transformKeys(columnMap::get);
        Expression expression = new DomainTranslator(metadata).toPredicate(symbolTupleDomain);
        Block.Parameter parameter = new Block.Parameter(nameAllocator.newName(), irType(relationRowType));
        Block.Builder blockBuilder = new Block.Builder(Optional.empty(), ImmutableList.of(parameter));
        ImmutableMap.Builder<Symbol, Context.RowField> symbolMapping = ImmutableMap.builder();
        for (int i = 0; i < symbolList.size(); i++) {
            symbolMapping.put(symbolList.get(i), new Context.RowField(parameter, i));
        }
        // rewrite the Expression to block
        expression.accept(
                new ScalarProgramBuilder(nameAllocator, ImmutableMap.builder()),
                new Context(blockBuilder, symbolMapping.buildOrThrow()));
        // add Return operation to finish the Block
        // TODO extract and reuse addReturnOperation() from RelationalProgramBuilder
        Operation recentOperation = blockBuilder.recentOperation();
        Return returnOperation = new Return(nameAllocator.newName(), recentOperation.result(), recentOperation.attributes());
        blockBuilder.addOperation(returnOperation);
        return optimizeLogicalOperations(blockBuilder.build(), nameAllocator);
    }

    /**
     * Compute mapping to rewrite references to the output fields of componentTableScan in terms of the output fields of unifiedTableScan.
     */
    private static FieldMapping computeMapping(TableScan componentTableScan, Map<ColumnHandle, Integer> unifiedIndexes)
    {
        List<ColumnHandle> componentHandles = COLUMN_HANDLES.getAttribute(componentTableScan.attributes());
        if (componentHandles.isEmpty()) {
            return FieldMapping.EMPTY;
        }

        ImmutableMap.Builder<Integer, Integer> indexMapping = ImmutableMap.builder();
        for (int i = 0; i < componentHandles.size(); i++) {
            indexMapping.put(i, unifiedIndexes.get(componentHandles.get(i)));
        }

        return new FieldMapping(indexMapping.buildOrThrow());
    }

    /**
     * Find which fields of the unifiedTableScan were not originally present in the componentTableScan,
     * and should be pruned when we want to restore the componentTableScan semantics.
     * <p>
     * Note: for the componentTableScan, there might be extracted predicate, recorded as TraversalContext.predicateToApply,
     * which uses some fields identified as fields to prune. We won't be able to effectively prune those fields
     * until that predicate is satisfied.
     */
    private static Set<Integer> computeFieldsToPrune(TableScan componentTableScan, Map<ColumnHandle, Integer> unifiedIndexes)
    {
        Set<ColumnHandle> componentHandles = ImmutableSet.copyOf(COLUMN_HANDLES.getAttribute(componentTableScan.attributes()));

        return unifiedIndexes.entrySet().stream()
                .filter(entry -> !componentHandles.contains(entry.getKey()))
                .map(Map.Entry::getValue)
                .collect(toImmutableSet());
    }

    /**
     * Return the _only_ downstream operation of the provided operation. It is the operation that uses this operation's result as an argument.
     * The sourceIndex indicates which argument of the downstream operation the current operation is.
     * <p>
     * Optional.empty() is returned in two cases:
     * - the provided operation's result is not used. This is the case for terminal operations, like Output, or Return, or for dead code
     * - the provided operation has multiple downstream dependencies (diamond shape)
     * For the POC, we aim to perform CTE reuse in a single bottom-up pass, so we should not encounter diamond shape in the program.
     * Later, we might consider handling it.
     * <p>
     * For now, we assume that neither of the cases is achievable. We will not call this method on terminal operations.
     */
    private static Optional<OperationAndIndex> getNextOperation(Operation operation, Multimap<Operation, Operation> usesMap)
    {
        Collection<Operation> uses = usesMap.get(operation);
        if (uses.size() != 1) {
            return Optional.empty();
        }
        Operation nextOperation = getOnlyElement(uses);
        return Optional.of(new OperationAndIndex(nextOperation, nextOperation.arguments().indexOf(operation.result())));
    }

    /**
     * The next downstream operation and the index of the upstream operation as the downstream operation's source.
     */
    private record OperationAndIndex(Operation operation, int sourceIndex)
    {
        private OperationAndIndex
        {
            requireNonNull(operation, "operation is null");
        }
    }

    /**
     * Merge branches on top of the unifiedOperation.
     * <p>
     * Notes about creating new operations.
     * The algorithm creates new relational operations for different purposes:
     * - to represent common part of semantics of the merged branches
     * - to merge operations from different branches
     * - to create a splitting point (remote exchange) when merging is no longer possible
     * - to compensate for differences after merging branches
     * - to wire the rewritten part of the plan to the original plan
     * Generally, each created operation has a new result name obtained from the ValueNameAllocator so that there
     * cannot be a clash between the original operations and the new operations.
     * One exception to this rule is the situation when we wire the old and new parts of the plan:
     * The border operation reuses the original result name so that it fits in with the original downstream plan.
     * When we create the new plan composed of the old and new operations, we always prioritize the new operations
     * so that we use the new border operation instead of the old operation.
     * <p>
     * Not all operations collected in newOperations are guaranteed to be used in the resulting plan.
     * Some of them might become redundant when the algorithm backtracks, or when it created a checkpoint
     * that is never used. This redundancy is not an issue. The resulting plan will be created in the upstream direction,
     * starting from the root (Output) operation. Only those new operations which are achievable through the border operations
     * will be included. The algorithm never backtracks from a path where it created a border operation.
     *
     * @param unifiedOperation -- the recent unified operation. All branches are based on it.
     * @param branches -- a list of TraversalStates to be merged. The order of branches is meaningful.
     * @param checkpoint -- the point to backtrack to when merging fails. It is a remote exchange. Can be null.
     * The states recorded in the checkpoint correspond to the current branches order-wise.
     * @param sourcePartitioned -- indicates whether the unified operation has physical partitioning property. If true, a remote exchange cannot be inserted in the plan.
     * @param usesMap -- map (operation -> downstream operations) in the original plan
     * @param nameAllocator -- ValueNameAllocator needed for creating new operations
     * @param newOperations -- a collection of newly created operations
     */
    private static void mergeGroupRecursively(
            Operation unifiedOperation,
            List<TraversalState> branches,
            UnifiedStates checkpoint,
            boolean sourcePartitioned,
            Multimap<Operation, Operation> usesMap,
            ProgramBuilder.ValueNameAllocator nameAllocator,
            ImmutableSet.Builder<Operation> newOperations)
    {
        checkArgument(branches.size() >= 2, "attempt to merge less than two branches");
        checkArgument(checkpoint == null || branches.size() == checkpoint.residualStates().size(), "checkpoint and branches mismatch");

        // for each branch, ingest operations into context as long as possible
        List<TraversalState> ingestedStates = branches.stream()
                .map(branch -> ingestOperationsIntoContext(branch, unifiedOperation, usesMap, nameAllocator))
                .collect(toImmutableList());

        // each branch is fully ingested. extract the common part of all branches, and add operations for it
        UnifiedStates commonPartMerged = outputCommonSemantics(ingestedStates, unifiedOperation, nameAllocator, newOperations);

        // analyze the next operations for all branches and find subgroups that can be merged
        List<List<Integer>> subgroups = identifySubgroupsToMerge(commonPartMerged.residualStates());

        // case 1: all branches can be merged further
        // no need for a split point. merge the next operations, pull the contexts through and merge the group recursively
        if (subgroups.size() == 1) {
            UnifiedStates nextOperationMerged = outputNextOperation(commonPartMerged.unifiedOperation(), commonPartMerged.residualStates(), nameAllocator, newOperations);
            // determine if the merged operation can be used as a checkpoint
            UnifiedStates updatedCheckpoint = updateCheckpoint(checkpoint, nextOperationMerged, commonPartMerged, sourcePartitioned, nameAllocator, newOperations);
            mergeGroupRecursively(
                    nextOperationMerged.unifiedOperation(),
                    nextOperationMerged.residualStates(),
                    updatedCheckpoint,
                    sourcePartitioned,
                    usesMap,
                    nameAllocator,
                    newOperations);
        }
        // case 2: split merging into subgroups
        // must insert a split point or use a recorded checkpoint. for each subgroup, merge the next operations, pull the contexts through and merge the group recursively
        else {
            // case 2.1: no checkpoint recorded
            if (checkpoint == null) {
                if (sourcePartitioned) {
                    // case 2.1.1: no checkpoint recorded and cannot insert one because it would break the physical properties
                    return;
                }
                // case 2.1.2: no checkpoint recorded. insert arbitrary exchange as a split point. use it as a checkpoint
                Exchange splittingExchange = arbitraryExchange(commonPartMerged.unifiedOperation(), nameAllocator, newOperations);
                for (List<Integer> subgroupIndexes : subgroups) {
                    List<TraversalState> subgroupStates = subgroupIndexes.stream()
                            .map(commonPartMerged.residualStates()::get)
                            .collect(toImmutableList());
                    if (subgroupStates.size() == 1) {
                        compensateAndWire(getOnlyElement(subgroupStates), splittingExchange, nameAllocator, newOperations);
                    }
                    else {
                        // no need to remap the states, as the exchange is pass-through
                        UnifiedStates updatedCheckpoint = new UnifiedStates(splittingExchange, subgroupStates);
                        UnifiedStates nextOperationMerged = outputNextOperation(splittingExchange, subgroupStates, nameAllocator, newOperations);
                        // determine if the merged operation can be used as a checkpoint
                        if (nextOperationMerged.unifiedOperation() instanceof Exchange exchange && EXCHANGE_SCOPE.getAttribute(exchange.attributes()).equals(REMOTE)) {
                            updatedCheckpoint = nextOperationMerged;
                        }
                        mergeGroupRecursively(
                                nextOperationMerged.unifiedOperation(),
                                nextOperationMerged.residualStates(),
                                updatedCheckpoint,
                                sourcePartitioned,
                                usesMap,
                                nameAllocator,
                                newOperations);
                    }
                }
            }
            // case 2.2: there is checkpoint recorded. go back to the checkpoint and try to merge subgroups from there
            else {
                for (List<Integer> subgroupIndexes : subgroups) {
                    List<TraversalState> subgroupStates = subgroupIndexes.stream()
                            .map(checkpoint.residualStates()::get)
                            .collect(toImmutableList());
                    if (subgroupStates.size() == 1) {
                        compensateAndWire(getOnlyElement(subgroupStates), checkpoint.unifiedOperation(), nameAllocator, newOperations);
                    }
                    else {
                        UnifiedStates updatedCheckpoint = new UnifiedStates(checkpoint.unifiedOperation(), subgroupStates);
                        mergeGroupRecursively(
                                checkpoint.unifiedOperation(),
                                subgroupStates,
                                updatedCheckpoint,
                                sourcePartitioned,
                                usesMap,
                                nameAllocator,
                                newOperations);
                    }
                }
            }
        }
    }

    /**
     * Accumulate pruning projections and filters in the TraversalContext.
     */
    private static TraversalState ingestOperationsIntoContext(TraversalState branchState, Operation unifiedOperation, Multimap<Operation, Operation> usesMap, ProgramBuilder.ValueNameAllocator nameAllocator)
    {
        Operation nextOperation = branchState.nextOperation().operation();
        if (nextOperation instanceof Project project && project.isPruning()) {
            Block rebasedAssignments = rebaseBlock(project.assignments(), relationRowType(trinoType(unifiedOperation.result().type())), branchState.traversalContext().fieldMapping(), nameAllocator).orElseThrow();
            TraversalContext newContext = new TraversalContext(
                    getPassthroughMapping(rebasedAssignments).inverse(),
                    getPrunedFields(rebasedAssignments),
                    branchState.traversalContext().predicateToApply(),
                    branchState.traversalContext().enforcedPredicate(),
                    branchState.traversalContext().enforcedLimit());
            return ingestOperationsIntoContext(new TraversalState(newContext, getNextOperation(project, usesMap).orElseThrow()), unifiedOperation, usesMap, nameAllocator);
        }
        else if (nextOperation instanceof Filter filter) { // TODO check if deterministic
            Block newPredicateToApply = rebaseBlock(filter.predicate(), relationRowType(trinoType(unifiedOperation.result().type())), branchState.traversalContext().fieldMapping(), nameAllocator).orElseThrow();
            newPredicateToApply = conjunction(ImmutableList.of(branchState.traversalContext().predicateToApply(), newPredicateToApply), nameAllocator);
            newPredicateToApply = optimizeLogicalOperations(newPredicateToApply, nameAllocator);
            newPredicateToApply = removeConjuncts(newPredicateToApply, branchState.traversalContext().enforcedPredicate(), nameAllocator);
            TraversalContext newContext = new TraversalContext(
                    branchState.traversalContext().fieldMapping(),
                    branchState.traversalContext().fieldsToPrune(),
                    newPredicateToApply,
                    branchState.traversalContext().enforcedPredicate(),
                    branchState.traversalContext().enforcedLimit());
            return ingestOperationsIntoContext(new TraversalState(newContext, getNextOperation(filter, usesMap).orElseThrow()), unifiedOperation, usesMap, nameAllocator);
        }
        return branchState;
    }

    /**
     * Compute the common part of the TraversalContexts, and the residual part of each context. Create operations for the unified part.
     * This method assumes that all provided states are based on baseUnifiedOperation, so they can be safely combined.
     * There must be at least two states provided.
     */
    private static UnifiedStates outputCommonSemantics(List<TraversalState> states, Operation baseUnifiedOperation, ProgramBuilder.ValueNameAllocator nameAllocator, ImmutableSet.Builder<Operation> newOperations)
    {
        checkArgument(states.size() > 1, "at least two branches must be provided for unification");

        List<TraversalContext> contexts = states.stream()
                .map(TraversalState::traversalContext)
                .collect(toImmutableList());

        // all contexts have the same enforced predicate
        Block enforcedPredicate = contexts.getFirst().enforcedPredicate();

        Block unifiedPredicateToApply = disjunction(
                contexts.stream()
                        .map(TraversalContext::predicateToApply)
                        .collect(toImmutableList()),
                nameAllocator);
        unifiedPredicateToApply = optimizeLogicalOperations(unifiedPredicateToApply, nameAllocator);
        // remove the conjuncts that are already enforced
        unifiedPredicateToApply = removeConjuncts(unifiedPredicateToApply, enforcedPredicate, nameAllocator);

        ImmutableList.Builder<Block> residualPredicatesBuilder = ImmutableList.builder();
        for (Block predicateToApply : contexts.stream()
                .map(TraversalContext::predicateToApply)
                .collect(toImmutableList())) {
            predicateToApply = removeConjuncts(predicateToApply, enforcedPredicate, nameAllocator);
            predicateToApply = removeConjuncts(predicateToApply, unifiedPredicateToApply, nameAllocator);
            residualPredicatesBuilder.add(predicateToApply);
        }
        List<Block> residualPredicates = residualPredicatesBuilder.build();

        Set<Integer> unifiedFieldsToPrune = contexts.stream()
                .map(TraversalContext::fieldsToPrune)
                .reduce((first, second) -> ImmutableSet.copyOf(Sets.intersection(first, second)))
                .orElseThrow();
        // do not prune fields if they are used by residual predicates
        // field indexes from different residual predicates are compatible because all predicates are based on baseUnifiedOperation
        Set<Integer> residualPredicateFields = residualPredicates.stream()
                .map(predicate -> extractReferencedFields(predicate, getOnlyElement(predicate.parameters())))
                .flatMap(Set::stream)
                .collect(toImmutableSet());
        unifiedFieldsToPrune = Sets.difference(unifiedFieldsToPrune, residualPredicateFields);

        // build unified operations
        Operation unifiedOperation = baseUnifiedOperation;
        FieldMapping unifiedMapping = FieldMapping.identity(relationRowType(trinoType(unifiedOperation.result().type())));

        // add unified Filter operation
        if (!isTrue(unifiedPredicateToApply)) {
            unifiedOperation = new Filter(
                    nameAllocator.newName(),
                    unifiedOperation.result(),
                    unifiedPredicateToApply.withLabel("^predicate"),
                    unifiedOperation.attributes());
            newOperations.add(unifiedOperation);
            // don't bother about mapping, filter is passthrough
        }

        // add unified Project operation
        if (!unifiedFieldsToPrune.isEmpty()) {
            Block pruningAssignments = getPruningAssignments(relationRowType(trinoType(unifiedOperation.result().type())), unifiedFieldsToPrune, nameAllocator);
            unifiedOperation = new Project(
                    nameAllocator.newName(),
                    unifiedOperation.result(),
                    pruningAssignments,
                    unifiedOperation.attributes());
            unifiedMapping = unifiedMapping.composeWith(getPassthroughMapping(pruningAssignments));
            newOperations.add(unifiedOperation);
        }

        // compute the new enforced predicate and rebase it onto the last unified operation.
        // Note: some conjuncts might no longer be supported after pruning and must be removed.
        Block newEnforcedPredicate = conjunction(ImmutableList.of(enforcedPredicate, unifiedPredicateToApply), nameAllocator);
        newEnforcedPredicate = rebasePredicateAndPruneUnsupportedConjuncts(newEnforcedPredicate, unifiedOperation, unifiedMapping, nameAllocator);

        // compute residual contexts and rebase them on the unified operation.
        ImmutableList.Builder<TraversalState> newStates = ImmutableList.builder();
        for (int i = 0; i < states.size(); i++) {
            TraversalState oldState = states.get(i);
            TraversalContext oldContext = oldState.traversalContext();

            // rebase the old mapping onto the last unified operation
            FieldMapping newFieldMapping = oldContext.fieldMapping().composeWith(unifiedMapping);

            // find the remaining fields to prune and rebase them onto the last unified operation
            Set<Integer> newFieldsToPrune = Sets.difference(oldContext.fieldsToPrune(), unifiedFieldsToPrune);
            newFieldsToPrune = remapIndexes(newFieldsToPrune, unifiedMapping);

            // rebase the residual predicate onto the last unified operation. Note: residual predicates are fully supported: all fields used by them were retained.
            Block newPredicateToApply = residualPredicates.get(i);
            newPredicateToApply = rebaseBlock(newPredicateToApply, relationRowType(trinoType(unifiedOperation.result().type())), unifiedMapping, nameAllocator).orElseThrow();

            TraversalContext newContext = new TraversalContext(newFieldMapping, newFieldsToPrune, newPredicateToApply, newEnforcedPredicate, oldContext.enforcedLimit());
            newStates.add(new TraversalState(newContext, oldState.nextOperation()));
        }

        return new UnifiedStates(unifiedOperation, newStates.build());
    }

    private static Block rebasePredicateAndPruneUnsupportedConjuncts(Block block, Operation baseOperation, FieldMapping fieldMapping, ProgramBuilder.ValueNameAllocator nameAllocator)
    {
        Block optimizedPredicate = optimizeLogicalOperations(block, nameAllocator);
        List<Block> conjuncts = extractConjuncts(optimizedPredicate, nameAllocator);

        Type baseRowType = relationRowType(trinoType(baseOperation.result().type()));
        List<Block> rebasedSupportedConjuncts = conjuncts.stream()
                .map(conjunct -> rebaseBlock(conjunct, baseRowType, fieldMapping, nameAllocator))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(toImmutableList());

        if (rebasedSupportedConjuncts.isEmpty()) {
            return truePredicate(block.name(), ImmutableList.of(new Block.Parameter(nameAllocator.newName(), irType(baseRowType))), nameAllocator);
        }

        return conjunction(rebasedSupportedConjuncts, nameAllocator);
    }

    private static Set<Integer> remapIndexes(Set<Integer> indexes, FieldMapping fieldMapping)
    {
        return indexes.stream()
                .map(fieldMapping::get)
                .collect(toImmutableSet());
    }

    private static List<List<Integer>> identifySubgroupsToMerge(List<TraversalState> branches)
    {
        // TODO ... For now, report each branch as a separate singleton group (as if nothing can be merged)
        // TODO consider side effects, non-deterministic
        // TODO special case to consider: if the next operation is a multi-source one (for example Union, Exchange), it can be _the same_ operation
        //  for multiple branches -- the place where the branches were naturally merged in the original plan
        //  it should be handled per-case
        //  self-join is the same situation but with intervening exchanges - we should detect the pattern
        return IntStream.range(0, branches.size())
                .boxed()
                .map(ImmutableList::of)
                .collect(toImmutableList());
    }

    private static UnifiedStates outputNextOperation(Operation unifiedOperation, List<TraversalState> branches, ProgramBuilder.ValueNameAllocator nameAllocator, ImmutableSet.Builder<Operation> newOperations)
    {
        // TODO added to silence error prone check; remove
        requireNonNull(unifiedOperation);
        requireNonNull(branches);
        requireNonNull(nameAllocator);
        requireNonNull(newOperations);

        // TODO record every new operation in newOperations
        throw new UnsupportedOperationException();
    }

    private static UnifiedStates updateCheckpoint(UnifiedStates currentCheckpoint, UnifiedStates newUnifiedStates, UnifiedStates previousUnifiedStates, boolean sourcePartitioned, ProgramBuilder.ValueNameAllocator nameAllocator, ImmutableSet.Builder<Operation> newOperations)
    {
        if (newUnifiedStates.unifiedOperation() instanceof Exchange exchange) {
            Attributes.ExchangeScope scope = EXCHANGE_SCOPE.getAttribute(exchange.attributes());
            if (scope.equals(REMOTE)) {
                return newUnifiedStates;
            }
            if (currentCheckpoint == null && !sourcePartitioned && scope.equals(LOCAL)) {
                // create a remote exchange right before the local exchange and set it as a checkpoint
                Exchange splittingExchange = arbitraryExchange(previousUnifiedStates.unifiedOperation(), nameAllocator, newOperations);
                return new UnifiedStates(splittingExchange, previousUnifiedStates.residualStates());
            }
        }

        return currentCheckpoint;
    }

    private static Exchange arbitraryExchange(Operation input, ProgramBuilder.ValueNameAllocator nameAllocator, ImmutableSet.Builder<Operation> newOperations)
    {
        String exchangeResultName = nameAllocator.newName();
        Type inputRowType = relationRowType(trinoType(input.result().type()));

        Exchange arbitraryExchange = new Exchange(
                exchangeResultName,
                ImmutableList.of(input.result()),
                ImmutableList.of(getFullPassthroughFieldSelector("^inputSelector", inputRowType, nameAllocator)),
                getEmptyFieldSelector("^boundArguments", inputRowType, nameAllocator),
                getEmptyFieldSelector("^hashSelector", inputRowType, nameAllocator),
                getEmptyFieldSelector("^orderingSelector", inputRowType, nameAllocator),
                REPARTITION,
                REMOTE,
                ARBITRARY_DISTRIBUTION,
                new Attributes.NullableValues(new NullableValue[] {}),
                false,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of(input.attributes()));

        newOperations.add(arbitraryExchange);

        return arbitraryExchange;
    }

    /**
     * Restore the original semantics of the branch on top of the unifiedOperation:
     * - apply remaining predicate
     * - prune additional fields
     * - reorder fields to match the input type of the next downstream operation
     * Wire the new part of the plan to the old plan by replacing the argument of the next downstream operation
     * with the result of the last new operation.
     */
    private static void compensateAndWire(TraversalState branch, Operation unifiedOperation, ProgramBuilder.ValueNameAllocator nameAllocator, ImmutableSet.Builder<Operation> newOperations)
    {
        Operation recentOperation = unifiedOperation;
        FieldMapping mapping = branch.traversalContext().fieldMapping();

        // apply filter
        if (!isTrue(branch.traversalContext().predicateToApply())) {
            recentOperation = new Filter(
                    nameAllocator.newName(),
                    recentOperation.result(),
                    branch.traversalContext().predicateToApply().withLabel("^predicate"),
                    recentOperation.attributes());
            newOperations.add(recentOperation);
        }

        // apply pruning
        if (!branch.traversalContext().fieldsToPrune().isEmpty()) {
            Block pruningAssignments = getPruningAssignments(relationRowType(trinoType(recentOperation.result().type())), branch.traversalContext().fieldsToPrune(), nameAllocator);
            recentOperation = new Project(
                    nameAllocator.newName(),
                    recentOperation.result(),
                    pruningAssignments,
                    recentOperation.attributes());
            newOperations.add(recentOperation);
            mapping = mapping.composeWith(getPassthroughMapping(pruningAssignments));
        }

        // reorder fields to match the next operation's input
        if (!mapping.isIdentity(relationRowType(trinoType(recentOperation.result().type())))) {
            recentOperation = new Project(
                    nameAllocator.newName(),
                    recentOperation.result(),
                    getReorderingAssignments(relationRowType(trinoType(recentOperation.result().type())), mapping, nameAllocator),
                    recentOperation.attributes());
            newOperations.add(recentOperation);
        }

        // compensations are fully applied. wire recentOperation to the nextOperation
        // the operation created by the withArgument() method has the same result as the original operation,
        // therefore it can be used by the downstream plan without further adjustments
        Operation nextOperationWired = ((TrinoOperation) branch.nextOperation().operation()).withArgument(recentOperation.result(), branch.nextOperation().sourceIndex());
        newOperations.add(nextOperationWired);
    }

    private static Block layoutOperations(Block oldMainBlock, Set<Operation> newOperations)
    {
        // find the Output operation of the resulting plan
        Set<Operation> oldOperations = ImmutableSet.copyOf(oldMainBlock.operations());
        Operation rootOperation = newOperations.stream()
                .filter(Output.class::isInstance)
                .findFirst()
                .orElse(oldOperations.stream()
                        .filter(Output.class::isInstance)
                        .findFirst()
                        .orElseThrow());

        Block.Builder newMainBlock = new Block.Builder(oldMainBlock.name(), oldMainBlock.parameters());

        layoutOperations(
                rootOperation,
                newOperations.stream()
                        .collect(toImmutableMap(Operation::result, identity())),
                oldOperations.stream()
                        .collect(toImmutableMap(Operation::result, identity())),
                newMainBlock,
                new HashSet<>());

        return newMainBlock.build();
    }

    /**
     * Build a block representing the updated query with diamond shape.
     * <p>
     * The root operation is the Output operation, being the root of the query plan. Starting from this operation, we recursively output
     * the operation's sources, and then the operation itself. This way we assure the correct layout of the program where each value
     * is declared before it is used.
     * <p>
     * When searching for the source operations, we first check in the newOperations, and then in the oldOperations.
     * We find the operation by its result name. Generally, the oldOperations and the newOperations use different result names.
     * There is one exception to this rule: the operations on the border between the old parts of the program and the rewritten parts of the program
     * are new operations that reuse the old operation's result so that they fit in to the old program.
     * <p>
     * Because there is diamond shape, the common subqueries will be visited multiple times in this method.
     * We layout them once, on the first visit.
     *
     * @param operation -- the Output operation, being the root of the query plan
     * @param newOperations -- the relational operations created by the CTE reuse algorithm
     * @param oldOperations -- all the top-level relational operations from the original program
     * @param block -- a builder of the new top-level block
     * @param alreadyOutputOperations -- results of the operations that are already in the block
     */
    private static void layoutOperations(Operation operation, Map<Value, Operation> newOperations, Map<Value, Operation> oldOperations, Block.Builder block, Set<Value> alreadyOutputOperations)
    {
        if (!alreadyOutputOperations.contains(operation.result())) {
            for (Value value : operation.arguments()) {
                Operation source = newOperations.get(value);
                if (source == null) {
                    source = oldOperations.get(value);
                }
                requireNonNull(source, "source operation not found");
                layoutOperations(source, newOperations, oldOperations, block, alreadyOutputOperations);
            }
            block.addOperation(operation);
            alreadyOutputOperations.add(operation.result());
        }
    }

    /**
     * A group of compatible tables with unification results.
     *
     * @param tableScans -- a list of unified TableScans in the order of unification
     * @param unificationResults -- a list of partial and final unification results in the order of unification
     */
    private record UnifiedGroup(List<TableScan> tableScans, List<UnificationResult<TableHandle>> unificationResults)
    {
        private UnifiedGroup
        {
            requireNonNull(tableScans, "tableScans is null");
            requireNonNull(unificationResults, "unificationResults is null");
            checkArgument(tableScans.size() == unificationResults.size() + 1, "the number of unified tables does not match the number of unification results");
        }
    }

    /**
     * The branch-specific context for the CTE reuse traversal. Logically, it is applicable on top of the recently processed operation in this branch.
     *
     * @param fieldMapping -- the output type of the recent operation might have changed. The mapping serves to update the next operation accordingly
     * @param fieldsToPrune -- additional fields output by the recent operation as the result of unifying with other branches
     * @param predicateToApply -- predicate extracted from this branch as the result of unifying with other branches. Note: it might use fields marked as fieldsToPrune.
     * The predicate is assumed to be optimized.
     * @param enforcedPredicate -- predicate guaranteed for the unified plan. Note: it might not be the full guaranteed predicate.
     * It only contains the conjuncts supported by the output fields of the recent operation. It is used when creating new predicates to avoid repetition.
     * The predicate is assumed to be optimized.
     * @param enforcedLimit -- limit guaranteed for the unified plan
     */
    private record TraversalContext(FieldMapping fieldMapping, Set<Integer> fieldsToPrune, Block predicateToApply, Block enforcedPredicate, OptionalLong enforcedLimit)
    {
        private TraversalContext
        {
            requireNonNull(fieldMapping, "mapping is null");
            requireNonNull(fieldsToPrune, "fieldsToPrune is null");
            requireNonNull(predicateToApply, "predicateToApply is null");
            requireNonNull(enforcedPredicate, "enforcedPredicate is null");
            requireNonNull(enforcedLimit, "enforcedLimit is null");
            fieldsToPrune = ImmutableSet.copyOf(fieldsToPrune);
        }
    }

    /**
     * Traversal state for the branch. It consists of traversal context and the next operation in the branch.
     */
    private record TraversalState(TraversalContext traversalContext, OperationAndIndex nextOperation)
    {
        private TraversalState
        {
            requireNonNull(traversalContext, "traversalContext is null");
            requireNonNull(nextOperation, "nextOperation is null");
        }
    }

    /**
     * Traversal state for the merged group. It consists of the common unified operation and traversal states for all branches in the group.
     */
    private record UnifiedStates(Operation unifiedOperation, List<TraversalState> residualStates)
    {
        private UnifiedStates
        {
            requireNonNull(unifiedOperation, "unifiedOperation is null");
            requireNonNull(residualStates, "residualStates is null");
            residualStates = ImmutableList.copyOf(residualStates);
        }
    }
}
