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
import io.trino.sql.dialect.trino.operation.FieldReference;
import io.trino.sql.dialect.trino.operation.Filter;
import io.trino.sql.dialect.trino.operation.Output;
import io.trino.sql.dialect.trino.operation.Project;
import io.trino.sql.dialect.trino.operation.Query;
import io.trino.sql.dialect.trino.operation.Return;
import io.trino.sql.dialect.trino.operation.Row;
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
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Multimaps.toMultimap;
import static io.trino.spi.StandardErrorCode.IR_ERROR;
import static io.trino.spi.type.EmptyRowType.EMPTY_ROW;
import static io.trino.sql.dialect.trino.Attributes.COLUMN_HANDLES;
import static io.trino.sql.dialect.trino.Attributes.EXCHANGE_SCOPE;
import static io.trino.sql.dialect.trino.Attributes.TABLE_HANDLE;
import static io.trino.sql.dialect.trino.Attributes.UPDATE_TARGET;
import static io.trino.sql.dialect.trino.Attributes.USE_CONNECTOR_NODE_PARTITIONING;
import static io.trino.sql.dialect.trino.RelationalProgramBuilder.relationRowType;
import static io.trino.sql.dialect.trino.TrinoDialect.irType;
import static io.trino.sql.dialect.trino.TrinoDialect.trinoType;
import static io.trino.sql.planner.optimizations.ctereuse.AssignmentsUtils.getEmptyFieldSelector;
import static io.trino.sql.planner.optimizations.ctereuse.AssignmentsUtils.getPassthroughMapping;
import static io.trino.sql.planner.optimizations.ctereuse.AssignmentsUtils.getPrunedFields;
import static io.trino.sql.planner.optimizations.ctereuse.AssignmentsUtils.getPruningAssignments;
import static io.trino.sql.planner.optimizations.ctereuse.AssignmentsUtils.getReorderingAssignments;
import static io.trino.sql.planner.optimizations.ctereuse.RewriteUtils.extractReferencedFields;
import static io.trino.sql.planner.optimizations.ctereuse.RewriteUtils.rebaseBlock;
import static io.trino.sql.newir.Region.singleBlockRegion;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_ARBITRARY_DISTRIBUTION;
import static io.trino.sql.planner.optimizations.ctereuse.PredicateUtils.conjunction;
import static io.trino.sql.planner.optimizations.ctereuse.PredicateUtils.disjunction;
import static io.trino.sql.planner.optimizations.ctereuse.PredicateUtils.isTrue;
import static io.trino.sql.planner.optimizations.ctereuse.PredicateUtils.removeConjuncts;
import static io.trino.sql.planner.optimizations.ctereuse.PredicateUtils.truePredicate;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

public class CteReuse
{
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

        // find compatible subgroups for each connector
        List<UnifiedGroup> unifiedGroups = tableScansByCatalog.asMap().values().stream()
                .flatMap(catalogTables -> unifyTableSubgroups(catalogTables, plannerContext.getMetadata(), session).stream())
                .collect(toImmutableList());

        // initialize the <operation -> downstream operations> map for the original program
        // it will be used to traverse the original program and will not be updated when we change parts of the program
        Multimap<Operation, Operation> usesMap = buildUsesMap(program);

        // initialize ValueNameAllocator to be compatible with the original program
        // when we create new operations using this allocator, they will be ready to incorporate
        // in the original program without causing duplicate name issues
        ProgramBuilder.ValueNameAllocator nameAllocator = initializeNameAllocator(program);

        // unify each group
        ImmutableSet.Builder<Operation> operationsToAddBuilder = ImmutableSet.builder();
        for (UnifiedGroup unifiedGroup : unifiedGroups) {
            // record the old operations to remove and the new operations to add
            ImmutableList.Builder<Operation> operationsToRemove = ImmutableList.builder(); // TODO remove this

            // initialize a list of unified operations
            ImmutableList.Builder<Operation> unifiedOperations = ImmutableList.builder();

            // unified TableScan is the first unified operation
            TableScan unifiedTableScan = getUnifiedTableScan(unifiedGroup, nameAllocator);
            unifiedOperations.add(unifiedTableScan);
            operationsToAddBuilder.add(unifiedTableScan);
            operationsToRemove.addAll(unifiedGroup.tableScans());

            // checkpoint records the recent safe place to backtrack if unifying fails. initially null.
            Checkpoint checkpoint = null;

            // initialize traversal context for each branch on top of the unified TableScan
            List<TraversalContext> traversalContexts = initializeTraversalContexts(unifiedGroup, unifiedTableScan, plannerContext.getMetadata(), nameAllocator);

            // initialize traversal state for each branch by combining the TraversalContext with the next downstream operation
            ImmutableList.Builder<TraversalState> traversalStates = ImmutableList.builder();
            for (int i = 0; i < unifiedGroup.tableScans().size(); i++) {
                traversalStates.add(new TraversalState(traversalContexts.get(i), getNextOperation(unifiedGroup.tableScans().get(i), usesMap).orElseThrow()));
            }

            mergeGroup(
                    traversalStates.build(),
                    unifiedTableScan,
                    checkpoint,
                    USE_CONNECTOR_NODE_PARTITIONING.getAttribute(unifiedTableScan.attributes()),
                    program.getValueMap(),
                    usesMap,
                    nameAllocator,
                    operationsToRemove,
                    operationsToAddBuilder);

            // for each branch: ingest operations until blocked
            // compute common part of contexts and residual contexts
            // output common part of contexts
            // rebase residual contexts on the last output operation
            // for each branch: remap the next operation and try to pull context through. // TODO side effects, non-deterministic
            //  - success for all branches and operations identical --> output unified operation. save checkpoint if necessary. restart the ingest phase with the pulled-through contexts
            //  - failure for some branch or not identical
            //      - insert a split point at the last checkpoint or in-place
            //      - find subgroups at the failure point and start merging those subgroups from the split point - recursively
        }

        Set<Operation> newOperations = operationsToAddBuilder.build();

        Block oldMainBlock = getOnlyElement(((Query) program.getRoot()).regions()).getOnlyBlock();
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
        Program newProgram = new Program(((TrinoOperation) program.getRoot()).withRegions(ImmutableList.of(singleBlockRegion(newMainBlock.build()))), ImmutableMap.of());

        System.out.println(newProgram.print(1, formatOptions));

        // TODO remove dead code, update usesMap (or is it only local?) and valueMap

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
        Block topLevelBlock = getOnlyElement(((Query) program.getRoot()).regions()).getOnlyBlock();
        // TODO add helper in Program to access the top level Query operation
        // TODO add helper in Query to get the only Block

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
     * Initialize traversal context for a group of branches after their initial TableScan operations were unified.
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
                .filter(((columnHandle, domain) -> unifiedHandlesSet.contains(columnHandle)));
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
     * This method uses a temporary hack. It invokes the `DomainTranslator` to translate the TupleDomain to the old IR: TupleDomain -> Expression.
     * Then we translate from the old IR to the new IR: Expression -> Block. We should translate directly: TupleDomain -> Block.
     * However, this translation is not trivial, and it involves optimization of the created predicate. It will not be migrated to the new IR
     * as part of the CTE reuse POC.
     * TODO rewrite DomainTranslator to new IR
     */
    private static Block translateToBlock(TableScan tableScan, TupleDomain<ColumnHandle> tupleDomain, Metadata metadata, ProgramBuilder.ValueNameAllocator nameAllocator)
    // TODO move to helper class for handling predicates
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
        // note: we drop the created value map. We will have to update the program's value map
        // with all newly created values whenever we incorporate the block into the program.
        // also, the block parameter will have to be mapped.
        expression.accept(
                new ScalarProgramBuilder(nameAllocator, ImmutableMap.builder()),
                new Context(blockBuilder, symbolMapping.buildOrThrow()));
        // add Return operation to finish the Block
        Operation recentOperation = blockBuilder.recentOperation();
        Return returnOperation = new Return(nameAllocator.newName(), recentOperation.result(), recentOperation.attributes());
        blockBuilder.addOperation(returnOperation);
        return blockBuilder.build();
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
     * <p>
     * Optional.empty() is returned in two cases:
     * - the provided operation's result is not used. This is the case for terminal operations, like Output, or Return, or for dead code
     * - the provided operation has multiple downstream dependencies (diamond shape)
     * For the POC, we aim to perform CTE reuse in a single bottom-up pass, so we should not encounter diamond shape in the program.
     * Later, we might consider handling it.
     * <p>
     * For now, we assume that neither of the cases is achievable. We will not call this method on terminal operations.
     * // TODO comment that the index is the index of `operation` in the arguments list of the next operation.
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
     * The next downstream operation and the index of the previous operation as the downstream operation's source.
     *
     * @param operation
     * @param index
     */
    private record OperationAndIndex(Operation operation, int index)
    {
        // TODO rename index to chileIndex or sth smarter
        private OperationAndIndex
        {
            requireNonNull(operation, "operation is null");
        }
    }

    // TODO javadoc when method finished
    private static void mergeGroup(
            List<TraversalState> branches,
            Operation unifiedOperation,
            Checkpoint checkpoint,
            boolean sourcePartitioned,
            Map<Value, SourceNode> valueMap,
            Multimap<Operation, Operation> usesMap,
            ProgramBuilder.ValueNameAllocator nameAllocator,
            ImmutableList.Builder<Operation> operationsToRemove,
            ImmutableSet.Builder<Operation> operationsToAdd)
    {
        // for each branch, ingest operations into context as long as possible
        List<TraversalState> ingestedStates = branches.stream()
                .map(branch -> ingestOperationsIntoContext(branch, unifiedOperation, valueMap, usesMap, nameAllocator, operationsToRemove))
                .collect(toImmutableList());

        // each branch is fully ingested. extract the common part of all branches, and add operations for it
        UnifiedStates unifiedStates = unifyAndOutput(ingestedStates, unifiedOperation, valueMap, nameAllocator, operationsToAdd);

        // TODO for now, end merging: insert a split point, apply compensations, and wire to plan
        //  later: rebase the next operations. If all can be merged further and all contexts can be pulled through, no split point.
        //    Just output the next operation, pull contexts through, and enter ingestion phase
        //    NOTE: check that the next operation is single source. Watch out for exchanges!
        //  otherwise, insert a split point. Find subgroups so that the next operation can be merged and contexts can be pulled through.
        //    For each subgroup output the common operation, pull contexts through, and enter ingestion phase
        //  for branches that don't have a subgroup, apply compensations and wire to plan
        if (checkpoint == null) {
            if (sourcePartitioned) {
                throw new UnsupportedOperationException("sorry, cannot merge this at all. It would require inserting an exchange which would break the physical propetries");
            }
            // insert a split point where we are, record it as checkpoint
            Exchange splittingExchange = roundRobinExchange(unifiedStates.unifiedOperation(), nameAllocator);
            operationsToAdd.add(splittingExchange);
            // all TraversalContexts can be pulled through this exchange. We don't need to remap them, as the exchange is pass-through. We only need to base them on the exchange.
            // for each branch, apply compensations on top of the exchange and wire to the Program
            for (TraversalState state : unifiedStates.residualStates()) {
                Operation recentOperation = splittingExchange;
                FieldMapping mapping = state.traversalContext().fieldMapping();
                // apply filter
                if (!isTrue(state.traversalContext().predicateToApply())) {
                    recentOperation = new Filter(
                            nameAllocator.newName(),
                            recentOperation.result(),
                            state.traversalContext().predicateToApply().withLabel("^predicate"),
                            recentOperation.attributes());
                    operationsToAdd.add(recentOperation);
                    // no need to adjust mapping. filter operation is pass-through
                }
                // apply pruning
                if (!state.traversalContext().fieldsToPrune().isEmpty()) {
                    Block pruningAssignments = getPruningAssignments(relationRowType(trinoType(recentOperation.result().type())), state.traversalContext().fieldsToPrune(), nameAllocator);
                    recentOperation = new Project(
                            nameAllocator.newName(),
                            recentOperation.result(),
                            pruningAssignments,
                            recentOperation.attributes());
                    mapping = mapping.composeWith(getPassthroughMapping(pruningAssignments));
                    operationsToAdd.add(recentOperation);
                }
                // okay... now we must reorder fields to match the next operations input. We will use the final mapping.
                // TODO do this as part of the above projection
                if (!mapping.isIdentity(relationRowType(trinoType(recentOperation.result().type())))) {
                    recentOperation = new Project(
                            nameAllocator.newName(),
                            recentOperation.result(),
                            getReorderingAssignments(relationRowType(trinoType(recentOperation.result().type())), mapping, nameAllocator),
                            recentOperation.attributes());
                    operationsToAdd.add(recentOperation);
                }
                // compensations are fully applied. Now we can wire recentOperation to the nextOperation
                // the operation created by the `withArgument()` method has the same result as the original operation.
                // therefore it can be used by the downstream plan without further adjustments. Thanks to the Value indirection, we don't have to remap anything downstream.
                Operation nextOperationWired = ((TrinoOperation) state.nextOperation().operation()).withArgument(recentOperation.result(), state.nextOperation().index());
                operationsToAdd.add(nextOperationWired);
                operationsToRemove.add(state.nextOperation().operation());
            }
            return;
        }
        if (checkpoint.operation() instanceof Exchange exchange) {
            checkState(EXCHANGE_SCOPE.getAttribute(exchange.attributes()).equals(Attributes.ExchangeScope.REMOTE));
            // use this as split point
        }
        // checkpoint is the last operation before the first local exchange
        // check that the next unified operation is a local exchange. Need to be able to navigate to next operation in the newly created part of the program.
        if (sourcePartitioned) {
            throw new UnsupportedOperationException("sorry, cannot merge this at all. It would require inserting an exchange which would break the physical propetries");
        }
        // insert a split point after this operation and before the local exchange. Record it as checkpoint

    }

    private static TraversalState ingestOperationsIntoContext(TraversalState branchState, Operation unifiedOperation, Map<Value, SourceNode> valueMap, Multimap<Operation, Operation> usesMap, ProgramBuilder.ValueNameAllocator nameAllocator, ImmutableList.Builder<Operation> operationsToRemove)
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
            operationsToRemove.add(project);
            return ingestOperationsIntoContext(new TraversalState(newContext, getNextOperation(project, usesMap).orElseThrow()), unifiedOperation, valueMap, usesMap, nameAllocator, operationsToRemove);
        }
        else if (nextOperation instanceof Filter filter) { // TODO check if deterministic
            Block rebasedPredicate = rebaseBlock(getOnlyElement(filter.regions()).getOnlyBlock(), relationRowType(trinoType(unifiedOperation.result().type())), branchState.traversalContext().fieldMapping(), nameAllocator).orElseThrow();
            // TODO do not add the conjunct if it is satisfied by enforced predicate or by predicate to apply
            TraversalContext newContext = new TraversalContext(
                    branchState.traversalContext().fieldMapping(),
                    branchState.traversalContext().fieldsToPrune(),
                    conjunction(ImmutableList.of(branchState.traversalContext().predicateToApply(), rebasedPredicate), nameAllocator),
                    branchState.traversalContext().enforcedPredicate(),
                    branchState.traversalContext().enforcedLimit());
        }
        return branchState;
    }

    /**
     * Compute the common part of the TraversalContexts, and the residual part of each context. Create operations for the unified part.
     * This method assumes that all provided states are based on baseUnifiedOperation, so they can be safely combined.
     * There must be at least two states provided.
     */
    private static UnifiedStates unifyAndOutput(List<TraversalState> states, Operation baseUnifiedOperation, Map<Value, SourceNode> valueMap, ProgramBuilder.ValueNameAllocator nameAllocator, ImmutableSet.Builder<Operation> operationsToAdd)
    {
        checkArgument(states.size() > 1, "at least two branches must be provided for unification");

        List<TraversalContext> contexts = states.stream()
                .map(TraversalState::traversalContext)
                .collect(toImmutableList());

        Block enforcedPredicate = getOnlyElement(contexts.stream()
                .map(TraversalContext::enforcedPredicate)
                .distinct()
                .collect(toImmutableList()));

        Block unifiedPredicateToApply = disjunction(
                contexts.stream()
                        .map(TraversalContext::predicateToApply)
                        .collect(toImmutableList()),
                nameAllocator);
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
            operationsToAdd.add(unifiedOperation);
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
            operationsToAdd.add(unifiedOperation);
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
            Block newPredicateToApply = residualPredicates.get(i); // TODO add predicate optimization where applicable -> PredicateUtils.optimizeLogicalOperations()
            newPredicateToApply = rebaseBlock(newPredicateToApply, relationRowType(trinoType(unifiedOperation.result().type())), unifiedMapping, nameAllocator).orElseThrow();

            TraversalContext newContext = new TraversalContext(newFieldMapping, newFieldsToPrune, newPredicateToApply, newEnforcedPredicate, oldContext.enforcedLimit());
            newStates.add(new TraversalState(newContext, oldState.nextOperation()));
        }

        return new UnifiedStates(unifiedOperation, newStates.build());
    }

    private static Set<Integer> remapIndexes(Set<Integer> indexes, FieldMapping fieldMapping)
    {
        return indexes.stream()
                .map(fieldMapping::get)
                .collect(toImmutableSet());
    }

    // TODO move to Exchange operation
    // TODO do arbitrary: SystemPartitioningHandle.ARBITRARY_DISTRIBUTION
    private static Exchange roundRobinExchange(Operation input, ProgramBuilder.ValueNameAllocator nameAllocator)
    {
        String exchangeResultName = nameAllocator.newName();

        // input selector block: select all fields from input
        Type inputRowType = relationRowType(trinoType(input.result().type()));
        Block.Parameter inputSelectorParameter = new Block.Parameter(nameAllocator.newName(), irType(inputRowType));
        // TODO try to use `fieldSelectorBlock()` from RelationalProgramBuilder
        Block inputSelector;
        if (inputRowType.equals(EMPTY_ROW)) {
            inputSelector = getEmptyFieldSelector("^inputSelector", inputRowType, nameAllocator);
        }
        else { // TODO helper method to select _all_ fields
            Block.Builder inputSelectorBuilder = new Block.Builder(Optional.of("^inputSelector"), ImmutableList.of(inputSelectorParameter));
            ImmutableList.Builder<Operation> fieldReferencesBuilder = ImmutableList.builder();
            for (int i = 0; i < inputRowType.getTypeParameters().size(); i++) {
                FieldReference fieldReference = new FieldReference(nameAllocator.newName(), inputSelectorParameter, i, ImmutableMap.of());
                inputSelectorBuilder.addOperation(fieldReference);
                fieldReferencesBuilder.add(fieldReference);
            }
            List<Operation> fieldReferences = fieldReferencesBuilder.build();
            Row rowConstructor = new Row(
                    nameAllocator.newName(),
                    fieldReferences.stream()
                            .map(Operation::result)
                            .collect(toImmutableList()),
                    fieldReferences.stream()
                            .map(Operation::attributes)
                            .collect(toImmutableList()));
            inputSelectorBuilder.addOperation(rowConstructor);
            Return returnOperation = new Return(nameAllocator.newName(), rowConstructor.result(), rowConstructor.attributes());
            inputSelectorBuilder.addOperation(returnOperation);
            inputSelector = inputSelectorBuilder.build();
        }

        return new Exchange(
                exchangeResultName,
                ImmutableList.of(input.result()),
                ImmutableList.of(inputSelector),
                getEmptyFieldSelector("^boundArguments", inputRowType, nameAllocator),
                getEmptyFieldSelector("^hashSelector", inputRowType, nameAllocator),
                getEmptyFieldSelector("^orderingSelector", inputRowType, nameAllocator),
                Attributes.ExchangeType.REPARTITION,
                Attributes.ExchangeScope.REMOTE,
                FIXED_ARBITRARY_DISTRIBUTION,
                new Attributes.NullableValues(new NullableValue[] {}),
                false,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of(input.attributes()));
    }

    // TODO support multiple sources
    private static Block rebasePredicateAndPruneUnsupportedConjuncts(Block block, Operation baseOperation, FieldMapping fieldMapping, ProgramBuilder.ValueNameAllocator nameAllocator)
    {
        // TODO: extract conjuncts, try to rebase each one, retain the ones that succeed, build conjunction -- using PredicateUtils

        Optional<Block> rebased = rebaseBlock(block, relationRowType(trinoType(baseOperation.result().type())), fieldMapping, nameAllocator);
        if (rebased.isEmpty()) {
            // TODO for now returning trivial predicate. We should only remove unsupported conjuncts, not all of them
            Type inputRowType = relationRowType(trinoType(baseOperation.result().type()));
            Block.Parameter newParameter = new Block.Parameter(nameAllocator.newName(), irType(inputRowType));
            return truePredicate(block.name(), ImmutableList.of(newParameter), nameAllocator);
        }
        return rebased.get();
    }

    /**
     * Build a block representing the updated query with diamond shape.
     * <p>
     * The root operation is the Output operation, being the root of the query plan. Starting from this operation, we recursively output
     * the operation's sources, and then output the operation itself. This way we assure the correct layout of the program where each value
     * is declared before it is used.
     * <p>
     * When searching for the source operations, we first check in the newOperations, and then in the oldOperations.
     * We find the operation by its result name. Generally, the oldOperations and the newOperations use different result names.
     * There is one exception to this rule: the operations on the border between the old parts of the program and the rewritten parts of the program
     * are new operations that reuse the old operation's result so that they can be wired to the old program.
     * <p>
     * Because there is diamond shape, the common subqueries will be visited multiple times in this method.
     * We want to layout them once, on the first visit.
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
     * @param predicateToApply -- predicate extracted from this branch as the result of unifying with other branches. Note: it might use fields marked as fieldsToPrune
     * @param enforcedPredicate -- predicate guaranteed for the unified plan. Note: it might not be the full guaranteed predicate.
     * It only contains the conjuncts supported by the output fields of the base operation. It is used when creating new predicates to avoid repetition.
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
     * Traversal state for the branch. Consists of Traversal context and the next operation in the branch.
     * TODO the index in OperationAndIndex tells which source of the next operation the current operation is
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
     * A memoized point in the unified branch. Can be used for backtracking when unifying fails.
     *
     * @param operation -- the unified operation
     * @param traversalStates -- TraversalState for each branch on top of the unified operation
     */
    private record Checkpoint(Operation operation, List<TraversalState> traversalStates) // TODO use UnifiedStates instead
    {
        private Checkpoint
        {
            requireNonNull(operation, "operation is null");
            requireNonNull(traversalStates, "traversalStates is null");
            traversalStates = ImmutableList.copyOf(traversalStates);
        }
    }

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
