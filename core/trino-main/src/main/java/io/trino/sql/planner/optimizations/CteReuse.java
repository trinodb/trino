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
package io.trino.sql.planner.optimizations;

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
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.EmptyRowType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.sql.PlannerContext;
import io.trino.sql.dialect.trino.Context;
import io.trino.sql.dialect.trino.ProgramBuilder;
import io.trino.sql.dialect.trino.ScalarProgramBuilder;
import io.trino.sql.dialect.trino.operation.FieldReference;
import io.trino.sql.dialect.trino.operation.FieldSelection;
import io.trino.sql.dialect.trino.operation.Filter;
import io.trino.sql.dialect.trino.operation.Project;
import io.trino.sql.dialect.trino.operation.Query;
import io.trino.sql.dialect.trino.operation.Return;
import io.trino.sql.dialect.trino.operation.Row;
import io.trino.sql.dialect.trino.operation.TableScan;
import io.trino.sql.ir.Expression;
import io.trino.sql.newir.Block;
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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Multimaps.toMultimap;
import static io.trino.spi.StandardErrorCode.IR_ERROR;
import static io.trino.spi.type.EmptyRowType.EMPTY_ROW;
import static io.trino.sql.dialect.trino.Attributes.COLUMN_HANDLES;
import static io.trino.sql.dialect.trino.Attributes.FIELD_INDEX;
import static io.trino.sql.dialect.trino.Attributes.FIELD_NAME;
import static io.trino.sql.dialect.trino.Attributes.TABLE_HANDLE;
import static io.trino.sql.dialect.trino.Attributes.UPDATE_TARGET;
import static io.trino.sql.dialect.trino.Attributes.USE_CONNECTOR_NODE_PARTITIONING;
import static io.trino.sql.dialect.trino.RelationalProgramBuilder.relationRowType;
import static io.trino.sql.dialect.trino.TrinoDialect.irType;
import static io.trino.sql.dialect.trino.TrinoDialect.trinoType;
import static io.trino.sql.planner.optimizations.PredicateUtils.conjunction;
import static io.trino.sql.planner.optimizations.PredicateUtils.disjunction;
import static io.trino.sql.planner.optimizations.PredicateUtils.isTrue;
import static io.trino.sql.planner.optimizations.PredicateUtils.removeConjuncts;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

public class CteReuse
{
    public static Optional<Program> reuseCommonSubqueries(Plan plan, PlannerContext plannerContext, Session session)
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
        for (UnifiedGroup unifiedGroup : unifiedGroups) {
            // initialize a list of unified operations
            ImmutableList.Builder<Operation> unifiedOperations = ImmutableList.builder();

            // unified TableScan is the first unified operation
            TableScan unifiedTableScan = getUnifiedTableScan(unifiedGroup, nameAllocator);
            unifiedOperations.add(unifiedTableScan);
            Operation recentUnifiedOperation = unifiedTableScan;

            // checkpoint records the recent safe place to backtrack if unifying fails. initially null.
            Checkpoint checkpoint = null;

            // initialize traversal context for each branch on top of the unified TableScan
            List<TraversalContext> traversalContexts = initializeTraversalContexts(unifiedGroup, unifiedTableScan, plannerContext.getMetadata(), nameAllocator);

            // initialize traversal state for each branch by combining the TraversalContext with the next downstream operation
            ImmutableList.Builder<TraversalState> traversalStates = ImmutableList.builder();
            for (int i = 0; i < unifiedGroup.tableScans().size(); i++) {
                Operation nextOperation = getNextOperation(unifiedGroup.tableScans().get(i), usesMap).orElseThrow();
                traversalStates.add(new TraversalState(traversalContexts.get(i), nextOperation));
            }

            List<TraversalState> debug = traversalStates.build();

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
                        unificationResult.firstCompensation().filter(),
                        unificationResult.secondCompensation().filter()))
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

        // map ColumnHandles to field indexes and field names in the unifiedTableScan
        ImmutableMap.Builder<ColumnHandle, Integer> unifiedIndexesBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<ColumnHandle, String> unifiedNamesBuilder = ImmutableMap.builder();
        List<ColumnHandle> unifiedHandles = COLUMN_HANDLES.getAttribute(unifiedTableScan.attributes());
        Type unifiedRowType = relationRowType(trinoType(unifiedTableScan.result().type()));
        if (unifiedRowType instanceof RowType rowType) {
            for (int i = 0; i < unifiedHandles.size(); i++) {
                // the unified TableScan has no duplicate ColumnHandles, as they were de-duplicated in the `getUnifiedTableScan()` method
                unifiedIndexesBuilder.put(unifiedHandles.get(i), i);
                unifiedNamesBuilder.put(unifiedHandles.get(i), rowType.getFields().get(i).getName().orElseThrow());
            }
        }
        Map<ColumnHandle, Integer> unifiedIndexes = unifiedIndexesBuilder.buildOrThrow();
        Map<ColumnHandle, String> unifiedNames = unifiedNamesBuilder.buildOrThrow();

        // process component tables in reverse order to compose compensations
        TupleDomain<ColumnHandle> currentCompensationPredicate = TupleDomain.all();
        OptionalLong currentCompensationLimit = OptionalLong.empty();
        for (int i = unifiedGroup.tableScans().size() - 1; i >= 1; i--) {
            TableScan tableScan = unifiedGroup.tableScans().get(i);
            UnificationResult<TableHandle> unificationResult = unifiedGroup.unificationResults().get(i - 1);
            TraversalContext.Mapping mapping = computeMapping(tableScan, unifiedIndexes, unifiedNames);
            Set<Integer> fieldsToPrune = computeFieldsToPrune(tableScan, unifiedIndexes);
            Block compensationPredicate = translateToBlock(unifiedTableScan, currentCompensationPredicate.intersect(unificationResult.secondCompensation().filter()), metadata, nameAllocator);
            OptionalLong compensationLimit = combineLimit(currentCompensationLimit, unificationResult.secondCompensation().limit());

            currentCompensationPredicate = currentCompensationPredicate.intersect(unificationResult.firstCompensation().filter());
            currentCompensationLimit = combineLimit(currentCompensationLimit, unificationResult.firstCompensation().limit());

            resultBuilder.add(new TraversalContext(mapping, fieldsToPrune, compensationPredicate, compensationLimit, enforcedPredicate, enforcedLimit));
        }

        // process the first component table
        TableScan tableScan = unifiedGroup.tableScans().get(0);
        TraversalContext.Mapping mapping = computeMapping(tableScan, unifiedIndexes, unifiedNames);
        Set<Integer> fieldsToPrune = computeFieldsToPrune(tableScan, unifiedIndexes);
        Block compensationPredicate = translateToBlock(unifiedTableScan, currentCompensationPredicate, metadata, nameAllocator);
        OptionalLong compensationLimit = currentCompensationLimit;
        resultBuilder.add(new TraversalContext(mapping, fieldsToPrune, compensationPredicate, compensationLimit, enforcedPredicate, enforcedLimit));

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
        if (relationRowType instanceof RowType rowType) {
            for (int i = 0; i < symbolList.size(); i++) {
                symbolMapping.put(symbolList.get(i), new Context.RowField(parameter, rowType.getFields().get(i).getName().orElseThrow()));
            }
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
    private static TraversalContext.Mapping computeMapping(TableScan componentTableScan, Map<ColumnHandle, Integer> unifiedIndexes, Map<ColumnHandle, String> unifiedNames)
    {
        List<ColumnHandle> componentHandles = COLUMN_HANDLES.getAttribute(componentTableScan.attributes());
        if (componentHandles.isEmpty()) {
            return new TraversalContext.Mapping(ImmutableMap.of(), ImmutableMap.of());
        }

        RowType componentRowType = (RowType) relationRowType(trinoType(componentTableScan.result().type()));
        ImmutableMap.Builder<Integer, Integer> indexMapping = ImmutableMap.builder();
        ImmutableMap.Builder<String, String> nameMapping = ImmutableMap.builder();
        for (int i = 0; i < componentHandles.size(); i++) {
            indexMapping.put(i, unifiedIndexes.get(componentHandles.get(i)));
            nameMapping.put(componentRowType.getFields().get(i).getName().orElseThrow(), unifiedNames.get(componentHandles.get(i)));
        }

        return new TraversalContext.Mapping(indexMapping.buildOrThrow(), nameMapping.buildOrThrow());
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

    private static OptionalLong combineLimit(OptionalLong firstLimit, OptionalLong secondLimit)
    {
        return LongStream.concat(firstLimit.stream(), secondLimit.stream()).min();
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
     */
    private static Optional<Operation> getNextOperation(Operation operation, Multimap<Operation, Operation> usesMap)
    {
        Collection<Operation> uses = usesMap.get(operation);
        if (uses.size() != 1) {
            return Optional.empty();
        }
        return Optional.of(getOnlyElement(uses));
    }

    // TODO javadoc when method finished
    private static void mergeGroup(List<TraversalState> branches, Operation unifiedOperation, Map<Value, SourceNode> valueMap, Multimap<Operation, Operation> usesMap, ProgramBuilder.ValueNameAllocator nameAllocator)
    {
        // for each branch, ingest operations into context as long as possible
        List<TraversalState> ingestedStates = branches.stream()
                .map(branch -> ingestOperationsIntoContext(branch, unifiedOperation, valueMap, usesMap, nameAllocator))
                .collect(toImmutableList());

        // each branch is fully ingested. extract the common part of all branches, and add operations for it
        UnifiedStates unifiedStates = unifyAndOutput(ingestedStates, unifiedOperation, valueMap, nameAllocator);
    }

    private static TraversalState ingestOperationsIntoContext(TraversalState branchState, Operation unifiedOperation, Map<Value, SourceNode> valueMap, Multimap<Operation, Operation> usesMap, ProgramBuilder.ValueNameAllocator nameAllocator)
    {
        Operation nextOperation = branchState.nextOperation();
        if (nextOperation instanceof Project project && project.isPruning(valueMap)) {
            Project rebased = rebase(project, unifiedOperation, branchState.traversalContext().mapping(), nameAllocator);
            TraversalContext newContext = new TraversalContext(
                    computeMapping(project, rebased, valueMap),
                    getPrunedFields(rebased, valueMap),
                    branchState.traversalContext().predicateToApply(),
                    branchState.traversalContext().limitToApply(),
                    branchState.traversalContext().enforcedPredicate(),
                    branchState.traversalContext().enforcedLimit());
            return ingestOperationsIntoContext(new TraversalState(newContext, getNextOperation(project, usesMap).orElseThrow()), unifiedOperation, valueMap, usesMap, nameAllocator);
        }
        /*else if (nextOperation instanceof Filter) { // TODO check if deterministic

        }
        else if (nextOperation instanceof Limit) {

        }*/
        return branchState;
    }

    /**
     * Rebase projection onto unifiedOperation. The provided Project operation must be pruning.
     * This method assumes that there is no dead code in project.assignments(). That is, all operations contained in this block
     * are: FieldSelections, Row constructor using all the FieldSelections, and the Return operation.
     */
    private static Project rebase(Project project, Operation unifiedOperation, TraversalContext.Mapping mapping, ProgramBuilder.ValueNameAllocator nameAllocator)
    {
        Block oldAssignments = project.assignments();
        Block.Parameter newAssignmentsParameter = new Block.Parameter(nameAllocator.newName(), irType(relationRowType(trinoType(unifiedOperation.result().type()))));
        Block.Builder newAssignments = new Block.Builder(Optional.of("^assignments"), ImmutableList.of(newAssignmentsParameter));
        for (Operation operation : oldAssignments.operations()) {
            if (operation instanceof FieldSelection fieldSelection) {
                newAssignments.addOperation(new FieldSelection(
                        fieldSelection.result().name(), // TODO remember to update value map when we incorporate the new operations which use the old names
                        newAssignmentsParameter,
                        mapping.fieldNameMapping().get(FIELD_NAME.getAttribute(fieldSelection.attributes())),
                        ImmutableMap.of()));
            }
            else {
                newAssignments.addOperation(operation);
            }
        }

        return new Project(
                project.result().name(),
                unifiedOperation.result(),
                newAssignments.build(),
                unifiedOperation.attributes());
    }

    /**
     * Compute mapping to rewrite references to the output fields of oldProject in terms of the output fields of the unified operation, using newProject.
     * oldProject and newProject must be pruning. newProject is a result of rebasing oldProject onto the unified operation.
     * newProject will not be incorporated in the unified plan. Instead, it will be ingested into the traversal context.
     * The subsequent downstream operation will be rebased onto the unified operation using the mapping.
     */
    private static TraversalContext.Mapping computeMapping(Project oldProject, Project newProject, Map<Value, SourceNode> valueMap)
    {
        Type oldOutputRowType = relationRowType(trinoType(oldProject.result().type()));

        if (oldOutputRowType.equals(EMPTY_ROW)) {
            return new TraversalContext.Mapping(ImmutableMap.of(), ImmutableMap.of());
        }

        List<String> oldOutputFields = ((RowType) oldOutputRowType).getFields().stream()
                .map(RowType.Field::getName)
                .map(Optional::orElseThrow)
                .collect(toImmutableList());

        Block newAssignments = newProject.assignments();
        List<String> inputFields = ((RowType) trinoType(getOnlyElement(newAssignments.parameters()).type())).getFields().stream()
                .map(RowType.Field::getName)
                .map(Optional::orElseThrow)
                .collect(toImmutableList());
        Row newRowConstructor = (Row) valueMap.get(((Return) newAssignments.getTerminalOperation()).argument());

        ImmutableMap.Builder<Integer, Integer> indexMapping = ImmutableMap.builder();
        ImmutableMap.Builder<String, String> nameMapping = ImmutableMap.builder();

        for (int i = 0; i < oldOutputFields.size(); i++) {
            String oldName = oldOutputFields.get(i);
            String newName = FIELD_NAME.getAttribute(((FieldSelection) valueMap.get(newRowConstructor.arguments().get(i))).attributes());
            int newIndex = inputFields.indexOf(newName);
            indexMapping.put(i, newIndex);
            nameMapping.put(oldName, newName);
        }

        return new TraversalContext.Mapping(indexMapping.buildOrThrow(), nameMapping.buildOrThrow());
    }

    /**
     * Compute indexes of pruned input fields. project must be pruning.
     */
    private static Set<Integer> getPrunedFields(Project project, Map<Value, SourceNode> valueMap)
    {
        Type inputRowType = relationRowType(trinoType(getOnlyElement(project.arguments()).type()));
        if (inputRowType.equals(EMPTY_ROW)) {
            // nothing to prune
            return ImmutableSet.of();
        }
        Block assignments = getOnlyElement(project.regions()).getOnlyBlock();
        Row rowConstructor = (Row) valueMap.get(((Return) assignments.getTerminalOperation()).argument());
        Set<String> referencedFields = rowConstructor.arguments().stream()
                .map(valueMap::get)
                .map(FieldSelection.class::cast)
                .map(FieldSelection::attributes)
                .map(FIELD_NAME::getAttribute)
                .collect(toImmutableSet());
        ImmutableSet.Builder<Integer> prunedFields = ImmutableSet.builder();
        RowType rowType = (RowType) inputRowType;
        for (int i = 0; i < rowType.getFields().size(); i++) {
            if (!referencedFields.contains(rowType.getFields().get(i).getName().orElseThrow())) {
                prunedFields.add(i);
            }
        }
        return prunedFields.build();
    }

    /**
     * Compute the common part of the TraversalContexts, and the residual part of each context. Create operations for the unified part.
     * This method assumes that all provided states are based on baseUnifiedOperation, so they can be safely combined.
     * There must be at least two states provided.
     */
    private static UnifiedStates unifyAndOutput(List<TraversalState> states, Operation baseUnifiedOperation, Map<Value, SourceNode> valueMap, ProgramBuilder.ValueNameAllocator nameAllocator)
    {
        checkArgument(states.size() > 1, "at least two branches must be provided for unification");

        List<TraversalContext> contexts = states.stream()
                .map(TraversalState::traversalContext)
                .collect(toImmutableList());

        Block enforcedPredicate = getOnlyElement(contexts.stream()
                .map(TraversalContext::enforcedPredicate)
                .distinct()
                .collect(toImmutableList()));

        OptionalLong enforcedLimit = getOnlyElement(contexts.stream()
                .map(TraversalContext::enforcedLimit)
                .distinct()
                .collect(toImmutableList()));

        Block unifiedPredicateToApply = disjunction(
                contexts.stream()
                        .map(TraversalContext::predicateToApply)
                        .collect(toImmutableList()),
                nameAllocator);
        // remove the conjuncts that are already enforced
        unifiedPredicateToApply = removeConjuncts(unifiedPredicateToApply, enforcedPredicate);

        ImmutableList.Builder<Block> residualPredicatesBuilder = ImmutableList.builder();
        for (Block predicateToApply : contexts.stream()
                .map(TraversalContext::predicateToApply)
                .collect(toImmutableList())) {
            predicateToApply = removeConjuncts(predicateToApply, enforcedPredicate);
            predicateToApply = removeConjuncts(predicateToApply, unifiedPredicateToApply);
            residualPredicatesBuilder.add(predicateToApply);
        }
        List<Block> residualPredicates = residualPredicatesBuilder.build();

        OptionalLong unifiedLimitToApply;
        List<OptionalLong> limitsToApply = contexts.stream()
                .map(TraversalContext::limitToApply)
                .collect(toImmutableList());
        if (limitsToApply.stream()
                .anyMatch(OptionalLong::isEmpty)) {
            unifiedLimitToApply = OptionalLong.empty();
        }
        else {
            long unifiedLimit = limitsToApply.stream()
                    .map(OptionalLong::orElseThrow)
                    .max(Long::compare)
                    .orElseThrow();
            if (enforcedLimit.isPresent() && enforcedLimit.getAsLong() <= unifiedLimit) {
                unifiedLimitToApply = OptionalLong.empty();
            }
            else {
                unifiedLimitToApply = OptionalLong.of(unifiedLimit);
            }
        }

        Set<Integer> unifiedFieldsToPrune = contexts.stream()
                .map(TraversalContext::fieldsToPrune)
                .reduce((first, second) -> ImmutableSet.copyOf(Sets.intersection(first, second)))
                .orElseThrow();
        // do not prune fields if they are used by residual predicates
        // field indexes from different residual predicates are compatible because all predicates are based on baseUnifiedOperation
        Set<Integer> residualPredicateFields = residualPredicates.stream()
                .map(CteReuse::extractReferencedFields)
                .flatMap(Set::stream)
                .collect(toImmutableSet());
        unifiedFieldsToPrune = Sets.difference(unifiedFieldsToPrune, residualPredicateFields);

        // build unified operations
        Operation unifiedOperation = baseUnifiedOperation;
        ImmutableList.Builder<TraversalContext.Mapping> unifiedMappings = ImmutableList.builder();

        // add unified Filter operation
        if (!isTrue(unifiedPredicateToApply, valueMap)) {
            unifiedOperation = new Filter(
                    nameAllocator.newName(),
                    unifiedOperation.result(),
                    unifiedPredicateToApply,
                    unifiedOperation.attributes());
            unifiedMappings.add(computePassthroughMapping(
                    relationRowType(trinoType(((Filter) unifiedOperation).argument().type())),
                    relationRowType(trinoType(unifiedOperation.result().type()))));
        }

        // add unified Limit operation
        if (unifiedLimitToApply.isPresent()) {
            // TODO Must implement Limit operation. Add Limit operation and mapping
            unifiedMappings.add(computePassthroughMapping(
                    relationRowType(trinoType(((Limit) unifiedOperation).argument().type())),
                    relationRowType(trinoType(unifiedOperation.result().type()))));
        }

        // add unified Project operation
        if (!unifiedFieldsToPrune.isEmpty()) {
            unifiedOperation = new Project(
                    nameAllocator.newName(),
                    unifiedOperation.result(),
                    pruningAssignments(unifiedOperation, unifiedFieldsToPrune),
                    unifiedOperation.attributes());
            unifiedMappings.add(computeMappingForPruning((Project) unifiedOperation));
        }

        Optional<TraversalContext.Mapping> unifiedMapping = composeMappings(unifiedMappings.build());

        // compute the new enforced predicate and rebase it onto the last unified operation.
        // Note: some conjuncts might no longer be supported after pruning and must be removed.
        Block newEnforcedPredicate = conjunction(ImmutableList.of(enforcedPredicate, unifiedPredicateToApply), nameAllocator);
        newEnforcedPredicate = remapPredicateAndPruneUnsupportedConjuncts(newEnforcedPredicate, unifiedMapping);

        OptionalLong newEnforcedLimit = combineLimit(enforcedLimit, unifiedLimitToApply);

        // compute residual contexts and rebase them on the unified operation.
        ImmutableList.Builder<TraversalState> newStates = ImmutableList.builder();
        for (int i = 0; i < states.size(); i++) {
            TraversalState oldState = states.get(i);
            TraversalContext oldContext = oldState.traversalContext();

            // rebase the old mapping onto the last unified operation
            TraversalContext.Mapping newMapping = composeMappings(ImmutableList.builder() // TODO return Optional.empty() when list empty
                    .add(oldContext.mapping())
                    .addAll(unifiedMappings.build())
                    .build())
                    .orElseThrow();

            // find the remaining fields to prune and rebase them onto the last unified operation
            Set<Integer> newFieldsToPrune = remapIndexes(Sets.difference(oldContext.fieldsToPrune, unifiedFieldsToPrune), unifiedMapping);

            // rebase the residual predicate onto the last unified operation. Note: residual predicates sre fully supported. All fields used by them were retained.
            Block newPredicateToApply = remapBlock(residualPredicates.get(i), unifiedMapping);

            // compute the residual limit
            OptionalLong newLimitToApply;
            if (unifiedLimitToApply.isPresent() && oldContext.limitToApply().isPresent() && unifiedLimitToApply.getAsLong() <= oldContext.limitToApply().getAsLong()) {
                newLimitToApply = OptionalLong.empty();
            }
            else {
                newLimitToApply = oldContext.limitToApply();
            }

            TraversalContext newContext = new TraversalContext(newMapping, newFieldsToPrune, newPredicateToApply, newLimitToApply, newEnforcedPredicate, newEnforcedLimit);
            newStates.add(new TraversalState(newContext, oldState.nextOperation()));
        }

        return new UnifiedStates(unifiedOperation, newStates.build());
    }

    /**
     * Return indexes of referenced input fields. Outer correlated references are ignored.
     * block must have one parameter
     */
    private static Set<Integer> extractReferencedFields(Block block)
    {
        checkArgument(block.parameters().size() == 1, "expected one block parameter");
        Block.Parameter parameter = getOnlyElement(block.parameters());
        if (trinoType(parameter.type()) instanceof EmptyRowType) {
            return ImmutableSet.of();
        }
        List<String> fieldNames = ((RowType) trinoType(parameter.type())).getFields().stream()
                .map(RowType.Field::getName)
                .map(Optional::orElseThrow)
                .collect(toImmutableList());

        ImmutableSet.Builder<Integer> builder = ImmutableSet.builder();
        for (Operation operation : block.operations()) {
            extractReferencedFields(operation, fieldNames, parameter, builder);
        }
        return builder.build();
    }

    private static void extractReferencedFields(Operation operation, List<String> fieldNames, Block.Parameter parameter, ImmutableSet.Builder<Integer> builder)
    {
        if (operation instanceof FieldSelection fieldSelection) {
            // TODO ignores outer correlated references
            if (fieldSelection.base().equals(parameter)) {
                builder.add(fieldNames.indexOf(FIELD_NAME.getAttribute(fieldSelection.attributes())));
            }

        }
        if (operation instanceof FieldReference fieldReference) {
            // TODO ignores outer correlated references
            if (fieldReference.base().equals(parameter)) {
                builder.add(FIELD_INDEX.getAttribute(fieldReference.attributes()));
            }
        }
        for (Region region : operation.regions()) {
            for (Block block : region.blocks()) {
                for (Operation nestedOperation : block.operations()) {
                    extractReferencedFields(nestedOperation, fieldNames, parameter, builder);
                }
            }
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
     * @param mapping -- the output type of the recent operation might have changed. The mapping serves to update the next operation accordingly
     * @param fieldsToPrune -- additional fields output by the recent operation as the result of unifying with other branches
     * @param predicateToApply -- predicate extracted from this branch as the result of unifying with other branches. Note: it might use fields marked as fieldsToPrune
     * @param limitToApply -- limit extracted from this branch as the result of unifying with other branches
     * @param enforcedPredicate -- predicate guaranteed for the unified plan. Note: it might not be the full guaranteed predicate.
     * It only contains the conjuncts supported by the output fields of the base operation. It is used when creating new predicates to avoid repetition.
     * @param enforcedLimit -- limit guaranteed for the unified plan
     */
    private record TraversalContext(Mapping mapping, Set<Integer> fieldsToPrune, Block predicateToApply, OptionalLong limitToApply, Block enforcedPredicate,
                                    OptionalLong enforcedLimit)
    {
        private TraversalContext
        {
            requireNonNull(mapping, "mapping is null");
            requireNonNull(fieldsToPrune, "fieldsToPrune is null");
            requireNonNull(predicateToApply, "predicateToApply is null");
            requireNonNull(limitToApply, "limitToApply is null");
            requireNonNull(enforcedPredicate, "enforcedPredicate is null");
            requireNonNull(enforcedLimit, "enforcedLimit is null");
            fieldsToPrune = ImmutableSet.copyOf(fieldsToPrune);
        }

        private record Mapping(Map<Integer, Integer> fieldIndexMapping, Map<String, String> fieldNameMapping)
        {
            private Mapping
            {
                requireNonNull(fieldIndexMapping, "fieldIndexMapping is null");
                requireNonNull(fieldNameMapping, "fieldNameMapping is null");
                fieldIndexMapping = ImmutableMap.copyOf(fieldIndexMapping);
                fieldNameMapping = ImmutableMap.copyOf(fieldNameMapping);
            }
        }
    }

    /**
     * Traversal state for the branch. Consists of Traversal context and the next operation in the branch.
     */
    private record TraversalState(TraversalContext traversalContext, Operation nextOperation)
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
    private record Checkpoint(Operation operation, List<TraversalState> traversalStates)
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
