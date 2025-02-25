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

/*import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Iterators.peekingIterator;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.ir.IrUtils.combineConjuncts;
import static java.util.Objects.requireNonNull;*/

public class PredicateUtilsWIP
{
    /*private final Metadata metadata;
    private final ProgramBuilder.ValueNameAllocator nameAllocator;
    // TODO ValueMap -- map every Value that we add: block parameter and operation results

    public PredicateUtils(Metadata metadata, ProgramBuilder.ValueNameAllocator nameAllocator)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.nameAllocator = requireNonNull(nameAllocator, "nameAllocator is null");
    }

    *//**
 * Translate a TupleDomain into a Block containing the predicate program.
 * The program is based on the output type of the provided TableScan.
 * If any column referred in the TupleDomain is not available in the TableScan,
 * the rewrite fails, and Optional.empty() is returned.
 * <p>
 * Based on DomainTranslator.
 * // TODO if we build a Block already, we produce for example TRUE conjuncts, which we must remove later.
 * Or for example if there is a FALSE conjunct, we should remove all conjuncts, and fold the conjunction to FALSE.
 * // TODO change the approach: instead of passing the Block.Builder everywhere and adding all operations to the Block already,
 * we should build a collection of operations using:
 * - ValueNameAllocator
 * - Block.Parameter
 * - field name for each column.
 * This can be a Map <Operation name, Operation>, based on List to preserve order.
 * Later, when we combine the operations in a disjunction / conjunction, we are free to drop some of them.
 * They will not become dead code, because they are not in the Block yet.
 * Lastly, we go back from the final operation, and identify all operations that were used.
 * Those go to the Block.
 * In the end, we build a ValueMap for everything in the Block.
 *
 *//*
    public Optional<Block> tupleDomainToBlock(TupleDomain<ColumnHandle> tupleDomain, TableScan tableScan)
    {
        Type relationRowType = relationRowType(trinoType(tableScan.result().type()));
        Block.Parameter parameter = new Block.Parameter(nameAllocator.newName(), irType(relationRowType));
        Block.Builder builder = new Block.Builder(Optional.empty(), ImmutableList.of(parameter));

        if (tupleDomain.isNone()) {
            builder.addOperation(new Constant(nameAllocator.newName(), BOOLEAN, false));
        }
        else {
            ImmutableList.Builder<Operation> conjuncts = ImmutableList.builder();
            for (Map.Entry<ColumnHandle, Domain> entry : tupleDomain.getDomains().orElseThrow().entrySet()) {
                ColumnHandle columnHandle = entry.getKey();
                Domain domain = entry.getValue();
                Optional<String> fieldName = getFieldName(columnHandle, relationRowType, COLUMN_HANDLES.getAttribute(tableScan.attributes()));
                if (fieldName.isEmpty()) {
                    return Optional.empty();
                }
                Operation conjunct = domainToOperations(fieldName.orElseThrow(), domain, parameter, builder);
                conjuncts.add(conjunct);
            }
            // TODO add conjunction of conjuncts (TRUE if conjuncts is empty)
        }

        //TODO add return operation and return Block
    }

    private Optional<String> getFieldName(ColumnHandle columnHandle, Type relationRowType, List<ColumnHandle> availableColumnHandles)
    {
        int index = availableColumnHandles.indexOf(columnHandle);
        if (index == -1) {
            return Optional.empty();
        }
        return Optional.of(((RowType) relationRowType).getFields().get(index).getName().orElseThrow());
    }

    private Operation domainToOperations(String fieldName, Domain domain, Block.Parameter parameter, Block.Builder builder)
    {
        if (domain.getValues().isNone()) {
            if (domain.isNullAllowed()) {
                Operation fieldSelection = new FieldSelection(nameAllocator.newName(), parameter, fieldName, ImmutableMap.of());
                Operation isNull = new IsNull(nameAllocator.newName(), fieldSelection.result(), ImmutableMap.of());
                builder.addOperation(fieldSelection);
                builder.addOperation(isNull);
                return isNull;
            }
            else {
                Operation constantFalse = new Constant(nameAllocator.newName(), BOOLEAN, false);
                builder.addOperation(constantFalse); // TODO fold conjunction if it contains constant false
                return constantFalse;
            }
        }

        if (domain.getValues().isAll()) {
            if (domain.isNullAllowed()) {
                Operation constantTrue = new Constant(nameAllocator.newName(), BOOLEAN, true);
                builder.addOperation(constantTrue); // TODO should be skipped when creating conjunction -> must remove it afterwards as dead code
                return constantTrue;
            }
            else {
                Operation fieldSelection = new FieldSelection(nameAllocator.newName(), parameter, fieldName, ImmutableMap.of());
                Operation isNull = new IsNull(nameAllocator.newName(), fieldSelection.result(), ImmutableMap.of());
                Operation negated = negate(isNull);
                builder.addOperation(fieldSelection);
                builder.addOperation(isNull);
                builder.addOperation(negated);
                return negated;
            }
        }

        ImmutableList.Builder<Operation> disjuncts = ImmutableList.builder();

        // add nullability disjunct
        if (domain.isNullAllowed()) {
            Operation fieldSelection = new FieldSelection(nameAllocator.newName(), parameter, fieldName, ImmutableMap.of()); // TODO pass source attrs everywhere
            Operation isNull = new IsNull(nameAllocator.newName(), fieldSelection.result(), ImmutableMap.of());
            builder.addOperation(fieldSelection);
            builder.addOperation(isNull);
            disjuncts.add(isNull);
        }

        // TODO add remaining disjuncts -- add everything to block builder
        disjuncts.addAll(domain.getValues().getValuesProcessor().transform(
                ranges -> extractDisjuncts(domain.getType(), ranges, parameter, fieldName, builder),
                discreteValues -> extractDisjuncts(domain.getType(), discreteValues, parameter, fieldName, builder),
                allOrNone -> {
                    throw new IllegalStateException("Case should not be reachable");
                }));

        // TODO combine disjuncts, and add disjunction operation to block builder
        // TODO return the disjunction operation
        return combineDisjunctsWithDefault(disjuncts, TRUE);
    }

    private Operation negate(Operation operation)
    {
        ResolvedFunction notFunction = metadata.resolveBuiltinFunction("$not", fromTypes(BOOLEAN));
        return new Call(nameAllocator.newName(), ImmutableList.of(operation.result()), notFunction, ImmutableList.of(operation.attributes()));
    }

    private List<Operation> extractDisjuncts(Type type, Ranges ranges, Block.Parameter parameter, String fieldName, Block.Builder builder)
    {
        List<Operation> disjuncts = new ArrayList<>();
        List<Operation> singleValues = new ArrayList<>();
        List<Range> orderedRanges = ranges.getOrderedRanges();

        SortedRangeSet sortedRangeSet = SortedRangeSet.copyOf(type, orderedRanges);
        SortedRangeSet complement = sortedRangeSet.complement();

        List<Range> singleValueExclusionsList = complement.getOrderedRanges().stream().filter(Range::isSingleValue).collect(toList());
        List<Range> originalUnionSingleValues = SortedRangeSet.copyOf(type, singleValueExclusionsList).union(sortedRangeSet).getOrderedRanges();
        PeekingIterator<Range> singleValueExclusions = peekingIterator(singleValueExclusionsList.iterator());

        *//*
        For types including NaN, it is incorrect to introduce range "all" while processing a set of ranges,
        even if the component ranges cover the entire value set.
        This is because partial ranges don't include NaN, while range "all" does.
        Example: ranges (unbounded , 1.0) and (1.0, unbounded) should not be coalesced to (unbounded, unbounded) with excluded point 1.0.
        That result would be further translated to expression "xxx <> 1.0", which is satisfied by NaN.
        To avoid error, in such case the ranges are not optimised.
         *//*
        if (type instanceof RealType || type instanceof DoubleType) {
            boolean originalRangeIsAll = orderedRanges.stream().anyMatch(Range::isAll);
            boolean coalescedRangeIsAll = originalUnionSingleValues.stream().anyMatch(Range::isAll);
            if (!originalRangeIsAll && coalescedRangeIsAll) {
                for (Range range : orderedRanges) {
                    disjuncts.add(processRange(type, range, parameter, fieldName, builder));
                }
                return disjuncts;
            }
        }

        for (Range range : originalUnionSingleValues) {
            if (range.isSingleValue()) {
                Operation constant = new Constant(nameAllocator.newName(), type, range.getSingleValue());
                builder.addOperation(constant);
                singleValues.add(constant);
                continue;
            }

            // attempt to optimize ranges that can be coalesced as long as single value points are excluded
            List<Operation> singleValuesInRange = new ArrayList<>();
            while (singleValueExclusions.hasNext() && range.contains(singleValueExclusions.peek())) {
                Operation constant = new Constant(nameAllocator.newName(), type, singleValueExclusions.next().getSingleValue());
                builder.addOperation(constant);
                singleValuesInRange.add(constant);
            }
            if (!singleValuesInRange.isEmpty()) {
                Operation excludedPointsOperation;
                Operation fieldSelection = new FieldSelection(nameAllocator.newName(), parameter, fieldName, ImmutableMap.of());
                builder.addOperation(fieldSelection);
                if (singleValuesInRange.size() > 1) {
                    Operation in = new In(
                            nameAllocator.newName(),
                            fieldSelection.result(),
                            singleValuesInRange.stream()
                                    .map(Operation::result)
                                    .collect(toImmutableList()),
                            Streams.concat(
                                            Stream.of(fieldSelection.attributes()),
                                            singleValuesInRange.stream()
                                                    .map(Operation::attributes))
                                    .collect(toImmutableList()));
                    Operation negated = negate(in);
                    builder.addOperation(in);
                    builder.addOperation(negated);
                    excludedPointsOperation = negated;
                }
                else {
                    Operation comparison = new Comparison(
                            nameAllocator.newName(),
                            fieldSelection.result(),
                            getOnlyElement(singleValuesInRange).result(),
                            Attributes.ComparisonOperator.NOT_EQUAL,
                            ImmutableList.of(fieldSelection.attributes(), getOnlyElement(singleValuesInRange).attributes()));
                    builder.addOperation(comparison);
                    excludedPointsOperation = comparison;
                }
                // TODO now combine processRange() with excludedPointsOperation and add this to disjuncts (and to block!)
                Operation rangeResult = processRange(type, range, parameter, fieldName, builder);
                Operation conjunction = combineConjuncts(rangeResult, excludedPointsOperation);
                builder.addOperation(conjunction);
                disjuncts.add(conjunction);
                continue;
            }

            disjuncts.add(processRange(type, range, parameter, fieldName, builder));
        }

        // add back all the possible single values either as an equality or an IN predicate
        if (singleValues.size() == 1) {
            Operation fieldSelection = new FieldSelection(nameAllocator.newName(), parameter, fieldName, ImmutableMap.of()); // TODO reuse FieldSelection instead of creating new each time
            Operation comparison = new Comparison(
                    nameAllocator.newName(),
                    fieldSelection.result(),
                    getOnlyElement(singleValues).result(),
                    Attributes.ComparisonOperator.EQUAL,
                    ImmutableList.of(fieldSelection.attributes(), getOnlyElement(singleValues).attributes()));
            builder.addOperation(fieldSelection);
            builder.addOperation(comparison);
            disjuncts.add(comparison);
        }
        else if (singleValues.size() > 1) {
            Operation fieldSelection = new FieldSelection(nameAllocator.newName(), parameter, fieldName, ImmutableMap.of());
            Operation in = new In(
                    nameAllocator.newName(),
                    fieldSelection.result(),
                    singleValues.stream()
                            .map(Operation::result)
                            .collect(toImmutableList()),
                    Streams.concat(
                                    Stream.of(fieldSelection.attributes()),
                                    singleValues.stream()
                                            .map(Operation::attributes))
                            .collect(toImmutableList()));
            builder.addOperation(fieldSelection);
            builder.addOperation(in);
            disjuncts.add(in);
        }
        return disjuncts;
    }

    private Operation processRange(Type type, Range range, Block.Parameter parameter, String fieldName, Block.Builder builder)
    {
        if (range.isAll()) {
            Operation constantTrue = new Constant(nameAllocator.newName(), BOOLEAN, true);
            builder.addOperation(constantTrue); // TODO should be skipped when creating conjunction -> must remove it afterwards as dead code
            return constantTrue;
        }

        Operation fieldSelection = new FieldSelection(nameAllocator.newName(), parameter, fieldName, ImmutableMap.of());
        builder.addOperation(fieldSelection);

        if (range.isLowInclusive() && range.isHighInclusive()) {
            // specialize the range with BETWEEN expression if possible b/c it is currently more efficient
            Operation min = new Constant(nameAllocator.newName(), type, range.getLowBoundedValue());
            Operation max = new Constant(nameAllocator.newName(), type, range.getHighBoundedValue());
            Operation between = new Between(
                    nameAllocator.newName(),
                    fieldSelection.result(),
                    min.result(),
                    max.result(),
                    ImmutableList.of(fieldSelection.attributes(), min.attributes(), max.attributes()));
            builder.addOperation(min);
            builder.addOperation(max);
            builder.addOperation(between);
            return between;
        }

        Operation lowBoundComparison = null;
        List<Expression> rangeConjuncts = new ArrayList<>();
        if (!range.isLowUnbounded()) {
            Operation constant = new Constant(nameAllocator.newName(), type, range.getLowBoundedValue());
            lowBoundComparison = new Comparison(
                    nameAllocator.newName(),
                    fieldSelection.result(),
                    constant.result(),
                    range.isLowInclusive() ? GREATER_THAN_OR_EQUAL : GREATER_THAN,
                    ImmutableList.of(fieldSelection.attributes(), constant.attributes()));
            builder.addOperation(constant);
            builder.addOperation(lowBoundComparison);
        }
        Operation highBoundComparison = null;
        if (!range.isHighUnbounded()) {
            Operation constant = new Constant(nameAllocator.newName(), type, range.getHighBoundedValue());
            highBoundComparison = new Comparison(
                    nameAllocator.newName(),
                    fieldSelection.result(),
                    constant.result(),
                    range.isHighInclusive() ? LESS_THAN_OR_EQUAL : LESS_THAN,
                    ImmutableList.of(fieldSelection.attributes(), constant.attributes()));
            builder.addOperation(constant);
            builder.addOperation(highBoundComparison);
        }
        // if both comparisons are null, then the range was ALL, which should already have been checked for
        checkState(lowBoundComparison != null || highBoundComparison != null);
        if (lowBoundComparison != null && highBoundComparison != null) {
            Operation conjunction = combineConjuncts(lowBoundComparison, highBoundComparison);
            builder.addOperation(conjunction);
            return conjunction;
        }
        if (lowBoundComparison != null) {
            return lowBoundComparison;
        }
        return highBoundComparison;
    }

    private Operation combineConjuncts(List<Operation> operations)
    {
        List<Expression> conjuncts = expressions.stream()
                .flatMap(e -> extractConjuncts(e).stream())
                .filter(e -> !e.equals(TRUE))
                .collect(toList());

        conjuncts = removeDuplicates(conjuncts);

        if (conjuncts.contains(FALSE)) {
            return FALSE;
        }

        return and(conjuncts);
    }*/
}
