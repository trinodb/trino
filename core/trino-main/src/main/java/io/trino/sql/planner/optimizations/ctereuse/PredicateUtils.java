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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.spi.predicate.NullableValue;
import io.trino.sql.dialect.trino.Attributes;
import io.trino.sql.dialect.trino.ProgramBuilder;
import io.trino.sql.dialect.trino.operation.Constant;
import io.trino.sql.dialect.trino.operation.Logical;
import io.trino.sql.dialect.trino.operation.Return;
import io.trino.sql.dialect.trino.operation.TrinoOperation;
import io.trino.sql.newir.Block;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Type;
import io.trino.sql.newir.Value;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.dialect.trino.Attributes.CONSTANT_RESULT;
import static io.trino.sql.dialect.trino.Attributes.LOGICAL_OPERATOR;
import static io.trino.sql.dialect.trino.Attributes.LogicalOperator.AND;
import static io.trino.sql.dialect.trino.Attributes.LogicalOperator.OR;
import static io.trino.sql.dialect.trino.TrinoDialect.trinoType;
import static io.trino.sql.planner.optimizations.ctereuse.RewriteUtils.remapParameters;
import static io.trino.sql.planner.optimizations.ctereuse.RewriteUtils.remapValues;
import static java.util.HashMap.newHashMap;

public class PredicateUtils
{
    private PredicateUtils()
    {}

    public static Block truePredicate(Optional<String> blockName, List<Block.Parameter> parameters, ProgramBuilder.ValueNameAllocator nameAllocator)
    {
        return constantBooleanPredicate(blockName, parameters, Boolean.TRUE, nameAllocator);
    }

    public static Block falsePredicate(Optional<String> blockName, List<Block.Parameter> parameters, ProgramBuilder.ValueNameAllocator nameAllocator)
    {
        return constantBooleanPredicate(blockName, parameters, Boolean.FALSE, nameAllocator);
    }

    public static Block booleanNullPredicate(Optional<String> blockName, List<Block.Parameter> parameters, ProgramBuilder.ValueNameAllocator nameAllocator)
    {
        return constantBooleanPredicate(blockName, parameters, null, nameAllocator);
    }

    private static Block constantBooleanPredicate(Optional<String> blockName, List<Block.Parameter> parameters, Boolean constantValue, ProgramBuilder.ValueNameAllocator nameAllocator)
    {
        Block.Builder builder = new Block.Builder(blockName, parameters);
        Constant constantBooleanOperation = new Constant(nameAllocator.newName(), BOOLEAN, constantValue);
        builder.addOperation(constantBooleanOperation);
        Return returnOperation = new Return(nameAllocator.newName(), constantBooleanOperation.result(), constantBooleanOperation.attributes());
        builder.addOperation(returnOperation);
        return builder.build();
    }

    public static boolean isTrue(Block block)
    {
        checkArgument(trinoType(block.getReturnedType()).equals(BOOLEAN), "expected block returning boolean");

        return block.operations().size() == 2 &&
                block.operations().getLast() instanceof Return returnOperation &&
                // TODO this could be another operation or block parameter with constant true value
                block.operations().getFirst() instanceof Constant constantOperation &&
                returnOperation.argument().equals(constantOperation.result()) &&
                CONSTANT_RESULT.getAttribute(constantOperation.attributes()).equals(NullableValue.of(BOOLEAN, true));
    }

    /**
     * Create a block with a conjunction (AND) of predicates from all component blocks. At least one component block must be provided.
     * The resulting block has the same parameters as the first component block.
     * <p>
     * Note: any correlated referenced in the component blocks will be preserved. The resulting block might contain values
     * from different contexts. Potentially, it might contain identical values which come from different contexts and
     * originally represented different semantics. It is up to the caller to avoid this kind of issues. It is recommended
     * to only call this method for uncorrelated blocks or for blocks belonging to the same context.
     */
    public static Block conjunction(List<Block> blocks, ProgramBuilder.ValueNameAllocator nameAllocator)
    {
        return logical(blocks, AND, nameAllocator);
    }

    /**
     * Create a block with a disjunction (OR) of predicates from all component blocks. At least one component block must be provided.
     * The resulting block has the same parameters as the first component block.
     * <p>
     * Note: any correlated referenced in the component blocks will be preserved. The resulting block might contain values
     * from different contexts. Potentially, it might contain identical values which come from different contexts and
     * originally represented different semantics. It is up to the caller to avoid this kind of issues. It is recommended
     * to only call this method for uncorrelated blocks or for blocks belonging to the same context.
     */
    public static Block disjunction(List<Block> blocks, ProgramBuilder.ValueNameAllocator nameAllocator)
    {
        return logical(blocks, OR, nameAllocator);
    }

    private static Block logical(List<Block> blocks, Attributes.LogicalOperator operator, ProgramBuilder.ValueNameAllocator nameAllocator)
    {
        // to create a block, we need a block parameter representing the input. We cannot create a block when the input type is not known
        checkArgument(!blocks.isEmpty(), "cannot combine 0 blocks");

        checkArgument(
                blocks.stream()
                        .allMatch(block -> trinoType(block.getReturnedType()).equals(BOOLEAN)),
                "expected blocks returning boolean");

        if (blocks.size() == 1) {
            return blocks.getFirst();
        }

        // validate that blocks are compatible, i.e. based on the same input types
        List<Type> parameterTypes = blocks.getFirst().parameters().stream()
                .map(Block.Parameter::type)
                .collect(toImmutableList());
        for (int i = 1; i < blocks.size(); i++) {
            checkArgument(
                    parameterTypes.equals(blocks.get(i).parameters().stream()
                            .map(Block.Parameter::type)
                            .collect(toImmutableList())),
                    "incompatible blocks: parameters mismatch");
        }

        List<Block.Parameter> resultParameters = blocks.getFirst().parameters();
        Block.Builder result = new Block.Builder(Optional.empty(), resultParameters);
        ImmutableList.Builder<Value> terms = ImmutableList.builder();
        for (Block block : blocks) {
            // remap operations in the block to use the first block's parameters
            Map<Value, Value> parameterMap = new HashMap<>();
            for (int i = 0; i < resultParameters.size(); i++) {
                parameterMap.put(block.parameters().get(i), resultParameters.get(i));
            }
            for (Operation operation : block.operations()) {
                if (!(operation instanceof Return returnOperation)) {
                    result.addOperation(remapValues(operation, parameterMap));
                }
                else {
                    terms.add(returnOperation.argument());
                }
            }
        }
        Logical logical = new Logical(nameAllocator.newName(), terms.build(), operator, ImmutableList.of()); // TODO pass source attributes when we remove ValueMap
        result.addOperation(logical);
        Return returnOperation = new Return(nameAllocator.newName(), logical.result(), logical.attributes());
        result.addOperation(returnOperation);

        return result.build();
    }

    public static List<Block> extractConjuncts(Block block, ProgramBuilder.ValueNameAllocator nameAllocator)
    {
        return extractLogicalTerms(block, AND, nameAllocator);
    }

    public static List<Block> extractDisjuncts(Block block, ProgramBuilder.ValueNameAllocator nameAllocator)
    {
        return extractLogicalTerms(block, OR, nameAllocator);
    }

    /**
     * Decompose boolean a block into logical terms. If the returned operation is Logical, create one block for each term.
     * Otherwise, return the original block as the only term.
     * All returned blocks have the same name and parameters as the original block.
     * <p>
     * Note: this method does not flatten nested Logical operations. For flattened terms, first use the optimizeLogicalOperations() method.
     */
    public static List<Block> extractLogicalTerms(Block block, Attributes.LogicalOperator operator, ProgramBuilder.ValueNameAllocator nameAllocator)
    {
        checkArgument(trinoType(block.getReturnedType()).equals(BOOLEAN), "expected block returning boolean");
        checkArgument(block.getTerminalOperation() instanceof Return, "expected block with terminal return operation");

        Optional<Operation> sourceOperation = getSource(((Return) block.getTerminalOperation()).argument(), block);
        if (sourceOperation.isPresent() && sourceOperation.get() instanceof Logical logical && LOGICAL_OPERATOR.getAttribute(logical.attributes()).equals(operator)) {
            Map<Value, Operation> operations = newHashMap(block.operations().size() + logical.arguments().size());
            block.operations().stream()
                    .forEach(operation -> operations.put(operation.result(), operation));
            ImmutableList.Builder<Block> terms = ImmutableList.builder();
            for (Value term : logical.arguments()) {
                Return returnOperation = new Return(nameAllocator.newName(), term, ImmutableMap.of()); // TODO pass source attributes
                operations.put(returnOperation.result(), returnOperation);
                Block.Builder builder = new Block.Builder(block.name(), block.parameters());
                layoutOperations(returnOperation.result(), builder, operations);
                terms.add(builder.build());
            }
            return terms.build();
        }

        return ImmutableList.of(block);
    }

    // TODO remove this method and use proper scoped value resolution when we remove ValueMap
    private static Optional<Operation> getSource(Value value, Block block)
    {
        for (Operation operation : block.operations()) {
            if (operation.result().equals(value)) {
                return Optional.of(operation);
            }
        }
        return Optional.empty();
    }

    /**
     * Remove given conjuncts from the block. The passed blocks must be independent wrt nesting.
     * <p>
     * Note: the removed conjuncts are remapped to use the same parameters as the from block.
     * This is not correct if one of the blocks is nested in the other. This method should only
     * be called for independent blocks.
     * <p>
     * Note: this method does not flatten the predicates. For flattened predicates, first use the optimizeLogicalOperations() method.
     */
    public static Block removeConjuncts(Block from, Block toRemove, ProgramBuilder.ValueNameAllocator nameAllocator)
    {
        checkArgument(trinoType(from.getReturnedType()).equals(BOOLEAN), "expected block returning boolean");
        checkArgument(trinoType(toRemove.getReturnedType()).equals(BOOLEAN), "expected block returning boolean");

        checkArgument(
                from.parameters().stream()
                        .map(Block.Parameter::type)
                        .collect(toImmutableList())
                        .equals(toRemove.parameters().stream()
                                .map(Block.Parameter::type)
                                .collect(toImmutableList())),
                "incompatible blocks: parameters mismatch");

        // remap block toRemove so that it uses the same parameters as from
        Block toRemoveRemapped = remapParameters(toRemove, from.parameters());

        List<Block> fromConjuncts = extractConjuncts(from, nameAllocator);
        Set<Block> conjunctsToRemove = ImmutableSet.copyOf(extractConjuncts(toRemoveRemapped, nameAllocator));

        List<Block> remainingConjuncts = fromConjuncts.stream()
                .filter(conjunct -> !conjunctsToRemove.contains(conjunct))
                .collect(toImmutableList());

        if (remainingConjuncts.isEmpty()) {
            return truePredicate(from.name(), from.parameters(), nameAllocator);
        }

        return conjunction(remainingConjuncts, nameAllocator);
    }

    public static Block optimizeLogicalOperations(Block block, ProgramBuilder.ValueNameAllocator nameAllocator)
    {
        Map<Value, Operation> originalOperations = block.operations().stream()
                .collect(toImmutableMap(Operation::result, Function.identity()));
        Map<Value, Operation> processedOperations = new HashMap<>();
        Operation terminalOperation = block.getTerminalOperation();

        Value optimizedTerminalResult = optimizeLogicalOperations(terminalOperation, originalOperations, processedOperations, nameAllocator);

        Block.Builder optimizedBlock = new Block.Builder(block.name(), block.parameters());
        layoutOperations(optimizedTerminalResult, optimizedBlock, processedOperations);

        return optimizedBlock.build();
    }

    /**
     * Optimizes logical operations (AND, OR) recursively. The optimization consists of:
     * - flattening
     * - deduplicating terms
     * - removing trivial terms
     * - constant-folding
     *
     * @param operation -- the root operation
     * @param originalOperations -- the map value -> operation to resolve source operations
     * @param processedOperations -- the map value -> operation where all processed operations are recorded
     * The processed operations might be either rewritten or unchanged. In both cases, the original result values
     * might be reused. Therefore, all the processed operations must be recorded so that we can build the resulting
     * optimized block consisting of the processed operations, and not the of original ones
     * @param nameAllocator -- ValueNameAllocator needed to create new values during optimization
     * @return -- the result of the optimized root operation. It might be the same as the original result, or different
     */
    private static Value optimizeLogicalOperations(
            Operation operation,
            Map<Value, Operation> originalOperations,
            Map<Value, Operation> processedOperations,
            ProgramBuilder.ValueNameAllocator nameAllocator)
    {
        List<Value> processedArguments = operation.arguments().stream()
                .map(argument -> {
                    Operation source = originalOperations.get(argument);
                    // source == null indicates that the argument is a block parameter or correlated
                    if (source == null) {
                        return argument;
                    }
                    return optimizeLogicalOperations(source, originalOperations, processedOperations, nameAllocator);
                })
                .collect(toImmutableList());

        if (operation instanceof Logical) {
            Attributes.LogicalOperator operator = LOGICAL_OPERATOR.getAttribute(operation.attributes());

            // flatten logical operations
            List<Value> flattenedTerms = processedArguments.stream()
                    .map(argument -> {
                        // find the source operation of the argument in processedOperations
                        // source == null indicates that the argument is a block parameter or correlated, and it wasn't processed
                        Operation source = processedOperations.get(argument);
                        if (source == null) {
                            return ImmutableList.of(argument);
                        }
                        if (source instanceof Logical && operator.equals(LOGICAL_OPERATOR.getAttribute(source.attributes()))) {
                            return source.arguments();
                        }
                        return ImmutableList.of(argument);
                    })
                    .flatMap(List::stream)
                    .collect(toImmutableList());

            // optimize logical operations: deduplicate terms, remove trivial terms, constant-fold
            List<Value> optimizedTerms = optimizeTerms(flattenedTerms, operator, processedOperations);

            if (optimizedTerms.isEmpty()) {
                // return constant TRUE or FALSE, depending on the logical operator. Reuse the original operation result to avoid downstream rewrite
                Constant constantBoolean = switch (operator) {
                    case AND -> new Constant(operation.result().name(), BOOLEAN, true);
                    case OR -> new Constant(operation.result().name(), BOOLEAN, false);
                };
                processedOperations.put(constantBoolean.result(), constantBoolean);
                return constantBoolean.result();
            }

            if (optimizedTerms.size() == 1) {
                // return the only term. if it is a processed operation result, it is already recorded in processedOperations
                return getOnlyElement(optimizedTerms);
            }

            // two or more terms remained. Create a new logical operation. Reuse the original operation result to avoid downstream rewrite
            Operation newLogical = new Logical(operation.result().name(), optimizedTerms, operator, ImmutableList.of()); // TODO pass source attributes
            processedOperations.put(newLogical.result(), newLogical);
            return newLogical.result();
        }

        else {
            // not a logical operation. Swap the arguments that changed
            for (int i = 0; i < operation.arguments().size(); i++) {
                if (!operation.arguments().get(i).equals(processedArguments.get(i))) {
                    operation = ((TrinoOperation) operation).withArgument(processedArguments.get(i), i);
                }
            }
            processedOperations.put(operation.result(), operation);
            return operation.result();
        }
    }

    private static List<Value> optimizeTerms(List<Value> terms, Attributes.LogicalOperator operator, Map<Value, Operation> operations)
    {
        // if a FALSE conjunct is found for AND, or a TRUE disjunct is found for OR, return it
        Optional<Operation> constantBoolean = terms.stream()
                // if the value is not mapped in the operations map, it indicates that it is a block parameter or correlated
                .filter(operations::containsKey)
                .map(operations::get)
                .filter(operation ->
                        (isFalse(operation) && operator.equals(AND)) ||
                                (isTrue(operation) && operator.equals(OR)))
                .findFirst();
        if (constantBoolean.isPresent()) {
            return ImmutableList.of(constantBoolean.get().result());
        }

        // deduplicate terms
        // TODO this way we won't detect identical operations if they have nested blocks.
        //  the blocks will differ in parameters even if the overall semantics is the same.
        Set<Operation> uniqueSourceOperations = new HashSet<>();
        List<Value> deduplicatedTerms = terms.stream()
                .distinct()
                .filter(value -> {
                    if (operations.containsKey(value)) {
                        return uniqueSourceOperations.add(operations.get(value)); // TODO do not deduplicate non-deterministic operations
                    }
                    // if the value is not mapped in the operations map, it indicates that it is a block parameter or correlated
                    return true;
                })
                .collect(toImmutableList());

        // remove all TRUE conjuncts from AND, and all FALSE disjuncts from OR
        return deduplicatedTerms.stream()
                .filter(value -> {
                    if (operations.containsKey(value)) {
                        return !((isFalse(operations.get(value)) && operator.equals(OR)) ||
                                (isTrue(operations.get(value)) && operator.equals(AND)));
                    }
                    // if the value is not mapped in the operations map, it indicates that it is a block parameter or correlated
                    return true;
                })
                .collect(toImmutableList());
    }

    private static boolean isTrue(Operation operation)
    {
        return operation instanceof Constant constantOperation &&
                CONSTANT_RESULT.getAttribute(constantOperation.attributes()).equals(NullableValue.of(BOOLEAN, true));
    }

    private static boolean isFalse(Operation operation)
    {
        return operation instanceof Constant constantOperation &&
                CONSTANT_RESULT.getAttribute(constantOperation.attributes()).equals(NullableValue.of(BOOLEAN, false));
    }

    private static void layoutOperations(Value rootResult, Block.Builder block, Map<Value, Operation> operations)
    {
        Operation rootOperation = operations.get(rootResult);
        // rootOperation == null indicates that the value is a block parameter or correlated, and it isn't declared inside this block
        if (rootOperation != null) {
            rootOperation.arguments().stream()
                    .forEach(argument -> layoutOperations(argument, block, operations));
            block.addOperation(rootOperation);
        }
    }
}
