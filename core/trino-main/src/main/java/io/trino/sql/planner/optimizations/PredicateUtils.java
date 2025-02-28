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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.predicate.NullableValue;
import io.trino.sql.dialect.trino.Attributes;
import io.trino.sql.dialect.trino.ProgramBuilder;
import io.trino.sql.dialect.trino.operation.Constant;
import io.trino.sql.dialect.trino.operation.FieldReference;
import io.trino.sql.dialect.trino.operation.FieldSelection;
import io.trino.sql.dialect.trino.operation.Logical;
import io.trino.sql.dialect.trino.operation.Return;
import io.trino.sql.newir.Block;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.SourceNode;
import io.trino.sql.newir.Type;
import io.trino.sql.newir.Value;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.dialect.trino.Attributes.CONSTANT_RESULT;
import static io.trino.sql.dialect.trino.Attributes.FIELD_INDEX;
import static io.trino.sql.dialect.trino.Attributes.FIELD_NAME;
import static io.trino.sql.dialect.trino.Attributes.LogicalOperator.AND;
import static io.trino.sql.dialect.trino.Attributes.LogicalOperator.OR;
import static io.trino.sql.dialect.trino.TrinoDialect.trinoType;

public class PredicateUtils
{
    private PredicateUtils()
    {}

    /**
     * Create a Block with conjunction of predicates from all component blocks.
     * The resulting Block has the same parameters as the first component Block.
     * At least one component Block must be provided.
     * <p>
     * TODO The created conjunction is flattened. Duplicate and trivial conjuncts are removed. In case of a false conjunct, the result is folded to constant false. Mind non-determinism.
     */
    public static Block conjunction(List<Block> blocks, ProgramBuilder.ValueNameAllocator nameAllocator)
    {
        return logical(blocks, AND, nameAllocator);
    }

    /**
     * Create a Block with disjunction of predicates from all component blocks.
     * The resulting Block has the same parameters as the first component Block.
     * At least one component Block must be provided.
     * <p>
     * TODO The created disjunction is flattened. Duplicate and trivial disjuncts are removed. In case of a true disjunct, the result is folded to constant true. Mind non-determinism.
     */
    public static Block disjunction(List<Block> blocks, ProgramBuilder.ValueNameAllocator nameAllocator)
    {
        return logical(blocks, OR, nameAllocator);
    }

    private static Block logical(List<Block> blocks, Attributes.LogicalOperator operator, ProgramBuilder.ValueNameAllocator nameAllocator)
    {
        // to create a block, we need a block parameter representing the input. We cannot create a block when the input type is not known
        checkArgument(!blocks.isEmpty(), "cannot combine 0 blocks");

        checkArgument(blocks.stream().allMatch(block -> trinoType(block.getReturnedType()).equals(BOOLEAN)), "expected blocks returning boolean");

        if (blocks.size() == 1) {
            // TODO flatten logical expressions within a single block
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
                if (!(operation instanceof Return)) {
                    result.addOperation(remapValues(operation, parameterMap));
                }
                else {
                    terms.add(((Return) operation).argument()); // TODO at least flatten the top-level conjunctions / disjunctions
                }
            }
        }
        Logical logical = new Logical(nameAllocator.newName(), terms.build(), operator, ImmutableList.of()); // TODO pass source attributes
        result.addOperation(logical);

        // add Return operation to finish the Block
        Return returnOperation = new Return(nameAllocator.newName(), logical.result(), logical.attributes());
        result.addOperation(returnOperation);

        return result.build();
    }

    // TODO add method to remap a Block to use parameters of another Block.
    //  first, it should check if parameter lists are compatible regarding the types
    //  then, create parameter mapping
    //  last, remap all operations in the block
    //  each operation should implement `remapValues(Map)`
    //
     //
    // TODO Note: we should also remap field names. For now, we skip it because field names are always the same. Soon we will shift to referring by index.
    public static Operation remapValues(Operation operation, Map<Value, Value> map)
    {
        if (operation instanceof FieldSelection fieldSelection) {
            return new FieldSelection(
                    fieldSelection.result().name(),
                    Optional.ofNullable(map.get(fieldSelection.base())).orElse(fieldSelection.base()),
                    FIELD_NAME.getAttribute(fieldSelection.attributes()),
                    ImmutableMap.of());
        }
        if (operation instanceof FieldReference fieldReference) {
            return new FieldReference(
                    fieldReference.result().name(),
                    Optional.ofNullable(map.get(fieldReference.base())).orElse(fieldReference.base()),
                    FIELD_INDEX.getAttribute(fieldReference.attributes()),
                    ImmutableMap.of());
        }
        // TODO this is incomplete. We must rewrite _all_ references to mapped values in operation's arguments and in nested blocks, for all types of Operations.
        if (!operation.regions().isEmpty()) {
            throw new UnsupportedOperationException();
        }
        if (operation.arguments().stream()
                .anyMatch(map.keySet()::contains)) {
            throw new UnsupportedOperationException();
        }
        return operation;
    }

    public static Block removeConjuncts(Block predicate, Block toRemove)
    {
        checkArgument(trinoType(predicate.getReturnedType()).equals(BOOLEAN), "expected block returning boolean");
        checkArgument(trinoType(toRemove.getReturnedType()).equals(BOOLEAN), "expected block returning boolean");
        checkArgument(
                predicate.parameters().stream().map(Block.Parameter::type).collect(toImmutableList())
                        .equals(toRemove.parameters().stream().map(Block.Parameter::type).collect(toImmutableList())),
                "incompatible blocks: parameters mismatch");
        // TODO
        //  - remap Block `toRemove` so that it uses parameyters of `predicate`
        //  - extract conjuncts of `predicate` and of `toRemove`
        //  - compare them by equality with and remove if equal
        return predicate;
    }

    public static boolean isTrue(Block block, Map<Value, SourceNode> valueMap)
    {
        checkArgument(trinoType(block.getReturnedType()).equals(BOOLEAN), "expected block returning boolean");

        return valueMap.get(((Return) block.getTerminalOperation()).argument()) instanceof Constant constant &&
                CONSTANT_RESULT.getAttribute(constant.attributes()).equals(NullableValue.of(BOOLEAN, true));
    }

    // TODO support with multiple Block parameters
    public static Block truePredicate(Optional<String> blockName, Block.Parameter parameter, ProgramBuilder.ValueNameAllocator nameAllocator)
    {
        Block.Builder builder = new Block.Builder(blockName, ImmutableList.of(parameter));
        Constant constantTrue = new Constant(nameAllocator.newName(), BOOLEAN, true);
        Return returnOperation = new Return(nameAllocator.newName(), constantTrue.result(), constantTrue.attributes());
        builder.addOperation(constantTrue);
        builder.addOperation(returnOperation);
        return builder.build();
    }
}
