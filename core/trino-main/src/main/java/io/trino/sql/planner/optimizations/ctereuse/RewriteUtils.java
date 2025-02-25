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
import io.trino.spi.type.Type;
import io.trino.sql.dialect.trino.ProgramBuilder;
import io.trino.sql.dialect.trino.operation.FieldReference;
import io.trino.sql.dialect.trino.operation.TrinoOperation;
import io.trino.sql.newir.Block;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.Value;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.spi.type.EmptyRowType.EMPTY_ROW;
import static io.trino.sql.dialect.trino.Attributes.FIELD_INDEX;
import static io.trino.sql.dialect.trino.TrinoDialect.irType;
import static io.trino.sql.dialect.trino.TrinoDialect.trinoType;
import static io.trino.sql.dialect.trino.TypeConstraint.IS_RELATION_ROW;
import static io.trino.sql.newir.Region.singleBlockRegion;

public class RewriteUtils
{
    private RewriteUtils()
    {}

    public static Optional<Block> rebaseBlock(Block block, Type newType, FieldMapping fieldMapping, ProgramBuilder.ValueNameAllocator nameAllocator)
    {
        checkArgument(block.parameters().size() == 1, "expected single parameter");
        return rebaseBlock(block, 0, newType, fieldMapping, nameAllocator);
    }

    /**
     * Change one of the block's parameters so that it uses newType instead of its original type, and remap nested operations accordingly.
     * - replace the parameter with a new parameter of newType
     * - update nested field references using mapping
     * Note: when newType is equal to the original type, and mapping is identity, the original block is returned.
     * <p>
     * The changed parameter and the new parameter must be of relation row type.
     * Optional.empty is returned when some nested field reference cannot be remapped. It can happen when there is no mapping
     * for some field in the old parameter. For example, when the new parameter type is narrower than the original type.
     * <p>
     * Example:
     * Block: Row(a, b, c) -> {a - b}
     * cannot be rebased on type Row(d, e) with mapping {a -> d, c -> e}, because there is no mapping for field b.
     * This block can be rebased on type Row(f, g) with mapping {a -> g, b -> f},
     * and the result is Block: Row(f, g) -> {g - f}
     *
     * @param block -- the block to be rebased
     * @param index -- the index of the parameter to be replaced
     * @param newType -- new type for the index-th parameter
     * @param fieldMapping -- mapping from fields of the old parameter to fields of the new parameter, used to remap nested operations
     * @param nameAllocator -- ValueNameAllocator needed to allocate a value name for the new parameter
     */
    public static Optional<Block> rebaseBlock(Block block, int index, Type newType, FieldMapping fieldMapping, ProgramBuilder.ValueNameAllocator nameAllocator)
    {
        Block.Parameter oldParameter = block.parameters().get(index);
        Type oldType = trinoType(oldParameter.type());

        if (oldType.equals(newType) && fieldMapping.isIdentity(oldType)) {
            return Optional.of(block);
        }

        validateMappedTypes(oldType, newType, fieldMapping);

        Block.Parameter newParameter = new Block.Parameter(nameAllocator.newName(), irType(newType));
        List<Block.Parameter> newParameters = new ArrayList<>(block.parameters());
        newParameters.set(index, newParameter);

        Block.Builder newBlockBuilder = new Block.Builder(block.name(), newParameters);
        for (Operation operation : block.operations()) {
            Optional<Operation> remapped = remapFieldReferences(operation, oldParameter, newParameter, fieldMapping);
            if (remapped.isEmpty()) {
                return Optional.empty();
            }
            newBlockBuilder.addOperation(remapped.get());
        }
        return Optional.of(newBlockBuilder.build());
    }

    /**
     * Remap field references in the operation. Remap nested regions recursively.
     * References to oldValue are replaced with references to newValue. The indexes of referenced fields are updated according to mapping.
     * The operation result type remains the same because the mapping preserves types. Also, the operation result name is not changed so that references to the result remain valid.
     * Optional.empty is returned when some field reference cannot be remapped.
     */
    private static Optional<Operation> remapFieldReferences(Operation operation, Value oldValue, Value newValue, FieldMapping fieldMapping)
    {
        if (operation instanceof FieldReference fieldReference && fieldReference.base().equals(oldValue)) {
            return Optional.ofNullable(fieldMapping.get(FIELD_INDEX.getAttribute(fieldReference.attributes())))
                    .map(newFieldIndex -> new FieldReference(fieldReference.result().name(), newValue, newFieldIndex, ImmutableMap.of())); // TODO missing source attributes
        }

        checkState(
                operation.arguments().stream()
                        .noneMatch(argument -> argument.equals(oldValue)),
                "illegal reference to relational parameter. Only field access operations are allowed");

        ImmutableList.Builder<Region> remappedRegions = ImmutableList.builder();
        for (Region region : operation.regions()) {
            Optional<Block> remapped = remapFieldReferences(region.getOnlyBlock(), oldValue, newValue, fieldMapping);
            if (remapped.isEmpty()) {
                return Optional.empty();
            }
            remappedRegions.add(singleBlockRegion(remapped.get()));
        }
        return Optional.of(((TrinoOperation) operation).withRegions(remappedRegions.build()));
    }

    /**
     * Remap field references in all operations of the block.
     * References to oldValue are replaced with references to newValue. The indexes of referenced fields are updated according to mapping.
     * Optional.empty is returned when some field reference cannot be remapped.
     */
    private static Optional<Block> remapFieldReferences(Block block, Value oldValue, Value newValue, FieldMapping fieldMapping)
    {
        Block.Builder builder = new Block.Builder(block.name(), block.parameters());
        for (Operation operation : block.operations()) {
            Optional<Operation> remapped = remapFieldReferences(operation, oldValue, newValue, fieldMapping);
            if (remapped.isEmpty()) {
                return Optional.empty();
            }
            builder.addOperation(remapped.get());
        }
        return Optional.of(builder.build());
    }

    /**
     * Return indexes of all fields of the parameter that are referenced in the block. Visit nested operations recursively.
     */
    public static Set<Integer> extractReferencedFields(Block block, Block.Parameter parameter)
    {
        checkArgument(IS_RELATION_ROW.test(trinoType(parameter.type())), "expected parameter of relation row type");

        if (trinoType(parameter.type()).equals(EMPTY_ROW)) {
            // no fields to be referenced
            return ImmutableSet.of();
        }

        return block.operations().stream()
                .map(operation -> extractReferencedFields(operation, parameter))
                .flatMap(Set::stream)
                .collect(toImmutableSet());
    }

    /**
     * Return indexes of all fields of the parameter that are referenced in the operation. Visit nested blocks recursively.
     */
    private static Set<Integer> extractReferencedFields(Operation operation, Block.Parameter parameter)
    {
        checkArgument(IS_RELATION_ROW.test(trinoType(parameter.type())), "expected parameter of relation row type");

        if (trinoType(parameter.type()).equals(EMPTY_ROW)) {
            // no fields to be referenced
            return ImmutableSet.of();
        }

        if (operation instanceof FieldReference fieldReference && fieldReference.base().equals(parameter)) {
            return ImmutableSet.of(FIELD_INDEX.getAttribute(fieldReference.attributes()));
        }

        checkState(
                operation.arguments().stream()
                        .noneMatch(argument -> argument.equals(parameter)),
                "illegal reference to relational parameter. Only field access operations are allowed");

        return operation.regions().stream()
                .map(Region::blocks)
                .flatMap(List::stream)
                .map(block -> extractReferencedFields(block, parameter))
                .flatMap(Set::stream)
                .collect(toImmutableSet());
    }

    /**
     * Replace parameters of the block with newParameters. Remap operations in the block accordingly.
     * The old and new parameters must match in type.
     */
    public static Block remapParameters(Block block, List<Block.Parameter> newParameters)
    {
        checkArgument(
                block.parameters().stream()
                        .map(Block.Parameter::type)
                        .collect(toImmutableList())
                        .equals(newParameters.stream()
                                .map(Block.Parameter::type)
                                .collect(toImmutableList())),
                "type mismatch");

        ImmutableMap.Builder<Value, Value> parameterMapBuilder = ImmutableMap.builder();
        for (int i = 0; i < block.parameters().size(); i++) {
            parameterMapBuilder.put(block.parameters().get(i), newParameters.get(i));
        }
        Map<Value, Value> parameterMap = parameterMapBuilder.buildOrThrow();
        Block.Builder remappedBlock = new Block.Builder(block.name(), newParameters);
        block.operations().stream()
                .map(operation -> remapValues(operation, parameterMap))
                .forEach(remappedBlock::addOperation);

        return remappedBlock.build();
    }

    /**
     * Replace references to old values with references to new values according to the map.
     * The old and new values must match in type.
     * Only operation arguments are replaced. Nested block parameter declarations remain unchanged.
     */
    public static Operation remapValues(Operation operation, Map<Value, Value> map)
    {
        checkArgument(
                map.entrySet().stream()
                        .allMatch(entry -> entry.getKey().type().equals(entry.getValue().type())),
                "type mismatch");

        for (int i = 0; i < operation.arguments().size(); i++) {
            Value oldArgument = operation.arguments().get(i);
            Value newArgument = map.get(oldArgument);
            if (newArgument != null) {
                operation = ((TrinoOperation) operation).withArgument(newArgument, i);
            }
        }

        List<Region> newRegions = operation.regions().stream()
                .map(Region::getOnlyBlock)
                .map(block -> {
                    Block.Builder newBlock = new Block.Builder(block.name(), block.parameters());
                    for (Operation oldOperation : block.operations()) {
                        newBlock.addOperation(remapValues(oldOperation, map));
                    }
                    return newBlock.build();
                })
                .map(Region::singleBlockRegion)
                .collect(toImmutableList());

        return ((TrinoOperation) operation).withRegions(newRegions);
    }

    /**
     * Check that fields in the oldType are mapped to fields in newType with the same type.
     */
    public static void validateMappedTypes(Type oldType, Type newType, FieldMapping fieldMapping)
    {
        checkArgument(IS_RELATION_ROW.test(oldType) && IS_RELATION_ROW.test(newType), "expected relation row type");

        List<Type> oldTypes = oldType.getTypeParameters();
        List<Type> newTypes = newType.getTypeParameters();

        for (int i = 0; i < oldTypes.size(); i++) {
            Integer newIndex = fieldMapping.get(i);
            checkArgument(newIndex == null || (newIndex >= 0 && newIndex < newTypes.size() && oldTypes.get(i).equals(newTypes.get(newIndex))), "invalid mapping");
        }
    }
}
