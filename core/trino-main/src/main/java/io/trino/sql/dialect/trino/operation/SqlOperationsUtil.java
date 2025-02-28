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
package io.trino.sql.dialect.trino.operation;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.sql.dialect.trino.ProgramBuilder;
import io.trino.sql.newir.Block;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.Value;
import io.trino.sql.planner.optimizations.CteReuse;

import java.util.List;
import java.util.Optional;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.sql.dialect.trino.Attributes.FIELD_INDEX;
import static io.trino.sql.dialect.trino.Attributes.FIELD_NAME;
import static io.trino.sql.dialect.trino.TrinoDialect.irType;
import static io.trino.sql.dialect.trino.TrinoDialect.trinoType;
import static io.trino.sql.dialect.trino.TypeConstraint.IS_RELATION_ROW;
import static io.trino.sql.newir.Region.singleBlockRegion;
import static org.assertj.core.util.Preconditions.checkArgument;
import static org.assertj.core.util.Preconditions.checkState;

public class SqlOperationsUtil
{
    private SqlOperationsUtil()
    {}

    /**
     * Check that fields in the oldType are mapped to fields in newType with the same type.
     */
    public static void validateMappedTypes(Type oldType, Type newType, CteReuse.Mapping mapping)
    {
        checkArgument(IS_RELATION_ROW.test(oldType) && IS_RELATION_ROW.test(newType), "expected relation row type");

        if (oldType instanceof RowType oldRowType && newType instanceof RowType newRowType) {
            mapping.fieldIndexMapping().entrySet().stream()
                    .forEach(entry -> {
                        Type oldFieldType = oldRowType.getFields().get(entry.getKey()).getType();
                        Type newFieldType = newRowType.getFields().get(entry.getValue()).getType();
                        checkState(oldFieldType.equals(newFieldType), "field type mismatch");
                    });
        }
    }

    /**
     * Rebase block onto baseType: adjust block parameter to baseType and update field references using mapping.
     * Block parameter must be of relation row type.
     * Optional.empty is returned when some field reference cannot be remapped.
     * TODO support rebasing with multiple block parameters: rebase one parameter at a time.
     */
    static Optional<Block> rebaseBlock(Block block, Type baseType, CteReuse.Mapping mapping, ProgramBuilder.ValueNameAllocator nameAllocator)
    {
        // TODO use the method below
        checkArgument(block.parameters().size() == 1, "only single-parameter blocks here, for multi-parameter use the other method");
        validateMappedTypes(trinoType(getOnlyElement(block.parameters()).type()), baseType, mapping);

        Block.Parameter oldParameter = getOnlyElement(block.parameters());
        Block.Parameter newParameter = new Block.Parameter(nameAllocator.newName(), irType(baseType));

        Block.Builder newBlockBuilder = new Block.Builder(block.name(), ImmutableList.of(newParameter));
        for (Operation operation : block.operations()) {
            Optional<Operation> remapped = remapFieldReferences(operation, oldParameter, newParameter, mapping);
            if (remapped.isEmpty()) {
                return Optional.empty();
            }
            newBlockBuilder.addOperation(remapped.get());
        }
        return Optional.of(newBlockBuilder.build());
    }

    static Optional<Block> rebaseBlock(Block block, int parameterIndex, Type baseType, CteReuse.Mapping mapping, ProgramBuilder.ValueNameAllocator nameAllocator)
    {
        validateMappedTypes(trinoType(block.parameters().get(parameterIndex).type()), baseType, mapping);

        Block.Parameter oldParameter = block.parameters().get(parameterIndex);
        Block.Parameter newParameter = new Block.Parameter(nameAllocator.newName(), irType(baseType));

        ImmutableList.Builder<Block.Parameter> newParametersBuilder = ImmutableList.builder();
        for (int i = 0; i < block.parameters().size(); i++) {
            if (i != parameterIndex) {
                newParametersBuilder.add(block.parameters().get(i));
            }
            else {
                newParametersBuilder.add(newParameter);
            }
        }

        Block.Builder newBlockBuilder = new Block.Builder(block.name(), newParametersBuilder.build());
        for (Operation operation : block.operations()) {
            Optional<Operation> remapped = remapFieldReferences(operation, oldParameter, newParameter, mapping);
            if (remapped.isEmpty()) {
                return Optional.empty();
            }
            newBlockBuilder.addOperation(remapped.get());
        }
        return Optional.of(newBlockBuilder.build());
    }

    /**
     * Remap field references in the operation. Remap nested regions recursively.
     * References to oldValue are replaced with references to newValue. The names and indexes of referenced fields are updated according to mapping.
     * The operation result name is not changed so that references to the result remain valid. The result type remains the same because the mapping preserves types.
     * Optional.empty is returned when some field reference cannot be remapped.
     * TODO support multiple values and multiple mappings. For now, only one value.
     */
    private static Optional<Operation> remapFieldReferences(Operation operation, Value oldValue, Value newValue, CteReuse.Mapping mapping)
    {
        if (operation instanceof FieldSelection fieldSelection && fieldSelection.base().equals(oldValue)) {
            String newFieldName = mapping.fieldNameMapping().get(FIELD_NAME.getAttribute(fieldSelection.attributes()));
            if (newFieldName == null) {
                return Optional.empty();
            }
            return Optional.of(new FieldSelection(fieldSelection.result().name(), newValue, newFieldName, ImmutableMap.of()));
        }
        if (operation instanceof FieldReference fieldReference && fieldReference.base().equals(oldValue)) {
            Integer newFieldIndex = mapping.fieldIndexMapping().get(FIELD_INDEX.getAttribute(fieldReference.attributes()));
            if (newFieldIndex == null) {
                return Optional.empty();
            }
            return Optional.of(new FieldReference(fieldReference.result().name(), newValue, newFieldIndex, ImmutableMap.of()));
        }
        Preconditions.checkState(
                operation.arguments().stream()
                        .noneMatch(argument -> argument.equals(oldValue)),
                "illegal reference to relational parameter. Only field access operations are allowed");

        ImmutableList.Builder<Region> remappedRegions = ImmutableList.builder();
        for (Region region : operation.regions()) {
            Optional<Block> remappedBlock = remapFieldReferences(region.getOnlyBlock(), oldValue, newValue, mapping);
            if (remappedBlock.isEmpty()) {
                return Optional.empty();
            }
            remappedRegions.add(singleBlockRegion(remappedBlock.get()));
        }
        return Optional.of(((SqlOperation) operation).withRegions(remappedRegions.build()));
    }

    /**
     * Remap field references in all operations of the block.
     * References to oldValue are replaced with references to newValue. The names and indexes of referenced fields are updated according to mapping.
     * Optional.empty is returned when some field reference cannot be remapped.
     * TODO support multiple values and multiple mappings. For now, only one value.
     */
    private static Optional<Block> remapFieldReferences(Block block, Value oldValue, Value newValue, CteReuse.Mapping mapping)
    {
        Block.Builder builder = new Block.Builder(block.name(), block.parameters());
        for (Operation operation : block.operations()) {
            Optional<Operation> remappedOperation = remapFieldReferences(operation, oldValue, newValue, mapping);
            if (remappedOperation.isEmpty()) {
                return Optional.empty();
            }
            builder.addOperation(remappedOperation.get());
        }
        return Optional.of(builder.build());
    }
}
