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
package io.trino.operator.scalar;

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.SqlMultiset;
import io.trino.spi.function.Convention;
import io.trino.spi.function.OperatorDependency;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.type.MultisetType;
import io.trino.spi.type.Type;
import io.trino.type.BlockTypeOperators.BlockPositionHashCode;
import io.trino.type.BlockTypeOperators.BlockPositionIsIdentical;

import static io.trino.operator.scalar.BlockSet.MAX_FUNCTION_MEMORY;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.OperatorType.HASH_CODE;
import static io.trino.spi.function.OperatorType.IDENTICAL;

/// Implements `MULTISET UNION`. The ALL form concatenates the operands so multiplicities add; the
/// DISTINCT form returns the set union of the two operands.
public final class MultisetUnionFunction
{
    private MultisetUnionFunction() {}

    @ScalarFunction(value = "$multiset_union_all", hidden = true, neverFails = true)
    @TypeParameter("E")
    @SqlType("multiset(E)")
    public static SqlMultiset unionAll(
            @TypeParameter("E") Type elementType,
            @TypeParameter("multiset(E)") Type multisetType,
            @SqlType("multiset(E)") SqlMultiset left,
            @SqlType("multiset(E)") SqlMultiset right)
    {
        BlockBuilder blockBuilder = elementType.createBlockBuilder(null, left.getSize() + right.getSize());
        for (int i = 0; i < left.getSize(); i++) {
            blockBuilder.append(left.getUnderlyingElementBlock(), left.getUnderlyingElementPosition(i));
        }
        for (int i = 0; i < right.getSize(); i++) {
            blockBuilder.append(right.getUnderlyingElementBlock(), right.getUnderlyingElementPosition(i));
        }
        return ((MultisetType) multisetType).toSqlMultiset(blockBuilder.build());
    }

    @ScalarFunction(value = "$multiset_union_distinct", hidden = true, neverFails = true)
    @TypeParameter("E")
    @SqlType("multiset(E)")
    public static SqlMultiset unionDistinct(
            @TypeParameter("E") Type elementType,
            @TypeParameter("multiset(E)") Type multisetType,
            @OperatorDependency(
                    operator = IDENTICAL,
                    argumentTypes = {"E", "E"},
                    convention = @Convention(arguments = {BLOCK_POSITION, BLOCK_POSITION}, result = FAIL_ON_NULL))
            BlockPositionIsIdentical elementIdentical,
            @OperatorDependency(
                    operator = HASH_CODE,
                    argumentTypes = "E",
                    convention = @Convention(arguments = BLOCK_POSITION, result = FAIL_ON_NULL))
            BlockPositionHashCode elementHashCode,
            @SqlType("multiset(E)") SqlMultiset left,
            @SqlType("multiset(E)") SqlMultiset right)
    {
        Block leftElements = left.getElementBlock();
        Block rightElements = right.getElementBlock();
        BlockSet set = new BlockSet(elementIdentical, elementHashCode, leftElements.getPositionCount() + rightElements.getPositionCount());
        for (int i = 0; i < leftElements.getPositionCount(); i++) {
            set.add(leftElements, i);
        }
        for (int i = 0; i < rightElements.getPositionCount(); i++) {
            set.add(rightElements, i);
        }

        BlockBuilder blockBuilder = elementType.createBlockBuilder(null, set.size());
        set.getAllWithSizeLimit(blockBuilder, "$multiset_union_distinct", MAX_FUNCTION_MEMORY);
        return ((MultisetType) multisetType).toSqlMultiset(blockBuilder.build());
    }
}
