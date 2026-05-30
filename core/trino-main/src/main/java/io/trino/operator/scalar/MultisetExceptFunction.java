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

/// Implements `MULTISET EXCEPT`. The ALL form subtracts multiplicities (each value's count in the
/// result is its left count minus its right count, floored at zero); the DISTINCT form keeps each
/// value of the left operand that does not occur in the right, once.
public final class MultisetExceptFunction
{
    private MultisetExceptFunction() {}

    @ScalarFunction(value = "$multiset_except_all", hidden = true, neverFails = true)
    @TypeParameter("E")
    @SqlType("multiset(E)")
    public static SqlMultiset exceptAll(
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
        // cancel each left element against an available right occurrence of the identical value; the
        // left elements that remain uncancelled form the difference. A multiplicity map of the right
        // operand makes the cancellation a single hashed lookup, so this is O(n + m) rather than the
        // O(n * m) of a pairwise scan.
        Block leftElements = left.getElementBlock();
        Block rightElements = right.getElementBlock();
        MultisetMultiplicities rightCounts = MultisetMultiplicities.of(elementIdentical, elementHashCode, rightElements);
        BlockBuilder blockBuilder = elementType.createBlockBuilder(null, leftElements.getPositionCount());
        for (int i = 0; i < leftElements.getPositionCount(); i++) {
            if (!rightCounts.consume(leftElements, i)) {
                blockBuilder.append(leftElements.getUnderlyingValueBlock(), leftElements.getUnderlyingValuePosition(i));
            }
        }
        return ((MultisetType) multisetType).toSqlMultiset(blockBuilder.build());
    }

    @ScalarFunction(value = "$multiset_except_distinct", hidden = true, neverFails = true)
    @TypeParameter("E")
    @SqlType("multiset(E)")
    public static SqlMultiset exceptDistinct(
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
        BlockSet rightSet = new BlockSet(elementIdentical, elementHashCode, rightElements.getPositionCount());
        for (int i = 0; i < rightElements.getPositionCount(); i++) {
            rightSet.add(rightElements, i);
        }

        BlockSet result = new BlockSet(elementIdentical, elementHashCode, leftElements.getPositionCount());
        for (int i = 0; i < leftElements.getPositionCount(); i++) {
            if (!rightSet.contains(leftElements, i)) {
                result.add(leftElements, i);
            }
        }

        BlockBuilder blockBuilder = elementType.createBlockBuilder(null, result.size());
        result.getAllWithSizeLimit(blockBuilder, "$multiset_except_distinct", MAX_FUNCTION_MEMORY);
        return ((MultisetType) multisetType).toSqlMultiset(blockBuilder.build());
    }
}
