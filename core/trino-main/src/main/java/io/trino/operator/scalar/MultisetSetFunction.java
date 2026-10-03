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
import io.trino.spi.block.BufferedArrayValueBuilder;
import io.trino.spi.block.SqlMultiset;
import io.trino.spi.function.Convention;
import io.trino.spi.function.Description;
import io.trino.spi.function.OperatorDependency;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MultisetType;
import io.trino.spi.type.Type;
import io.trino.type.BlockTypeOperators.BlockPositionHashCode;
import io.trino.type.BlockTypeOperators.BlockPositionIsIdentical;

import static io.trino.operator.scalar.BlockSet.MAX_FUNCTION_MEMORY;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.OperatorType.HASH_CODE;
import static io.trino.spi.function.OperatorType.IDENTICAL;

@ScalarFunction(value = "set", neverFails = true)
@Description("Returns the multiset with duplicate elements removed")
public final class MultisetSetFunction
{
    // a multiset shares the array block representation, so the elements are buffered as an array block
    private final BufferedArrayValueBuilder valueBuilder;
    private final MultisetType multisetType;

    @TypeParameter("E")
    public MultisetSetFunction(@TypeParameter("E") Type elementType, @TypeParameter("multiset(E)") Type multisetType)
    {
        valueBuilder = BufferedArrayValueBuilder.createBuffered(new ArrayType(elementType));
        this.multisetType = (MultisetType) multisetType;
    }

    @TypeParameter("E")
    @SqlType("multiset(E)")
    public SqlMultiset set(
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
            @SqlType("multiset(E)") SqlMultiset multiset)
    {
        if (multiset.getSize() < 2) {
            return multiset;
        }

        Block elements = multiset.getElementBlock();
        BlockSet distinctElements = new BlockSet(elementIdentical, elementHashCode, elements.getPositionCount());
        for (int i = 0; i < elements.getPositionCount(); i++) {
            distinctElements.add(elements, i);
        }

        Block distinct = valueBuilder.build(
                distinctElements.size(),
                blockBuilder -> distinctElements.getAllWithSizeLimit(blockBuilder, "set", MAX_FUNCTION_MEMORY));
        return multisetType.toSqlMultiset(distinct);
    }
}
