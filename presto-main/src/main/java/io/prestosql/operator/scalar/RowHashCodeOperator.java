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
package io.prestosql.operator.scalar;

import com.google.common.collect.ImmutableList;
import io.prestosql.annotation.UsedByGeneratedCode;
import io.prestosql.metadata.FunctionBinding;
import io.prestosql.metadata.SqlOperator;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeSignature;

import java.lang.invoke.MethodHandle;

import static io.prestosql.metadata.Signature.comparableWithVariadicBound;
import static io.prestosql.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.prestosql.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.prestosql.spi.function.OperatorType.HASH_CODE;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.util.Reflection.methodHandle;

public class RowHashCodeOperator
        extends SqlOperator
{
    public static final RowHashCodeOperator ROW_HASH_CODE = new RowHashCodeOperator();
    private static final MethodHandle METHOD_HANDLE = methodHandle(RowHashCodeOperator.class, "hash", Type.class, Block.class);

    private RowHashCodeOperator()
    {
        super(HASH_CODE,
                ImmutableList.of(comparableWithVariadicBound("T", "row")),
                ImmutableList.of(),
                BIGINT.getTypeSignature(),
                ImmutableList.of(new TypeSignature("T")),
                false);
    }

    @Override
    protected ScalarFunctionImplementation specialize(FunctionBinding functionBinding)
    {
        Type type = functionBinding.getTypeVariable("T");
        return new ScalarFunctionImplementation(
                FAIL_ON_NULL,
                ImmutableList.of(NEVER_NULL),
                METHOD_HANDLE.bindTo(type));
    }

    @UsedByGeneratedCode
    public static long hash(Type rowType, Block block)
    {
        BlockBuilder blockBuilder = rowType.createBlockBuilder(null, 1);
        blockBuilder.appendStructure(block);
        return rowType.hash(blockBuilder.build(), 0);
    }
}
