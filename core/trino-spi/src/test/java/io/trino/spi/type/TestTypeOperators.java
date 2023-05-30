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
package io.trino.spi.type;

import com.google.common.collect.ImmutableList;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.InvocationConvention.InvocationArgumentConvention;
import org.junit.jupiter.api.Test;

import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static com.google.common.base.Verify.verify;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION_NOT_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BOXED_NULLABLE;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.FLAT;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NULL_FLAG;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FLAT_RETURN;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.lang.invoke.MethodHandles.exactInvoker;
import static org.assertj.core.api.Assertions.assertThat;

class TestTypeOperators
{
    @Test
    void testDistinctGenerator()
            throws Throwable
    {
        TypeOperators typeOperators = new TypeOperators();

        List<InvocationArgumentConvention> argumentConventions = ImmutableList.of(NEVER_NULL, BOXED_NULLABLE, NULL_FLAG, BLOCK_POSITION_NOT_NULL, BLOCK_POSITION, FLAT);
        List<Long> testArguments = Arrays.asList(0L, 1L, 2L, null);
        for (InvocationArgumentConvention leftConvention : argumentConventions) {
            for (InvocationArgumentConvention rightConvention : argumentConventions) {
                MethodHandle operator = typeOperators.getDistinctFromOperator(BIGINT, simpleConvention(FAIL_ON_NULL, leftConvention, rightConvention));
                operator = exactInvoker(operator.type()).bindTo(operator);

                for (Long leftArgument : testArguments) {
                    for (Long rightArgument : testArguments) {
                        if (!leftConvention.isNullable() && leftArgument == null || !rightConvention.isNullable() && rightArgument == null) {
                            continue;
                        }
                        boolean expected = !Objects.equals(leftArgument, rightArgument);

                        ArrayList<Object> arguments = new ArrayList<>();
                        addCallArgument(typeOperators, leftConvention, leftArgument, arguments);
                        addCallArgument(typeOperators, rightConvention, rightArgument, arguments);
                        assertThat((boolean) operator.invokeWithArguments(arguments)).isEqualTo(expected);
                    }
                }
            }
        }
    }

    private static void addCallArgument(TypeOperators typeOperators, InvocationArgumentConvention convention, Long value, List<Object> callArguments)
            throws Throwable
    {
        switch (convention) {
            case NEVER_NULL, BOXED_NULLABLE -> callArguments.add(value);
            case NULL_FLAG -> {
                callArguments.add(value == null ? 0 : value);
                callArguments.add(value == null);
            }
            case BLOCK_POSITION, BLOCK_POSITION_NOT_NULL -> {
                BlockBuilder blockBuilder = BIGINT.createBlockBuilder(null, 1);
                if (value == null) {
                    verify(convention == BLOCK_POSITION);
                    blockBuilder.appendNull();
                }
                else {
                    BIGINT.writeLong(blockBuilder, value);
                }
                callArguments.add(blockBuilder.build());
                callArguments.add(0);
            }
            case FLAT -> {
                verify(value != null);

                byte[] fixedSlice = new byte[BIGINT.getFlatFixedSize()];
                MethodHandle writeFlat = typeOperators.getReadValueOperator(BIGINT, simpleConvention(FLAT_RETURN, NEVER_NULL));
                writeFlat.invoke(value, fixedSlice, 0, new byte[0], 0);

                callArguments.add(fixedSlice);
                callArguments.add(0);
                callArguments.add(new byte[0]);
            }
            default -> throw new UnsupportedOperationException();
        }
    }
}
