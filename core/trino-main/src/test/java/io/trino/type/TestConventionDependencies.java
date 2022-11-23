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
package io.trino.type;

import io.trino.metadata.InternalFunctionBundle;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.function.BlockIndex;
import io.trino.spi.function.BlockPosition;
import io.trino.spi.function.Convention;
import io.trino.spi.function.FunctionDependency;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;
import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.lang.invoke.MethodHandle;

import static com.google.common.base.Throwables.throwIfInstanceOf;
import static io.trino.operator.scalar.ApplyFunction.APPLY_FUNCTION;
import static io.trino.operator.scalar.InvokeFunction.INVOKE_FUNCTION;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.type.IntegerType.INTEGER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestConventionDependencies
{
    private QueryAssertions assertions;

    @BeforeAll
    public void init()
    {
        assertions = new QueryAssertions();

        assertions.addFunctions(InternalFunctionBundle.builder()
                .scalar(RegularConvention.class)
                .scalar(BlockPositionConvention.class)
                .scalar(Add.class)
                .build());

        assertions.addFunctions(new InternalFunctionBundle(APPLY_FUNCTION, INVOKE_FUNCTION));
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    public void testConventionDependencies()
    {
        assertThat(assertions.function("regular_convention", "1", "1"))
                .isEqualTo(2);

        assertThat(assertions.function("regular_convention", "50", "10"))
                .isEqualTo(60);

        assertThat(assertions.function("regular_convention", "1", "0"))
                .isEqualTo(1);

        assertThat(assertions.function("block_position_convention", "ARRAY[1, 2, 3]"))
                .isEqualTo(6);

        assertThat(assertions.function("block_position_convention", "ARRAY[25, 0, 5]"))
                .isEqualTo(30);

        assertThat(assertions.function("block_position_convention", "ARRAY[56, 275, 36]"))
                .isEqualTo(367);
    }

    @ScalarFunction("regular_convention")
    public static final class RegularConvention
    {
        @SqlType(StandardTypes.INTEGER)
        public static long testRegularConvention(
                @FunctionDependency(name = "add",
                        argumentTypes = {StandardTypes.INTEGER, StandardTypes.INTEGER},
                        convention = @Convention(arguments = {NEVER_NULL, NEVER_NULL}, result = FAIL_ON_NULL)) MethodHandle function,
                @SqlType(StandardTypes.INTEGER) long left,
                @SqlType(StandardTypes.INTEGER) long right)
        {
            try {
                return (long) function.invokeExact(left, right);
            }
            catch (Throwable t) {
                throwIfInstanceOf(t, Error.class);
                throwIfInstanceOf(t, TrinoException.class);
                throw new TrinoException(GENERIC_INTERNAL_ERROR, t);
            }
        }
    }

    @ScalarFunction("block_position_convention")
    public static final class BlockPositionConvention
    {
        @SqlType(StandardTypes.INTEGER)
        public static long testBlockPositionConvention(
                @FunctionDependency(
                        name = "add",
                        argumentTypes = {StandardTypes.INTEGER, StandardTypes.INTEGER},
                        convention = @Convention(arguments = {NEVER_NULL, BLOCK_POSITION}, result = FAIL_ON_NULL)) MethodHandle function,
                @SqlType("array(integer)") Block array)
        {
            long sum = 0;
            for (int i = 0; i < array.getPositionCount(); i++) {
                try {
                    sum = (long) function.invokeExact(sum, array, i);
                }
                catch (Throwable t) {
                    throwIfInstanceOf(t, Error.class);
                    throwIfInstanceOf(t, TrinoException.class);
                    throw new TrinoException(GENERIC_INTERNAL_ERROR, t);
                }
            }
            return sum;
        }
    }

    @ScalarFunction("add")
    public static final class Add
    {
        @SqlType(StandardTypes.INTEGER)
        public static long add(
                @SqlType(StandardTypes.INTEGER) long left,
                @SqlType(StandardTypes.INTEGER) long right)
        {
            return Math.addExact((int) left, (int) right);
        }

        @SqlType(StandardTypes.INTEGER)
        public static long addBlockPosition(
                @SqlType(StandardTypes.INTEGER) long first,
                @BlockPosition @SqlType(value = StandardTypes.INTEGER, nativeContainerType = long.class) Block block,
                @BlockIndex int position)
        {
            return Math.addExact((int) first, (int) INTEGER.getLong(block, position));
        }
    }
}
