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
package io.trino.operator.aggregation;

import com.google.common.annotations.VisibleForTesting;
import io.trino.operator.aggregation.state.NullableLongState;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.BlockIndex;
import io.trino.spi.function.BlockPosition;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.Convention;
import io.trino.spi.function.Description;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OperatorDependency;
import io.trino.spi.function.OperatorType;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;

import java.lang.invoke.MethodHandle;

import static io.airlift.slice.Slices.wrappedLongArray;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.type.VarbinaryType.VARBINARY;

@AggregationFunction("checksum")
@Description("Checksum of the given values")
public final class ChecksumAggregationFunction
{
    @VisibleForTesting
    public static final long PRIME64 = 0x9E3779B185EBCA87L;

    private ChecksumAggregationFunction() {}

    @InputFunction
    @TypeParameter("T")
    public static void input(
            @OperatorDependency(
                    operator = OperatorType.XX_HASH_64,
                    argumentTypes = "T",
                    convention = @Convention(arguments = BLOCK_POSITION, result = FAIL_ON_NULL))
                    MethodHandle xxHash64Operator,
            @AggregationState NullableLongState state,
            @NullablePosition @BlockPosition @SqlType("T") Block block,
            @BlockIndex int position)
            throws Throwable
    {
        state.setNull(false);
        if (block.isNull(position)) {
            state.setValue(state.getValue() + PRIME64);
        }
        else {
            long valueHash = (long) xxHash64Operator.invokeExact(block, position);
            state.setValue(state.getValue() + valueHash * PRIME64);
        }
    }

    @CombineFunction
    public static void combine(
            @AggregationState NullableLongState state,
            @AggregationState NullableLongState otherState)
    {
        state.setNull(state.isNull() && otherState.isNull());
        state.setValue(state.getValue() + otherState.getValue());
    }

    @OutputFunction("VARBINARY")
    public static void output(
            @AggregationState NullableLongState state,
            BlockBuilder out)
    {
        if (state.isNull()) {
            out.appendNull();
        }
        else {
            VARBINARY.writeSlice(out, wrappedLongArray(state.getValue()));
        }
    }
}
