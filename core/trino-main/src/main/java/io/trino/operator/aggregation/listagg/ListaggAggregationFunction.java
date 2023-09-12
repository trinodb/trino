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
package io.trino.operator.aggregation.listagg;

import com.google.common.annotations.VisibleForTesting;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.VariableWidthBlockBuilder;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.BlockIndex;
import io.trino.spi.function.BlockPosition;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.Description;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlType;

import static io.trino.spi.StandardErrorCode.EXCEEDED_FUNCTION_MEMORY_LIMIT;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.block.PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
import static java.lang.String.format;

@AggregationFunction(value = "listagg", isOrderSensitive = true)
@Description("concatenates the input values with the specified separator")
public final class ListaggAggregationFunction
{
    private static final int MAX_OUTPUT_LENGTH = DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
    private static final int MAX_OVERFLOW_FILLER_LENGTH = 65_536;

    private ListaggAggregationFunction() {}

    @InputFunction
    public static void input(
            @AggregationState ListaggAggregationState state,
            @BlockPosition @SqlType("VARCHAR") Block value,
            @SqlType("VARCHAR") Slice separator,
            @SqlType("BOOLEAN") boolean overflowError,
            @SqlType("VARCHAR") Slice overflowFiller,
            @SqlType("BOOLEAN") boolean showOverflowEntryCount,
            @BlockIndex int position)
    {
        if (state.isEmpty()) {
            if (overflowFiller.length() > MAX_OVERFLOW_FILLER_LENGTH) {
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, format("Overflow filler length %d exceeds maximum length %d", overflowFiller.length(), MAX_OVERFLOW_FILLER_LENGTH));
            }
            // Set the parameters of the LISTAGG command within the state so that
            // they can be used within the `output` function
            state.setSeparator(separator);
            state.setOverflowError(overflowError);
            state.setOverflowFiller(overflowFiller);
            state.setShowOverflowEntryCount(showOverflowEntryCount);
        }
        state.add(value, position);
    }

    @CombineFunction
    public static void combine(@AggregationState ListaggAggregationState state, @AggregationState ListaggAggregationState otherState)
    {
        Slice previousSeparator = state.getSeparator();
        if (previousSeparator == null) {
            state.setSeparator(otherState.getSeparator());
            state.setOverflowError(otherState.isOverflowError());
            state.setOverflowFiller(otherState.getOverflowFiller());
            state.setShowOverflowEntryCount(otherState.showOverflowEntryCount());
        }

        state.merge(otherState);
    }

    @OutputFunction("VARCHAR")
    public static void output(ListaggAggregationState state, BlockBuilder out)
    {
        if (state.isEmpty()) {
            out.appendNull();
        }
        else {
            outputState(state, (VariableWidthBlockBuilder) out, MAX_OUTPUT_LENGTH);
        }
    }

    @VisibleForTesting
    public static void outputState(ListaggAggregationState state, VariableWidthBlockBuilder blockBuilder, int maxOutputLength)
    {
        Slice separator = state.getSeparator();
        int separatorLength = separator.length();
        OutputContext context = new OutputContext();
        blockBuilder.buildEntry(out -> {
            state.forEach((block, position) -> {
                int entryLength = block.getSliceLength(position);
                int spaceRequired = entryLength + (context.emittedEntryCount > 0 ? separatorLength : 0);

                if (context.outputLength + spaceRequired > maxOutputLength) {
                    context.overflow = true;
                    return false;
                }

                if (context.emittedEntryCount > 0) {
                    out.writeBytes(separator, 0, separatorLength);
                    context.outputLength += separatorLength;
                }

                block.writeSliceTo(position, 0, entryLength, out);
                context.outputLength += entryLength;
                context.emittedEntryCount++;

                return true;
            });

            if (context.overflow) {
                if (state.isOverflowError()) {
                    throw new TrinoException(EXCEEDED_FUNCTION_MEMORY_LIMIT, format("Concatenated string has the length in bytes larger than the maximum output length %d", maxOutputLength));
                }

                if (context.emittedEntryCount > 0) {
                    out.writeBytes(separator, 0, separatorLength);
                }
                out.writeBytes(state.getOverflowFiller(), 0, state.getOverflowFiller().length());

                if (state.showOverflowEntryCount()) {
                    out.writeBytes(Slices.utf8Slice("("), 0, 1);
                    Slice count = Slices.utf8Slice(Integer.toString(state.getEntryCount() - context.emittedEntryCount));
                    out.writeBytes(count, 0, count.length());
                    out.writeBytes(Slices.utf8Slice(")"), 0, 1);
                }
            }
        });
    }

    private static class OutputContext
    {
        long outputLength;
        int emittedEntryCount;
        boolean overflow;
    }
}
