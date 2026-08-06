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

import io.trino.operator.aggregation.state.LongAndNumberState;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RowBlock;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.BlockIndex;
import io.trino.spi.function.BlockPosition;
import io.trino.spi.function.Decomposition;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.TrinoNumber;

import java.math.BigDecimal;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.NumberType.NUMBER;
import static io.trino.type.NumberOperators.add;
import static io.trino.type.NumberOperators.divide;

// avg(number) intermediate is a (count, sum) pair, matching the generated LongAndNumberState
// serializer, which orders state fields alphabetically: (long, number)
@AggregationFunction
public final class NumberAverageDecomposedAggregation
{
    private NumberAverageDecomposedAggregation() {}

    @InputFunction
    public static void input(@AggregationState LongAndNumberState state, @SqlType(StandardTypes.NUMBER) TrinoNumber value)
    {
        NumberAverageAggregation.input(state, value);
    }

    @InputFunction(hidden = true)
    public static void intermediateInput(
            @AggregationState LongAndNumberState state,
            @BlockPosition @SqlType("row(count bigint, sum number)") ValueBlock block,
            @BlockIndex int position)
    {
        RowBlock rowBlock = (RowBlock) block;
        long count = BIGINT.getLong(rowBlock.getFieldBlock(0), position);
        if (count == 0) {
            return;
        }
        TrinoNumber sum = (TrinoNumber) NUMBER.getObject(rowBlock.getFieldBlock(1), position);
        if (state.getLong() == 0) {
            state.setLong(count);
            state.setNumber(sum);
        }
        else {
            state.setLong(state.getLong() + count);
            state.setNumber(add(state.getNumber(), sum));
        }
    }

    @AggregationFunction(value = "avg_number$intermediate", hidden = true)
    @OutputFunction(value = "row(count bigint, sum number)", decomposition = @Decomposition(partial = "avg_number$intermediate", output = "avg_number$intermediate"))
    public static void intermediateOutput(@AggregationState LongAndNumberState state, BlockBuilder out)
    {
        ((RowBlockBuilder) out).buildEntry(fieldBuilders -> {
            BIGINT.writeLong(fieldBuilders.get(0), state.getLong());
            if (state.getNumber() == null) {
                fieldBuilders.get(1).appendNull();
            }
            else {
                NUMBER.writeObject(fieldBuilders.get(1), state.getNumber());
            }
        });
    }

    @AggregationFunction(value = "avg_number$final", hidden = true)
    @SqlNullable
    @OutputFunction(value = StandardTypes.NUMBER, decomposition = @Decomposition(partial = "avg_number$intermediate", output = "avg_number$final"))
    public static void output(@AggregationState LongAndNumberState state, BlockBuilder out)
    {
        long count = state.getLong();
        if (count == 0) {
            out.appendNull();
        }
        else {
            TrinoNumber countAsNumber = TrinoNumber.from(BigDecimal.valueOf(count));
            NUMBER.writeObject(out, divide(state.getNumber(), countAsNumber));
        }
    }
}
