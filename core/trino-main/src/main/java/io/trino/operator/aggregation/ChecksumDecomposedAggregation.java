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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.operator.aggregation.state.NullableLongState;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.Decomposition;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;

// checksum hashes generic values into a bigint intermediate, so the functions merging that
// intermediate live in this separate class to keep their bigint input distinct from a checksum
// over a raw bigint column
@AggregationFunction
public final class ChecksumDecomposedAggregation
{
    private ChecksumDecomposedAggregation() {}

    @InputFunction
    public static void intermediateInput(@AggregationState NullableLongState state, @SqlType(StandardTypes.BIGINT) long value)
    {
        state.setNull(false);
        state.setValue(state.getValue() + value);
    }

    @AggregationFunction(value = "checksum$merge", hidden = true)
    @SqlNullable
    @OutputFunction(value = StandardTypes.BIGINT, decomposition = @Decomposition(partial = "checksum$merge", output = "checksum$merge"))
    public static void intermediateOutput(@AggregationState NullableLongState state, BlockBuilder out)
    {
        NullableLongState.write(BIGINT, state, out);
    }

    @AggregationFunction(value = "checksum$final", hidden = true)
    @SqlNullable
    @OutputFunction(value = "VARBINARY", decomposition = @Decomposition(partial = "checksum$merge", output = "checksum$final"))
    public static void output(@AggregationState NullableLongState state, BlockBuilder out)
    {
        if (state.isNull()) {
            out.appendNull();
        }
        else {
            Slice value = Slices.allocate(Long.BYTES);
            value.setLong(0, state.getValue());
            VARBINARY.writeSlice(out, value);
        }
    }
}
