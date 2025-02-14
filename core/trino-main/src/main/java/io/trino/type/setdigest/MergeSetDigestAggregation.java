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

package io.trino.type.setdigest;

import io.airlift.slice.Slice;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

import static io.trino.type.setdigest.SetDigestType.SET_DIGEST;

@AggregationFunction("merge_set_digest")
public final class MergeSetDigestAggregation
{
    private MergeSetDigestAggregation() {}

    @InputFunction
    public static void input(SetDigestState state, @SqlType(StandardTypes.SET_DIGEST) Slice value)
    {
        SetDigest instance = SetDigest.newInstance(value);
        merge(state, instance);
    }

    @CombineFunction
    public static void combine(SetDigestState state, SetDigestState otherState)
    {
        merge(state, otherState.getDigest());
    }

    private static void merge(SetDigestState state, SetDigest instance)
    {
        if (state.getDigest() == null) {
            state.setDigest(instance);
        }
        else {
            state.getDigest().mergeWith(instance);
        }
    }

    @OutputFunction(StandardTypes.SET_DIGEST)
    public static void output(SetDigestState state, BlockBuilder out)
    {
        if (state.getDigest() == null) {
            out.appendNull();
        }
        else {
            SET_DIGEST.writeSlice(out, state.getDigest().serialize());
        }
    }
}
