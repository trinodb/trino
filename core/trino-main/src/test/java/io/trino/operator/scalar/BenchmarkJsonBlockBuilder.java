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

import io.trino.json.Json;
import io.trino.json.JsonBlock;
import io.trino.json.JsonBlockBuilder;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.TimeUnit;

import static io.airlift.slice.Slices.utf8Slice;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(2)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class BenchmarkJsonBlockBuilder
{
    @Param({"32", "4096"})
    public int count;

    private JsonBlock source;
    private int[] positions;

    @Setup
    public void setup()
    {
        JsonBlockBuilder builder = new JsonBlockBuilder(null, count + 7);
        Json value = Json.unchecked(utf8Slice("{\"text\":\"abcdefghijklmnopqrstuvwxyz\",\"values\":[1,2,3]}"));
        for (int i = 0; i < count + 7; i++) {
            if (i % 17 == 0) {
                builder.appendNull();
            }
            else {
                builder.appendJson(value);
            }
        }
        source = builder.buildValueBlock().getRegion(7, count);
        positions = new int[count];
        for (int i = 0; i < count; i++) {
            positions[i] = count - i - 1;
        }
    }

    @Benchmark
    public JsonBlock appendRange()
    {
        JsonBlockBuilder builder = new JsonBlockBuilder(null, count);
        builder.appendRange(source, 0, count);
        return builder.buildValueBlock();
    }

    @Benchmark
    public JsonBlock appendRepeated()
    {
        JsonBlockBuilder builder = new JsonBlockBuilder(null, count);
        builder.appendRepeated(source, 1, count);
        return builder.buildValueBlock();
    }

    @Benchmark
    public JsonBlock appendPositions()
    {
        JsonBlockBuilder builder = new JsonBlockBuilder(null, count);
        builder.appendPositions(source, positions, 0, count);
        return builder.buildValueBlock();
    }
}
