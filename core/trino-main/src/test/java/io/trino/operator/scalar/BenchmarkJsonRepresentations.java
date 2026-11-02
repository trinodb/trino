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

import io.airlift.slice.Slice;
import io.trino.jmh.Benchmarks;
import io.trino.json.Json;
import io.trino.json.JsonItems;
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
public class BenchmarkJsonRepresentations
{
    @Param({"VARCHAR", "RAW_JSON", "TYPED_JSON"})
    public String representation;

    @Param({"8", "128"})
    public int width;

    private Slice text;
    private Json json;
    private JsonPath scalarPath;
    private JsonPath objectPath;

    @Setup
    public void setup()
    {
        StringBuilder document = new StringBuilder("{\"values\":[");
        for (int i = 0; i < width; i++) {
            if (i > 0) {
                document.append(',');
            }
            document.append("{\"value\":").append(i).append(",\"padding\":\"abcdefghijklmnopqrstuv\"}");
        }
        document.append("]}");
        text = utf8Slice(document.toString());
        json = representation.equals("TYPED_JSON") ? JsonItems.fromText(text) : Json.unchecked(text);
        scalarPath = new JsonPath("$.values[0].value");
        objectPath = new JsonPath("$.values[0]");
    }

    @Benchmark
    public Slice extractScalar()
    {
        return representation.equals("VARCHAR")
                ? JsonFunctions.varcharJsonExtractScalar(text, scalarPath)
                : JsonFunctions.jsonExtractScalar(json, scalarPath);
    }

    @Benchmark
    public Json extractObject()
    {
        return representation.equals("VARCHAR")
                ? JsonFunctions.varcharJsonExtract(text, objectPath)
                : JsonFunctions.jsonExtract(json, objectPath);
    }

    @Benchmark
    public Long size()
    {
        return representation.equals("VARCHAR")
                ? JsonFunctions.varcharJsonSize(text, objectPath)
                : JsonFunctions.jsonSize(json, objectPath);
    }

    public static void main(String[] args)
            throws Exception
    {
        Benchmarks.benchmark(BenchmarkJsonRepresentations.class).run();
    }
}
