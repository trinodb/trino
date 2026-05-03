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
package io.trino.plugin.base.util;

import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import org.junit.jupiter.api.Test;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.RunnerException;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ThreadLocalRandom;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.plugin.base.util.JsonTypeUtil.jsonParse;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;

@State(Scope.Thread)
@OutputTimeUnit(NANOSECONDS)
@BenchmarkMode(AverageTime)
@Fork(3)
@Warmup(iterations = 5, time = 500, timeUnit = MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = MILLISECONDS)
public class BenchmarkJsonTypeUtil
{
    @Benchmark
    public Slice benchmarkJsonParse(BenchmarkData data)
    {
        return jsonParse(data.getJsonSlice());
    }

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        @Param
        private JsonType jsonType;

        private Slice jsonSlice;

        @Setup
        public void setup()
        {
            jsonSlice = utf8Slice(switch (jsonType) {
                case SCALAR_STRING -> "\"test string value\"";
                case SCALAR_NUMBER -> "123456.789";
                case ARRAY_SMALL -> generateArray(10);
                case ARRAY_LARGE -> generateArray(100);
                case OBJECT_SMALL -> generateObject(10);
                case OBJECT_LARGE -> generateObject(100);
                case NESTED_SHALLOW -> generateNested(3, 5);
                case NESTED_DEEP -> generateNested(10, 3);
                case HIGHLY_NESTED_REAL_WORLD -> loadResourceFile("io/trino/plugin/base/util/highly-nested.json");
            });
        }

        private static String loadResourceFile(String resourcePath)
        {
            try (InputStream inputStream = BenchmarkJsonTypeUtil.class.getClassLoader().getResourceAsStream(resourcePath)) {
                if (inputStream == null) {
                    throw new IllegalArgumentException(format("Resource not found: %s", resourcePath));
                }
                return new String(inputStream.readAllBytes(), UTF_8);
            }
            catch (IOException e) {
                throw new RuntimeException(format("Failed to load resource: %s", resourcePath), e);
            }
        }

        private static String generateArray(int size)
        {
            try (SliceOutput output = new DynamicSliceOutput(size * 20)) {
                output.appendByte('[');
                for (int i = 0; i < size; i++) {
                    if (i > 0) {
                        output.appendByte(',');
                    }
                    output.appendBytes(generateRandomValue().getBytes(UTF_8));
                }
                output.appendByte(']');
                return output.slice().toStringUtf8();
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private static String generateObject(int size)
        {
            try (SliceOutput output = new DynamicSliceOutput(size * 30)) {
                output.appendByte('{');
                for (int i = 0; i < size; i++) {
                    if (i > 0) {
                        output.appendByte(',');
                    }
                    output.appendBytes(("\"key" + i + "\":").getBytes(UTF_8));
                    output.appendBytes(generateRandomValue().getBytes(UTF_8));
                }
                output.appendByte('}');
                return output.slice().toStringUtf8();
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private static String generateNested(int depth, int width)
        {
            if (depth == 0) {
                return generateRandomValue();
            }
            try (SliceOutput output = new DynamicSliceOutput(200)) {
                output.appendByte('{');
                for (int i = 0; i < width; i++) {
                    if (i > 0) {
                        output.appendByte(',');
                    }
                    output.appendBytes(("\"field" + i + "\":").getBytes(UTF_8));
                    if (i == 0) {
                        output.appendBytes(generateNested(depth - 1, width).getBytes(UTF_8));
                    }
                    else {
                        output.appendBytes(generateRandomValue().getBytes(UTF_8));
                    }
                }
                output.appendByte('}');
                return output.slice().toStringUtf8();
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private static String generateRandomValue()
        {
            return switch (ThreadLocalRandom.current().nextInt(3)) {
                case 0 -> String.valueOf(ThreadLocalRandom.current().nextLong());
                case 1 -> String.valueOf(ThreadLocalRandom.current().nextDouble());
                default -> "\"" + randomString(10) + "\"";
            };
        }

        private static String randomString(int length)
        {
            String characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
            StringBuilder builder = new StringBuilder(length);
            for (int i = 0; i < length; i++) {
                builder.append(characters.charAt(ThreadLocalRandom.current().nextInt(characters.length())));
            }
            return builder.toString();
        }

        public Slice getJsonSlice()
        {
            return jsonSlice;
        }
    }

    public enum JsonType
    {
        SCALAR_STRING,
        SCALAR_NUMBER,
        ARRAY_SMALL,
        ARRAY_LARGE,
        OBJECT_SMALL,
        OBJECT_LARGE,
        NESTED_SHALLOW,
        NESTED_DEEP,
        HIGHLY_NESTED_REAL_WORLD,
    }

    @Test
    public void verify()
    {
        BenchmarkData data = new BenchmarkData();
        for (JsonType type : JsonType.values()) {
            data.jsonType = type;
            data.setup();
            new BenchmarkJsonTypeUtil().benchmarkJsonParse(data);
        }
    }

    static void main()
            throws RunnerException
    {
        benchmark(BenchmarkJsonTypeUtil.class).run();
    }
}
