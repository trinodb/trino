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
import io.airlift.slice.Slices;
import io.trino.jmh.Benchmarks;
import org.junit.jupiter.api.Test;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.RunnerException;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@BenchmarkMode(Mode.AverageTime)
@Fork(2)
@Warmup(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
public class BenchmarkBuiltInConstantSpecializations
{
    @Benchmark
    public Slice wordStemDynamic(BenchmarkData data)
    {
        return WordStemFunction.wordStem(data.word, data.language);
    }

    @Benchmark
    public Slice wordStemSpecialized(BenchmarkData data)
    {
        return data.wordStem.wordStem(data.word);
    }

    @Benchmark
    public Slice translateDynamic(BenchmarkData data)
    {
        return StringFunctions.translate(data.source, data.from, data.to);
    }

    @Benchmark
    public Slice translateSpecialized(BenchmarkData data)
    {
        return data.translate.translate(data.source);
    }

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        private final Slice word = Slices.utf8Slice("intensifying");
        private final Slice language = Slices.utf8Slice("en");
        private final WordStemFunction.WithLanguage.ConstantLanguage wordStem = new WordStemFunction.WithLanguage.ConstantLanguage(language);

        private final Slice source = Slices.utf8Slice("V\u00e1rzea Paulista");
        private final Slice from = Slices.utf8Slice("\u00e1\u00e9\u00ed\u00f3\u00fa\u00c1\u00c9\u00cd\u00d3\u00da\u00e4\u00eb\u00ef\u00f6\u00fc\u00c4\u00cb\u00cf\u00d6\u00dc\u00e2\u00ea\u00ee\u00f4\u00fb\u00c2\u00ca\u00ce\u00d4\u00db\u00e3\u1ebd\u0129\u00f5\u0169\u00c3\u1ebc\u0128\u00d5\u0168");
        private final Slice to = Slices.utf8Slice("aeiouAEIOUaeiouAEIOUaeiouAEIOUaeiouAEIOU");
        private final StringFunctions.Translate.ConstantTranslation translate = new StringFunctions.Translate.ConstantTranslation(from, to);
    }

    @Test
    public void verify()
    {
        BenchmarkData data = new BenchmarkData();
        assertThat(wordStemSpecialized(data)).isEqualTo(wordStemDynamic(data));
        assertThat(translateSpecialized(data)).isEqualTo(translateDynamic(data));
    }

    static void main()
            throws RunnerException
    {
        Benchmarks.benchmark(BenchmarkBuiltInConstantSpecializations.class).run();
    }
}
