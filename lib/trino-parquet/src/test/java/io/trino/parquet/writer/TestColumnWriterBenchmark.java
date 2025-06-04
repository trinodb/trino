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
package io.trino.parquet.writer;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import java.io.IOException;

final class TestColumnWriterBenchmark
{
    @Test
    void testLongColumnWriterBenchmark()
            throws IOException
    {
        for (int bitWidth = 1; bitWidth <= 64; bitWidth += 4) {
            for (AbstractColumnWriterBenchmark.BloomFilterType bloomFilterType : AbstractColumnWriterBenchmark.BloomFilterType.values()) {
                for (int maxDictionaryPageSize : ImmutableList.of(1, 1048576)) {
                    BenchmarkLongColumnWriter benchmark = new BenchmarkLongColumnWriter();
                    benchmark.bitWidth = bitWidth;
                    benchmark.bloomFilterType = bloomFilterType;
                    benchmark.maxDictionaryPageSize = maxDictionaryPageSize;
                    benchmark.setup();
                    benchmark.write();
                }
            }
        }
    }

    @Test
    void testBinaryColumnWriterBenchmark()
            throws IOException
    {
        for (BenchmarkBinaryColumnWriter.FieldType fieldType : BenchmarkBinaryColumnWriter.FieldType.values()) {
            for (BenchmarkBinaryColumnWriter.PositionLength positionLength : BenchmarkBinaryColumnWriter.PositionLength.values()) {
                for (AbstractColumnWriterBenchmark.BloomFilterType bloomFilterType : AbstractColumnWriterBenchmark.BloomFilterType.values()) {
                    for (int maxDictionaryPageSize : ImmutableList.of(1, 1048576)) {
                        BenchmarkBinaryColumnWriter benchmark = new BenchmarkBinaryColumnWriter();
                        benchmark.type = fieldType;
                        benchmark.positionLength = positionLength;
                        benchmark.bloomFilterType = bloomFilterType;
                        benchmark.maxDictionaryPageSize = maxDictionaryPageSize;
                        benchmark.setup();
                        benchmark.write();
                    }
                }
            }
        }
    }
}
