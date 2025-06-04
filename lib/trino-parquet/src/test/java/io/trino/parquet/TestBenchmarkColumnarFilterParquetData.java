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
package io.trino.parquet;

import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

public class TestBenchmarkColumnarFilterParquetData
{
    @Test
    public void testBenchmark()
            throws Throwable
    {
        for (boolean columnarEvaluationEnabled : ImmutableList.of(true, false)) {
            for (BenchmarkColumnarFilterParquetData.FilterProvider filterProvider : BenchmarkColumnarFilterParquetData.FilterProvider.values()) {
                BenchmarkColumnarFilterParquetData benchmark = new BenchmarkColumnarFilterParquetData();
                benchmark.columnarEvaluationEnabled = columnarEvaluationEnabled;
                benchmark.filterProvider = filterProvider;
                benchmark.setup();
                benchmark.compiled();
            }
        }
    }
}
