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
package io.trino.parquet.reader.decoders;

import org.testng.annotations.Test;

import java.io.IOException;

public class TestRleBitPackingDecoderBenchmark
{
    @Test
    public void testRleBitPackingDecoderBenchmark()
            throws IOException
    {
        for (int bitWidth = 1; bitWidth <= 20; bitWidth++) {
            for (BenchmarkRleBitPackingDecoder.DataSet dataSet : BenchmarkRleBitPackingDecoder.DataSet.values()) {
                BenchmarkRleBitPackingDecoder benchmark = new BenchmarkRleBitPackingDecoder();
                benchmark.bitWidth = bitWidth;
                benchmark.dataSet = dataSet;
                benchmark.setup();
                benchmark.apacheRunLengthBitPackingHybridDecoder();
                benchmark.rleBitPackingHybridDecoder();
            }
        }
    }
}
