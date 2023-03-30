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
package io.trino.parquet.reader.flat;

import io.trino.parquet.reader.flat.BenchmarkFlatDefinitionLevelDecoder.DataGenerator;
import org.testng.annotations.Test;

import java.io.IOException;

public class TestNullsDecoderBenchmark
{
    @Test
    public void testDataGenerators()
            throws IOException
    {
        for (DataGenerator generator : DataGenerator.values()) {
            BenchmarkFlatDefinitionLevelDecoder benchmark = new BenchmarkFlatDefinitionLevelDecoder(10000, generator);
            benchmark.setup();
            benchmark.read();
        }
    }
}
