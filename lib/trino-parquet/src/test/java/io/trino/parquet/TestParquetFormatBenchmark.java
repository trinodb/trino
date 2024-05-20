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

import org.apache.parquet.format.CompressionCodec;
import org.testng.annotations.Test;

import java.io.IOException;

import static io.trino.parquet.BenchmarkParquetFormat.CompressionCounter;
import static io.trino.parquet.BenchmarkParquetFormat.DataSet;

public class TestParquetFormatBenchmark
{
    @Test
    public void testAllDatasets()
            throws Exception
    {
        for (DataSet dataSet : DataSet.values()) {
            executeBenchmark(dataSet);
        }
    }

    private static void executeBenchmark(DataSet dataSet)
            throws IOException
    {
        BenchmarkParquetFormat benchmark = new BenchmarkParquetFormat();
        try {
            benchmark.dataSet = dataSet;
            benchmark.compression = CompressionCodec.SNAPPY;
            benchmark.setup();
            benchmark.write(new CompressionCounter());
        }
        catch (Exception e) {
            throw new RuntimeException("Failed " + dataSet, e);
        }
        finally {
            benchmark.tearDown();
        }
    }
}
