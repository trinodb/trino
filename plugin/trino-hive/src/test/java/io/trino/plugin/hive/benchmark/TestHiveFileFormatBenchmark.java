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
package io.trino.plugin.hive.benchmark;

import io.trino.plugin.hive.HiveCompressionCodec;
import io.trino.plugin.hive.benchmark.BenchmarkHiveFileFormat.CompressionCounter;
import io.trino.plugin.hive.benchmark.BenchmarkHiveFileFormat.DataSet;
import org.testng.annotations.Test;

import java.io.IOException;

import static io.trino.plugin.hive.HiveCompressionCodec.SNAPPY;
import static io.trino.plugin.hive.benchmark.BenchmarkFileFormat.TRINO_ORC;
import static io.trino.plugin.hive.benchmark.BenchmarkFileFormat.TRINO_PARQUET;
import static io.trino.plugin.hive.benchmark.BenchmarkFileFormat.TRINO_RCBINARY;
import static io.trino.plugin.hive.benchmark.BenchmarkHiveFileFormat.DataSet.LARGE_MAP_VARCHAR_DOUBLE;
import static io.trino.plugin.hive.benchmark.BenchmarkHiveFileFormat.DataSet.LINEITEM;
import static io.trino.plugin.hive.benchmark.BenchmarkHiveFileFormat.DataSet.MAP_VARCHAR_DOUBLE;

public class TestHiveFileFormatBenchmark
{
    @Test
    public void testSomeFormats()
            throws Exception
    {
        executeBenchmark(LINEITEM, SNAPPY, TRINO_RCBINARY);
        executeBenchmark(LINEITEM, SNAPPY, TRINO_ORC);
        executeBenchmark(LINEITEM, SNAPPY, TRINO_PARQUET);
        executeBenchmark(MAP_VARCHAR_DOUBLE, SNAPPY, TRINO_RCBINARY);
        executeBenchmark(MAP_VARCHAR_DOUBLE, SNAPPY, TRINO_ORC);
        executeBenchmark(LARGE_MAP_VARCHAR_DOUBLE, SNAPPY, TRINO_RCBINARY);
        executeBenchmark(LARGE_MAP_VARCHAR_DOUBLE, SNAPPY, TRINO_ORC);
    }

    @Test
    public void testAllCompression()
            throws Exception
    {
        for (HiveCompressionCodec codec : HiveCompressionCodec.values()) {
            executeBenchmark(LINEITEM, codec, TRINO_RCBINARY);
        }
    }

    @Test
    public void testAllDataSets()
            throws Exception
    {
        for (DataSet dataSet : DataSet.values()) {
            executeBenchmark(dataSet, SNAPPY, TRINO_RCBINARY);
        }
    }

    private static void executeBenchmark(DataSet dataSet, HiveCompressionCodec codec, BenchmarkFileFormat format)
            throws IOException
    {
        BenchmarkHiveFileFormat benchmark = new BenchmarkHiveFileFormat(dataSet, codec, format);
        try {
            benchmark.setup();
            benchmark.read(new CompressionCounter());
            benchmark.write(new CompressionCounter());
        }
        catch (Exception e) {
            throw new RuntimeException("Failed " + dataSet + " " + codec + " " + format, e);
        }
        finally {
            benchmark.tearDown();
        }
    }
}
