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
package io.trino.parquet.reader;

import org.testng.annotations.Test;

import java.io.IOException;

public class TestParquetReaderUtilsBenchmarks
{
    @Test
    public void testBenchmarkReadUleb128Int()
            throws IOException
    {
        BenchmarkReadUleb128Int benchmark = new BenchmarkReadUleb128Int(10000, Integer.MAX_VALUE);
        benchmark.setUp();
        benchmark.readUleb128Int();
    }

    @Test
    public void testBenchmarkReadUleb128Long()
            throws IOException
    {
        BenchmarkReadUleb128Long benchmark = new BenchmarkReadUleb128Long();
        benchmark.size = 10000;
        benchmark.setUp();
        benchmark.readUleb128Long();
    }

    @Test
    public void testBenchmarkReadFixedWidthInt()
            throws IOException
    {
        for (int byteWidth = 0; byteWidth <= 4; byteWidth++) {
            BenchmarkReadFixedWidthInt benchmark = new BenchmarkReadFixedWidthInt();
            benchmark.size = 1000;
            benchmark.byteWidth = byteWidth;
            benchmark.setUp();
            benchmark.readFixedWidthInt();
        }
    }
}
