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

import com.google.common.collect.ImmutableList;
import io.trino.parquet.ParquetEncoding;
import org.testng.annotations.Test;

import java.io.IOException;

import static io.trino.parquet.ParquetEncoding.DELTA_BINARY_PACKED;
import static io.trino.parquet.ParquetEncoding.DELTA_BYTE_ARRAY;
import static io.trino.parquet.ParquetEncoding.PLAIN;
import static io.trino.parquet.ParquetEncoding.RLE;

public class TestColumnReaderBenchmark
{
    @Test
    public void testBooleanColumnReaderBenchmark()
            throws IOException
    {
        for (ParquetEncoding encoding : ImmutableList.of(PLAIN, RLE)) {
            BenchmarkBooleanColumnReader benchmark = new BenchmarkBooleanColumnReader();
            benchmark.encoding = encoding;
            benchmark.setup();
            benchmark.read();
        }
    }

    @Test
    public void testByteColumnReaderBenchmark()
            throws IOException
    {
        for (int bitWidth = 0; bitWidth <= 8; bitWidth++) {
            for (ParquetEncoding encoding : ImmutableList.of(PLAIN, DELTA_BINARY_PACKED)) {
                BenchmarkByteColumnReader benchmark = new BenchmarkByteColumnReader();
                benchmark.bitWidth = bitWidth;
                benchmark.encoding = encoding;
                benchmark.setup();
                benchmark.read();
            }
        }
    }

    @Test
    public void testShortColumnReaderBenchmark()
            throws IOException
    {
        for (int bitWidth = 0; bitWidth <= 16; bitWidth++) {
            for (ParquetEncoding encoding : ImmutableList.of(PLAIN, DELTA_BINARY_PACKED)) {
                BenchmarkShortColumnReader benchmark = new BenchmarkShortColumnReader();
                benchmark.bitWidth = bitWidth;
                benchmark.encoding = encoding;
                benchmark.setup();
                benchmark.read();
            }
        }
    }

    @Test
    public void testIntColumnReaderBenchmark()
            throws IOException
    {
        for (int bitWidth = 0; bitWidth <= 32; bitWidth++) {
            for (ParquetEncoding encoding : ImmutableList.of(PLAIN, DELTA_BINARY_PACKED)) {
                BenchmarkIntColumnReader benchmark = new BenchmarkIntColumnReader();
                benchmark.bitWidth = bitWidth;
                benchmark.encoding = encoding;
                benchmark.setup();
                benchmark.read();
            }
        }
    }

    @Test
    public void testInt32ToLongColumnReaderBenchmark()
            throws IOException
    {
        for (ParquetEncoding encoding : ImmutableList.of(PLAIN, DELTA_BINARY_PACKED)) {
            BenchmarkInt32ToLongColumnReader benchmark = new BenchmarkInt32ToLongColumnReader();
            benchmark.encoding = encoding;
            benchmark.setup();
            benchmark.read();
        }
    }

    @Test
    public void testLongColumnReaderBenchmark()
            throws IOException
    {
        for (int bitWidth = 0; bitWidth <= 64; bitWidth++) {
            for (ParquetEncoding encoding : ImmutableList.of(PLAIN, DELTA_BINARY_PACKED)) {
                BenchmarkLongColumnReader benchmark = new BenchmarkLongColumnReader();
                benchmark.bitWidth = bitWidth;
                benchmark.encoding = encoding;
                benchmark.setup();
                benchmark.read();
            }
        }
    }

    @Test
    public void testLongDecimalColumnReaderBenchmark()
            throws IOException
    {
        for (ParquetEncoding encoding : ImmutableList.of(PLAIN, DELTA_BYTE_ARRAY)) {
            BenchmarkLongDecimalColumnReader benchmark = new BenchmarkLongDecimalColumnReader();
            benchmark.encoding = encoding;
            benchmark.setup();
            benchmark.read();
        }
    }

    @Test
    public void testUuidColumnReaderBenchmark()
            throws IOException
    {
        for (ParquetEncoding encoding : ImmutableList.of(PLAIN, DELTA_BYTE_ARRAY)) {
            BenchmarkUuidColumnReader benchmark = new BenchmarkUuidColumnReader();
            benchmark.encoding = encoding;
            benchmark.setup();
            benchmark.read();
        }
    }

    @Test
    public void testInt96ColumnReaderBenchmark()
            throws IOException
    {
        BenchmarkInt96ColumnReader benchmark = new BenchmarkInt96ColumnReader();
        benchmark.setup();
        benchmark.read();
    }
}
