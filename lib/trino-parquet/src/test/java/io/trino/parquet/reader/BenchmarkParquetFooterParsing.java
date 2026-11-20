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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.parquet.ParquetDataSourceId;
import io.trino.parquet.metadata.ParquetMetadata;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.format.Statistics;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static org.apache.parquet.format.CompressionCodec.SNAPPY;
import static org.apache.parquet.format.Encoding.PLAIN;
import static org.apache.parquet.format.FieldRepetitionType.OPTIONAL;
import static org.apache.parquet.format.Type.INT64;
import static org.apache.parquet.format.Util.writeFileMetaData;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(MICROSECONDS)
public class BenchmarkParquetFooterParsing
{
    private static final ParquetDataSourceId DATA_SOURCE_ID = new ParquetDataSourceId("benchmark");

    @Param({"1", "128"})
    private int rowGroups;

    private Slice footer;

    @Setup
    public void setup()
            throws IOException
    {
        int columnCount = 105;
        List<SchemaElement> schema = new ArrayList<>();
        schema.add(new SchemaElement("root").setNum_children(columnCount));
        List<ColumnChunk> columns = new ArrayList<>();
        for (int i = 0; i < columnCount; i++) {
            String name = "column_" + i;
            schema.add(new SchemaElement(name).setType(INT64).setRepetition_type(OPTIONAL));
            ColumnMetaData metadata = new ColumnMetaData(INT64, List.of(PLAIN), List.of(name), SNAPPY, 1024, 8192, 4096, 4L + i * 4096L);
            metadata.setStatistics(new Statistics().setMin_value(new byte[8]).setMax_value(new byte[8]).setNull_count(0));
            columns.add(new ColumnChunk(0).setMeta_data(metadata));
        }
        RowGroup group = new RowGroup(columns, columnCount * 8192L, 1024);
        FileMetaData metadata = new FileMetaData(1, schema, rowGroups * 1024L, Collections.nCopies(rowGroups, group));
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        writeFileMetaData(metadata, output);
        footer = Slices.allocate(output.size() + 8);
        footer.setBytes(0, output.toByteArray());
        footer.setInt(output.size(), output.size());
        footer.setBytes(output.size() + Integer.BYTES, Slices.utf8Slice("PAR1"));
    }

    @Benchmark
    public ParquetMetadata parseFooter()
            throws IOException
    {
        return MetadataReader.parseFooter(DATA_SOURCE_ID, Integer.BYTES + footer.length(), footer, Optional.empty(), Optional.empty());
    }
}
