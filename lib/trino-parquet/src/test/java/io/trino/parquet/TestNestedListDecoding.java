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

import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.parquet.reader.FileParquetDataSource;
import io.trino.parquet.reader.MetadataReader;
import io.trino.parquet.reader.ParquetReader;
import io.trino.spi.block.Block;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static io.trino.parquet.ParquetTestUtils.createParquetReader;
import static io.trino.spi.type.RowType.field;
import static io.trino.spi.type.RowType.rowType;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.nio.file.Files.createTempFile;
import static java.util.Collections.singletonList;
import static org.apache.parquet.hadoop.ParquetFileWriter.Mode.OVERWRITE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

/**
 * End-to-end decode coverage for the nested legacy LIST shape from
 * <a href="https://github.com/trinodb/trino/issues/27766">#27766</a>. Unlike
 * {@link TestParquetTypeUtils}, which stops at {@code constructField}, these tests write a real
 * file with the ambiguous middle {@code array} group and read it back through the full reader so
 * the definition/repetition-level handling of the unwrapped shape is exercised.
 */
// ExampleParquetWriter is not thread-safe
@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
public final class TestNestedListDecoding
{
    // The #27766 shape: a LIST whose middle repeated group is named "array" and whose single
    // child is itself a LIST group. The same bytes decode as array(array(varchar)) or as
    // array(row(inner_list)) depending on the caller's requested Trino type.
    private static final MessageType SCHEMA = MessageTypeParser.parseMessageType(
            """
            message schema {
              optional group nested_list (LIST) {
                repeated group array {
                  optional group inner_list (LIST) {
                    repeated group array {
                      optional binary element (STRING);
                    }
                  }
                }
              }
            }
            """);

    @Test
    public void testReadAsNestedArray()
            throws IOException
    {
        File file = writeSampleFile();
        Type type = new ArrayType(new ArrayType(VARCHAR));

        assertThat(readColumn(file, type)).containsExactly(
                List.of(List.of("a", "b"), List.of("c")), // [["a", "b"], ["c"]]
                singletonList(null),                       // [null] (null inner list)
                List.of(List.of()),                        // [[]] (empty inner list)
                List.of(),                                 // [] (empty outer list)
                null);                                     // null (absent outer list)
    }

    @Test
    public void testReadAsArrayOfStructPreservesLegacyInterpretation()
            throws IOException
    {
        // The same file, read as array(row(inner_list array(varchar))): the middle "array" group
        // is resolved as a single-field struct (the pre-#27766 legacy interpretation), so every
        // outer element is wrapped in a row.
        File file = writeSampleFile();
        Type type = new ArrayType(rowType(field("inner_list", new ArrayType(VARCHAR))));

        assertThat(readColumn(file, type)).containsExactly(
                List.of(List.of(List.of("a", "b")), List.of(List.of("c"))), // [row(["a", "b"]), row(["c"])]
                List.of(singletonList(null)),                               // [row(null)]
                List.of(List.of(List.of())),                                // [row([])]
                List.of(),                                                  // []
                null);                                                      // null
    }

    private static File writeSampleFile()
            throws IOException
    {
        File file = createTempFile("nested-list", ".parquet").toFile();
        file.deleteOnExit();

        ExampleParquetWriter.Builder builder = ExampleParquetWriter.builder(new Path(file.getAbsolutePath()))
                .withType(SCHEMA)
                .withConf(new Configuration())
                .withWriteMode(OVERWRITE);

        try (ParquetWriter<Group> writer = builder.build()) {
            SimpleGroupFactory factory = new SimpleGroupFactory(SCHEMA);

            // [["a", "b"], ["c"]]
            Group row0 = factory.newGroup();
            Group outer0 = row0.addGroup("nested_list");
            Group inner00 = outer0.addGroup("array").addGroup("inner_list");
            inner00.addGroup("array").append("element", "a");
            inner00.addGroup("array").append("element", "b");
            outer0.addGroup("array").addGroup("inner_list").addGroup("array").append("element", "c");
            writer.write(row0);

            // [null] — outer element present, inner_list absent (null)
            Group row1 = factory.newGroup();
            row1.addGroup("nested_list").addGroup("array");
            writer.write(row1);

            // [[]] — outer element present, inner_list present but empty
            Group row2 = factory.newGroup();
            row2.addGroup("nested_list").addGroup("array").addGroup("inner_list");
            writer.write(row2);

            // [] — outer list present but empty
            Group row3 = factory.newGroup();
            row3.addGroup("nested_list");
            writer.write(row3);

            // null — outer list absent
            writer.write(factory.newGroup());
        }
        return file;
    }

    private static List<Object> readColumn(File file, Type type)
            throws IOException
    {
        try (FileParquetDataSource source = new FileParquetDataSource(file, ParquetReaderOptions.defaultOptions())) {
            ParquetMetadata metadata = MetadataReader.readFooter(source, ParquetReaderOptions.defaultOptions(), Optional.empty(), Optional.empty());
            try (ParquetReader reader = createParquetReader(source, metadata, List.of(type), List.of("nested_list"))) {
                List<Object> values = new ArrayList<>();
                SourcePage page;
                while ((page = reader.nextPage()) != null) {
                    Block block = page.getBlock(0);
                    for (int position = 0; position < block.getPositionCount(); position++) {
                        values.add(type.getObjectValue(block, position));
                    }
                }
                return values;
            }
        }
    }
}
