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
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slices;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.spi.block.Block;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.Type;
import io.trino.spi.variant.Variant;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.parquet.ParquetTestUtils.createParquetReader;
import static io.trino.spi.type.VariantType.VARIANT;
import static io.trino.spi.variant.Metadata.EMPTY_METADATA;
import static java.lang.Math.toIntExact;
import static org.assertj.core.api.Assertions.assertThat;

final class TestShreddedVariantParquetReader
{
    private static final MessageType SHREDDED_SCHEMA = MessageTypeParser.parseMessageType(
            """
            message m {
              optional group v {
                required binary metadata;
                optional binary value;
                optional group typed_value {
                  required group a {
                    optional binary value;
                    optional int64 typed_value;
                  }
                  required group b {
                    optional binary value;
                    optional binary typed_value (STRING);
                  }
                }
              }
            }
            """);

    private static final MessageType SCALAR_SCHEMA = MessageTypeParser.parseMessageType(
            """
            message m {
              optional group v {
                required binary metadata;
                optional binary value;
                optional group typed_value {
                  required group i8 { optional binary value; optional int32 typed_value (INTEGER(8,true)); }
                  required group dt { optional binary value; optional int32 typed_value (DATE); }
                  required group fl { optional binary value; optional float typed_value; }
                  required group dec { optional binary value; optional int32 typed_value (DECIMAL(5,2)); }
                }
              }
            }
            """);

    @Test
    void testReadShreddedVariant(@TempDir java.nio.file.Path tempDir)
            throws IOException
    {
        // row 0: fully shredded object {a: 5, b: "hi"}
        SimpleGroup row0 = new SimpleGroup(SHREDDED_SCHEMA);
        Group v0 = row0.addGroup("v");
        v0.append("metadata", metadataBinary());
        Group typedValue0 = v0.addGroup("typed_value");
        typedValue0.addGroup("a").append("typed_value", 5L);
        typedValue0.addGroup("b").append("typed_value", Binary.fromString("hi"));

        // row 1: null variant (optional group absent)
        SimpleGroup row1 = new SimpleGroup(SHREDDED_SCHEMA);

        // row 2: partially shredded object: a from typed_value, c carried in value; b missing
        Variant partial = Variant.ofObject(ImmutableMap.of(utf8Slice("c"), Variant.ofLong(3)));
        SimpleGroup row2 = new SimpleGroup(SHREDDED_SCHEMA);
        Group v2 = row2.addGroup("v");
        v2.append("metadata", Binary.fromConstantByteArray(partial.metadata().toSlice().getBytes()));
        v2.append("value", Binary.fromConstantByteArray(partial.data().getBytes()));
        Group typedValue2 = v2.addGroup("typed_value");
        typedValue2.addGroup("a").append("typed_value", 1L);
        typedValue2.addGroup("b");

        List<Object> variants = writeAndReadVariants(SHREDDED_SCHEMA, tempDir, ImmutableList.of(row0, row1, row2));
        assertThat(variants).containsExactly(
                ImmutableMap.of("a", 5L, "b", "hi"),
                null,
                ImmutableMap.of("a", 1L, "c", 3L));
    }

    @Test
    void testReadShreddedScalarTypes(@TempDir java.nio.file.Path tempDir)
            throws IOException
    {
        SimpleGroup row = new SimpleGroup(SCALAR_SCHEMA);
        Group v = row.addGroup("v");
        v.append("metadata", metadataBinary());
        Group typedValue = v.addGroup("typed_value");
        typedValue.addGroup("i8").append("typed_value", 7);
        typedValue.addGroup("dt").append("typed_value", toIntExact(LocalDate.of(2021, 2, 3).toEpochDay()));
        typedValue.addGroup("fl").append("typed_value", 1.5f);
        typedValue.addGroup("dec").append("typed_value", 123); // unscaled value of DECIMAL(5,2) 1.23

        List<Object> variants = writeAndReadVariants(SCALAR_SCHEMA, tempDir, ImmutableList.of(row));
        assertThat(variants).containsExactly(ImmutableMap.of(
                "i8", (byte) 7,
                "dt", LocalDate.of(2021, 2, 3),
                "fl", 1.5f,
                "dec", new BigDecimal("1.23")));
    }

    private static List<Object> writeAndReadVariants(MessageType schema, java.nio.file.Path tempDir, List<SimpleGroup> rows)
            throws IOException
    {
        java.nio.file.Path file = tempDir.resolve("shredded.parquet");
        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new Path(file.toUri()))
                .withType(schema)
                .withConf(new Configuration())
                .build()) {
            for (SimpleGroup row : rows) {
                writer.write(row);
            }
        }

        List<Type> types = ImmutableList.of(VARIANT);
        List<String> columnNames = ImmutableList.of("v");
        ParquetDataSource dataSource = new TestingParquetDataSource(
                Slices.wrappedBuffer(Files.readAllBytes(file)),
                ParquetReaderOptions.defaultOptions());
        ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, Optional.empty());
        // ParquetReader uses an adaptive batch size that starts small, so rows may span pages
        List<Object> variants = new ArrayList<>();
        try (ParquetReader reader = createParquetReader(dataSource, parquetMetadata, newSimpleAggregatedMemoryContext(), types, columnNames)) {
            SourcePage page;
            while ((page = reader.nextPage()) != null) {
                Block block = page.getBlock(0);
                for (int position = 0; position < page.getPositionCount(); position++) {
                    variants.add(block.isNull(position) ? null : variantAt(block, position));
                }
            }
        }
        return variants;
    }

    private static Object variantAt(Block block, int position)
    {
        return ((Variant) VARIANT.getObject(block, position)).toObject();
    }

    private static Binary metadataBinary()
    {
        return Binary.fromConstantByteArray(EMPTY_METADATA.toSlice().getBytes());
    }
}
