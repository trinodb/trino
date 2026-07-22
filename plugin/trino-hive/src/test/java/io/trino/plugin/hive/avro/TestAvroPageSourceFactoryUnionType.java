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
package io.trino.plugin.hive.avro;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slices;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.memory.MemoryFileSystemFactory;
import io.trino.metastore.HiveType;
import io.trino.metastore.type.TypeInfo;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HiveConfig;
import io.trino.plugin.hive.Schema;
import io.trino.spi.block.Block;
import io.trino.spi.block.RowBlock;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.RowType;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static io.trino.hive.formats.HiveClassNames.AVRO_SERDE_CLASS;
import static io.trino.hive.formats.avro.AvroHiveConstants.SCHEMA_LITERAL;
import static io.trino.metastore.type.TypeInfoFactory.getPrimitiveTypeInfo;
import static io.trino.metastore.type.TypeInfoFactory.getUnionTypeInfo;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.trino.plugin.hive.HiveColumnHandle.createBaseColumn;
import static io.trino.plugin.hive.HiveTestUtils.getHiveSession;
import static io.trino.plugin.hive.acid.AcidTransaction.NO_ACID_TRANSACTION;
import static io.trino.spi.type.RowType.field;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end test for {@link AvroPageSourceFactory}'s handling of Hive {@code UNIONTYPE} columns.
 *
 * <p>A Hive {@code uniontype<string>} column is stored in Avro as the simple nullable union
 * {@code ["null", "string"]}, which the Avro reader would optimize to a scalar VARCHAR. Verifies that
 * {@code maskColumnsFromTableSchema} stamps the {@code HIVE_UNION_TYPE_SCHEMA_PROP} marker so the reader
 * produces a {@code RowBlock} matching the Hive metastore type {@code ROW(tag tinyint, field0 varchar)}.
 * Without the fix the reader returns a {@code VariableWidthBlock} and the column dereference projection
 * throws {@code Unexpected block type: VariableWidthBlock}.
 */
public class TestAvroPageSourceFactoryUnionType
{
    @Test
    public void testUnionTypeColumnIsMarkedAndProducesRowBlock()
            throws IOException
    {
        // Avro file schema: a record with a simple nullable union field (no marker — as a real Avro file would have)
        org.apache.avro.Schema fileSchema = SchemaBuilder.record("testRecord")
                .fields()
                .name("col").type().nullable().stringType().noDefault()
                .endRecord();

        MemoryFileSystemFactory fileSystemFactory = new MemoryFileSystemFactory();
        ConnectorSession session = getHiveSession(new HiveConfig());
        TrinoFileSystem fileSystem = fileSystemFactory.create(session);
        Location location = Location.of("memory:///testUnion.avro");
        try (DataFileWriter<GenericRecord> writer = new DataFileWriter<>(new GenericDatumWriter<>(fileSchema))) {
            writer.create(fileSchema, fileSystem.newOutputFile(location).create());
            writer.append(new GenericRecordBuilder(fileSchema).set("col", "hello").build());
        }
        long fileSize = fileSystem.newInputFile(location).length();

        // Hive UNIONTYPE<string> column → Trino ROW(tag tinyint, field0 varchar)
        HiveType hiveUnionType = HiveType.fromTypeInfo(getUnionTypeInfo(ImmutableList.<TypeInfo>of(getPrimitiveTypeInfo("string"))));
        RowType unionRowType = RowType.from(ImmutableList.of(field("tag", TINYINT), field("field0", VARCHAR)));
        HiveColumnHandle unionColumn = createBaseColumn("col", 0, hiveUnionType, unionRowType, REGULAR, Optional.empty());

        Schema schema = new Schema(AVRO_SERDE_CLASS, false, ImmutableMap.of(SCHEMA_LITERAL, fileSchema.toString()));

        AvroPageSourceFactory factory = new AvroPageSourceFactory(fileSystemFactory);
        ConnectorPageSource pageSource = factory.createPageSource(
                        session,
                        location,
                        0,
                        fileSize,
                        fileSize,
                        12345,
                        schema,
                        ImmutableList.of(unionColumn),
                        TupleDomain.all(),
                        Optional.empty(),
                        OptionalInt.empty(),
                        false,
                        NO_ACID_TRANSACTION)
                .orElseThrow();

        try (pageSource) {
            SourcePage page = null;
            while (!pageSource.isFinished() && page == null) {
                page = pageSource.getNextSourcePage();
            }
            assertThat(page).isNotNull();
            assertThat(page.getPositionCount()).isEqualTo(1);

            // maskColumnsFromTableSchema must have marked the union schema so the reader produces a RowBlock.
            List<Block> fields = RowBlock.getRowFieldsFromBlock(page.getBlock(0));
            assertThat(fields).hasSize(2); // tag + field0

            assertThat(page.getBlock(0).isNull(0)).isFalse();
            assertThat(TINYINT.getLong(fields.get(0), 0)).isEqualTo(0L); // tag = 0 (first non-null branch)
            assertThat(VARCHAR.getSlice(fields.get(1), 0)).isEqualTo(Slices.utf8Slice("hello"));
        }
    }
}
