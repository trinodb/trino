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
package io.trino.hive.formats.avro;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.trino.filesystem.TrinoInputFile;
import io.trino.spi.Page;
import io.trino.spi.block.RowBlock;
import io.trino.spi.type.TimestampType;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.RandomData;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHiveAvroTypeBlockHandler
        extends TestAvroBase
{
    @Test
    public void testRead3UnionWith2UnionDataWith2Union()
            throws IOException, AvroTypeException
    {
        Schema twoUnion = Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.INT));
        Schema threeUnion = Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.INT), Schema.create(Schema.Type.STRING));

        Schema twoUnionRecord = SchemaBuilder.builder()
                .record("aRecord")
                .fields()
                .name("aField")
                .type(twoUnion)
                .noDefault()
                .endRecord();

        Schema threeUnionRecord = SchemaBuilder.builder()
                .record("aRecord")
                .fields()
                .name("aField")
                .type(threeUnion)
                .noDefault()
                .endRecord();

        // write a file with the 3 union schema, using 2 union data
        TrinoInputFile inputFile = createWrittenFileWithData(threeUnionRecord, ImmutableList.copyOf(Iterables.transform(new RandomData(twoUnionRecord, 1000), object -> (GenericRecord) object)));

        //read the file with the 2 union schema and ensure that no error thrown
        try (AvroFileReader avroFileReader = new AvroFileReader(inputFile, twoUnionRecord, new HiveAvroTypeBlockHandler(TimestampType.TIMESTAMP_MILLIS))) {
            while (avroFileReader.hasNext()) {
                assertThat(avroFileReader.next()).isNotNull();
            }
        }
    }

    @Test
    public void testCoercionOfUnionToStruct()
            throws IOException, AvroTypeException
    {
        Schema complexUnion = Schema.createUnion(Schema.create(Schema.Type.INT), Schema.create(Schema.Type.STRING), Schema.create(Schema.Type.NULL));

        Schema readSchema = SchemaBuilder.builder()
                .record("testComplexUnions")
                .fields()
                .name("readStraightUp")
                .type(complexUnion)
                .noDefault()
                .name("readFromReverse")
                .type(complexUnion)
                .noDefault()
                .name("readFromDefault")
                .type(complexUnion)
                .withDefault(42)
                .endRecord();

        Schema writeSchema = SchemaBuilder.builder()
                .record("testComplexUnions")
                .fields()
                .name("readStraightUp")
                .type(complexUnion)
                .noDefault()
                .name("readFromReverse")
                .type(Schema.createUnion(Schema.create(Schema.Type.STRING), Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.INT)))
                .noDefault()
                .endRecord();

        GenericRecord stringsOnly = new GenericData.Record(writeSchema);
        stringsOnly.put("readStraightUp", "I am in column 0 field 1");
        stringsOnly.put("readFromReverse", "I am in column 1 field 1");

        GenericRecord ints = new GenericData.Record(writeSchema);
        ints.put("readStraightUp", 5);
        ints.put("readFromReverse", 21);

        GenericRecord nulls = new GenericData.Record(writeSchema);
        nulls.put("readStraightUp", null);
        nulls.put("readFromReverse", null);

        TrinoInputFile input = createWrittenFileWithData(writeSchema, ImmutableList.of(stringsOnly, ints, nulls));
        try (AvroFileReader avroFileReader = new AvroFileReader(input, readSchema, new HiveAvroTypeBlockHandler(TimestampType.TIMESTAMP_MILLIS))) {
            int totalRecords = 0;
            while (avroFileReader.hasNext()) {
                Page p = avroFileReader.next();
                assertThat(p.getPositionCount()).withFailMessage("Page Batch should be at least 3").isEqualTo(3);
                //check first column
                //check first column first row coerced struct
                RowBlock readStraightUpStringsOnly = (RowBlock) p.getBlock(0).getSingleValueBlock(0);
                assertThat(readStraightUpStringsOnly.getFieldBlocks()).hasSize(3); // tag, int and string block fields
                assertThat(readStraightUpStringsOnly.getFieldBlocks().get(1).isNull(0)).isTrue(); // int field null
                assertThat(VARCHAR.getObjectValue(null, readStraightUpStringsOnly.getFieldBlocks().get(2), 0)).isEqualTo("I am in column 0 field 1"); //string field expected value
                // check first column second row coerced struct
                RowBlock readStraightUpInts = (RowBlock) p.getBlock(0).getSingleValueBlock(1);
                assertThat(readStraightUpInts.getFieldBlocks()).hasSize(3); // tag, int and string block fields
                assertThat(readStraightUpInts.getFieldBlocks().get(2).isNull(0)).isTrue(); // string field null
                assertThat(INTEGER.getObjectValue(null, readStraightUpInts.getFieldBlocks().get(1), 0)).isEqualTo(5);

                //check first column third row is null
                assertThat(p.getBlock(0).isNull(2)).isTrue();
                //check second column
                //check second column first row coerced struct
                RowBlock readFromReverseStringsOnly = (RowBlock) p.getBlock(1).getSingleValueBlock(0);
                assertThat(readFromReverseStringsOnly.getFieldBlocks()).hasSize(3); // tag, int and string block fields
                assertThat(readFromReverseStringsOnly.getFieldBlocks().get(1).isNull(0)).isTrue(); // int field null
                assertThat(VARCHAR.getObjectValue(null, readFromReverseStringsOnly.getFieldBlocks().get(2), 0)).isEqualTo("I am in column 1 field 1");
                //check second column second row coerced struct
                RowBlock readFromReverseUpInts = (RowBlock) p.getBlock(1).getSingleValueBlock(1);
                assertThat(readFromReverseUpInts.getFieldBlocks()).hasSize(3); // tag, int and string block fields
                assertThat(readFromReverseUpInts.getFieldBlocks().get(2).isNull(0)).isTrue(); // string field null
                assertThat(INTEGER.getObjectValue(null, readFromReverseUpInts.getFieldBlocks().get(1), 0)).isEqualTo(21);
                //check second column third row is null
                assertThat(p.getBlock(1).isNull(2)).isTrue();

                //check third column (default of 42 always)
                //check third column first row coerced struct
                RowBlock readFromDefaultStringsOnly = (RowBlock) p.getBlock(2).getSingleValueBlock(0);
                assertThat(readFromDefaultStringsOnly.getFieldBlocks()).hasSize(3); // tag, int and string block fields
                assertThat(readFromDefaultStringsOnly.getFieldBlocks().get(2).isNull(0)).isTrue(); // string field null
                assertThat(INTEGER.getObjectValue(null, readFromDefaultStringsOnly.getFieldBlocks().get(1), 0)).isEqualTo(42);
                //check third column second row coerced struct
                RowBlock readFromDefaultInts = (RowBlock) p.getBlock(2).getSingleValueBlock(1);
                assertThat(readFromDefaultInts.getFieldBlocks()).hasSize(3); // tag, int and string block fields
                assertThat(readFromDefaultInts.getFieldBlocks().get(2).isNull(0)).isTrue(); // string field null
                assertThat(INTEGER.getObjectValue(null, readFromDefaultInts.getFieldBlocks().get(1), 0)).isEqualTo(42);
                //check third column third row coerced struct
                RowBlock readFromDefaultNulls = (RowBlock) p.getBlock(2).getSingleValueBlock(2);
                assertThat(readFromDefaultNulls.getFieldBlocks()).hasSize(3); // int and string block fields
                assertThat(readFromDefaultNulls.getFieldBlocks().get(2).isNull(0)).isTrue(); // string field null
                assertThat(INTEGER.getObjectValue(null, readFromDefaultNulls.getFieldBlocks().get(1), 0)).isEqualTo(42);

                totalRecords += p.getPositionCount();
            }
            assertThat(totalRecords).isEqualTo(3);
        }
    }
}
