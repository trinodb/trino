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
package io.trino.plugin.kafka.schema.confluent;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TestingTypeManager;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecordBuilder;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.plugin.kafka.schema.confluent.AvroSchemaConverter.DUMMY_FIELD_NAME;
import static io.trino.plugin.kafka.schema.confluent.AvroSchemaConverter.EmptyFieldStrategy.FAIL;
import static io.trino.plugin.kafka.schema.confluent.AvroSchemaConverter.EmptyFieldStrategy.IGNORE;
import static io.trino.plugin.kafka.schema.confluent.AvroSchemaConverter.EmptyFieldStrategy.MARK;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

public class TestAvroSchemaConverter
{
    private static final String RECORD_NAME = "test";

    private static final TypeManager TYPE_MANAGER = new TestingTypeManager();

    @Test
    public void testConvertSchema()
    {
        Schema schema = SchemaBuilder.record(RECORD_NAME)
                .fields()
                .name("bool_col").type().booleanType().noDefault()
                .name("int_col").type().intType().noDefault()
                .name("long_col").type().longType().noDefault()
                .name("float_col").type().floatType().noDefault()
                .name("double_col").type().doubleType().noDefault()
                .name("string_col").type().stringType().noDefault()
                .name("enum_col").type().enumeration("colors").symbols("blue", "red", "yellow").noDefault()
                .name("bytes_col").type().bytesType().noDefault()
                .name("fixed_col").type().fixed("fixed").size(5).noDefault()
                .name("union_col").type().unionOf().nullType().and().floatType().and().doubleType().endUnion().noDefault()
                .name("union_col2").type().unionOf().nullType().and().intType().and().longType().endUnion().noDefault()
                .name("union_col3").type().unionOf().nullType().and().bytesType().and().type("fixed").endUnion().noDefault()
                .name("union_col4").type().unionOf().nullType().and().type("colors").and().stringType().endUnion().noDefault()
                .name("list_col").type().array().items().intType().noDefault()
                .name("map_col").type().map().values().intType().noDefault()
                .name("record_col").type().record("record_col")
                .fields()
                .name("nested_list").type().array().items().map().values().stringType().noDefault()
                .name("nested_map").type().map().values().array().items().stringType().noDefault()
                .endRecord()
                .noDefault()
                .endRecord();
        AvroSchemaConverter avroSchemaConverter = new AvroSchemaConverter(new TestingTypeManager(), IGNORE);
        List<Type> types = avroSchemaConverter.convertAvroSchema(schema);
        List<Type> expected = ImmutableList.<Type>builder()
                .add(BOOLEAN)
                .add(INTEGER)
                .add(BIGINT)
                .add(REAL)
                .add(DOUBLE)
                .add(VARCHAR)
                .add(VARCHAR)
                .add(VARBINARY)
                .add(VARBINARY)
                .add(DOUBLE)
                .add(BIGINT)
                .add(VARBINARY)
                .add(VARCHAR)
                .add(new ArrayType(INTEGER))
                .add(createType(INTEGER))
                .add(RowType.from(ImmutableList.<RowType.Field>builder()
                        .add(new RowType.Field(Optional.of("nested_list"), new ArrayType(createType(VARCHAR))))
                        .add(new RowType.Field(Optional.of("nested_map"), createType(new ArrayType(VARCHAR))))
                        .build()))
                .build();
        assertEquals(types, expected);
    }

    @Test
    public void testTypesWithDefaults()
    {
        Schema schema = SchemaBuilder.record(RECORD_NAME)
                .fields()
                .name("bool_col").type().booleanType().booleanDefault(true)
                .name("int_col").type().intType().intDefault(3)
                .name("long_col").type().longType().longDefault(3L)
                .name("float_col").type().floatType().floatDefault(3.3F)
                .name("double_col").type().doubleType().doubleDefault(3.3D)
                .name("string_col").type().stringType().stringDefault("three")
                .name("enum_col").type().enumeration("colors").symbols("blue", "red", "yellow").enumDefault("yellow")
                .name("bytes_col").type().bytesType().bytesDefault(new byte[] {1, 2, 3})
                .name("fixed_col").type().fixed("fixed").size(5).fixedDefault(new byte[] {1, 2, 3})
                .name("union_col").type().unionOf().nullType().and().floatType().and().doubleType().endUnion().nullDefault()
                .name("union_col2").type().unionOf().nullType().and().intType().and().longType().endUnion().nullDefault()
                .name("union_col3").type().unionOf().nullType().and().bytesType().and().type("fixed").endUnion().nullDefault()
                .name("union_col4").type().unionOf().nullType().and().type("colors").and().stringType().endUnion().nullDefault()
                .name("list_col").type().array().items().intType().arrayDefault(Arrays.asList(1, 2, 3))
                .name("map_col").type().map().values().intType().mapDefault(ImmutableMap.<String, Integer>builder()
                        .put("one", 1)
                        .put("two", 2)
                        .buildOrThrow())
                .name("record_col").type().record("record_col")
                .fields()
                .name("nested_list").type().array().items().map().values().stringType().noDefault()
                .name("nested_map").type().map().values().array().items().stringType().noDefault()
                .endRecord()
                .recordDefault(new GenericRecordBuilder(SchemaBuilder.record("record_col").fields()
                        .name("nested_list").type().array().items().map().values().stringType().noDefault()
                        .name("nested_map").type().map().values().array().items().stringType().noDefault()
                        .endRecord())
                        .set("nested_list", Arrays.asList(
                                ImmutableMap.<String, String>builder()
                                        .put("key", "value")
                                        .put("key1", "value1")
                                        .buildOrThrow(),
                                ImmutableMap.<String, String>builder()
                                        .put("key2", "value2")
                                        .put("key3", "value3")
                                        .buildOrThrow()))
                        .set("nested_map", ImmutableMap.<String, List<String>>builder()
                                .put("key1", Arrays.asList("one", "two", "three"))
                                .put("key2", Arrays.asList("four", "two", "three"))
                                .buildOrThrow())
                        .build())
                .endRecord();
        AvroSchemaConverter avroSchemaConverter = new AvroSchemaConverter(new TestingTypeManager(), IGNORE);
        List<Type> types = avroSchemaConverter.convertAvroSchema(schema);
        List<Type> expected = ImmutableList.<Type>builder()
                .add(BOOLEAN)
                .add(INTEGER)
                .add(BIGINT)
                .add(REAL)
                .add(DOUBLE)
                .add(VARCHAR)
                .add(VARCHAR)
                .add(VARBINARY)
                .add(VARBINARY)
                .add(DOUBLE)
                .add(BIGINT)
                .add(VARBINARY)
                .add(VARCHAR)
                .add(new ArrayType(INTEGER))
                .add(createType(INTEGER))
                .add(RowType.from(ImmutableList.<RowType.Field>builder()
                        .add(new RowType.Field(Optional.of("nested_list"), new ArrayType(createType(VARCHAR))))
                        .add(new RowType.Field(Optional.of("nested_map"), createType(new ArrayType(VARCHAR))))
                        .build()))
                .build();
        assertEquals(types, expected);
    }

    @Test
    public void testNullableColumns()
    {
        Schema schema = SchemaBuilder.record(RECORD_NAME)
                .fields()
                .name("bool_col").type().nullable().booleanType().noDefault()
                .name("int_col").type().nullable().intType().noDefault()
                .name("long_col").type().nullable().longType().noDefault()
                .name("float_col").type().nullable().floatType().noDefault()
                .name("double_col").type().nullable().doubleType().noDefault()
                .name("string_col").type().nullable().stringType().noDefault()
                .name("enum_col").type().nullable().enumeration("colors").symbols("blue", "red", "yellow").noDefault()
                .name("bytes_col").type().nullable().bytesType().noDefault()
                .name("fixed_col").type().nullable().fixed("fixed").size(5).noDefault()
                .name("union_col").type().unionOf().nullType().and().floatType().and().doubleType().endUnion().noDefault()
                .name("union_col2").type().unionOf().nullType().and().intType().and().longType().endUnion().noDefault()
                .name("union_col3").type().unionOf().nullType().and().bytesType().and().type("fixed").endUnion().noDefault()
                .name("union_col4").type().unionOf().nullType().and().type("colors").and().stringType().endUnion().noDefault()
                .name("list_col").type().nullable().array().items().intType().noDefault()
                .name("map_col").type().nullable().map().values().intType().noDefault()
                .name("record_col").type().nullable().record("record_col")
                .fields()
                .name("nested_list").type().nullable().array().items().map().values().stringType().noDefault()
                .name("nested_map").type().nullable().map().values().array().items().stringType().noDefault()
                .endRecord()
                .noDefault()
                .endRecord();
        AvroSchemaConverter avroSchemaConverter = new AvroSchemaConverter(new TestingTypeManager(), IGNORE);
        List<Type> types = avroSchemaConverter.convertAvroSchema(schema);
        List<Type> expected = ImmutableList.<Type>builder()
                .add(BOOLEAN)
                .add(INTEGER)
                .add(BIGINT)
                .add(REAL)
                .add(DOUBLE)
                .add(VARCHAR)
                .add(VARCHAR)
                .add(VARBINARY)
                .add(VARBINARY)
                .add(DOUBLE)
                .add(BIGINT)
                .add(VARBINARY)
                .add(VARCHAR)
                .add(new ArrayType(INTEGER))
                .add(createType(INTEGER))
                .add(RowType.from(ImmutableList.<RowType.Field>builder()
                        .add(new RowType.Field(Optional.of("nested_list"), new ArrayType(createType(VARCHAR))))
                        .add(new RowType.Field(Optional.of("nested_map"), createType(new ArrayType(VARCHAR))))
                        .build()))
                .build();
        assertEquals(types, expected);
    }

    @Test
    public void testUnsupportedUnionType()
    {
        assertThatThrownBy(() -> new AvroSchemaConverter(new TestingTypeManager(), IGNORE)
                .convertAvroSchema(SchemaBuilder.record(RECORD_NAME).fields()
                        .name("union_col").type().unionOf().nullType().and().floatType().and().longType().endUnion().noDefault()
                        .endRecord()))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageStartingWith("Incompatible UNION type:");

        assertThatThrownBy(() -> new AvroSchemaConverter(new TestingTypeManager(), IGNORE)
                .convertAvroSchema(SchemaBuilder.record(RECORD_NAME).fields()
                        .name("union_col").type().unionOf().nullType().and().fixed("fixed").size(5).and().stringType().endUnion().noDefault()
                        .endRecord()))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageStartingWith("Incompatible UNION type:");

        assertThatThrownBy(() -> new AvroSchemaConverter(new TestingTypeManager(), IGNORE)
                .convertAvroSchema(SchemaBuilder.record(RECORD_NAME).fields()
                        .name("union_col").type().unionOf().nullType().and().booleanType().and().intType().endUnion().noDefault()
                        .endRecord()))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageStartingWith("Incompatible UNION type:");
    }

    @Test
    public void testSimpleSchema()
    {
        Schema schema = Schema.create(Schema.Type.LONG);
        AvroSchemaConverter avroSchemaConverter = new AvroSchemaConverter(new TestingTypeManager(), IGNORE);
        List<Type> types = avroSchemaConverter.convertAvroSchema(schema);
        assertEquals(getOnlyElement(types), BIGINT);
    }

    @Test
    public void testEmptyFieldStrategy()
    {
        Schema schema = SchemaBuilder.record(RECORD_NAME)
                .fields()
                .name("my_int").type().intType().noDefault()
                .name("my_record").type().record("nested_record")
                .fields()
                .endRecord()
                .noDefault()
                .name("my_array").type().array().items().type("nested_record").noDefault()
                .name("my_map").type().map().values().type("nested_record").noDefault()
                .endRecord();

        List<Type> typesForIgnoreStrategy = ImmutableList.of(INTEGER);

        assertEquals(new AvroSchemaConverter(new TestingTypeManager(), IGNORE).convertAvroSchema(schema), typesForIgnoreStrategy);

        assertThatThrownBy(() -> new AvroSchemaConverter(new TestingTypeManager(), FAIL).convertAvroSchema(schema))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Struct type has no valid fields for schema: '%s'", SchemaBuilder.record("nested_record").fields().endRecord());

        List<Type> typesForAddDummyStrategy = ImmutableList.<Type>builder()
                .add(INTEGER)
                .add(RowType.from(ImmutableList.of(new RowType.Field(Optional.of(DUMMY_FIELD_NAME), BOOLEAN))))
                .add(new ArrayType(RowType.from(ImmutableList.of(new RowType.Field(Optional.of(DUMMY_FIELD_NAME), BOOLEAN)))))
                .add(createType(RowType.from(ImmutableList.of(new RowType.Field(Optional.of(DUMMY_FIELD_NAME), BOOLEAN)))))
                .build();

        assertEquals(new AvroSchemaConverter(new TestingTypeManager(), MARK).convertAvroSchema(schema), typesForAddDummyStrategy);
    }

    @Test
    public void testEmptyFieldStrategyForEmptySchema()
    {
        Schema schema = SchemaBuilder.record(RECORD_NAME)
                .fields()
                .name("my_record").type().record("nested_record")
                .fields()
                .endRecord()
                .noDefault()
                .name("my_array").type().array().items().type("nested_record").noDefault()
                .name("my_map").type().map().values().type("nested_record").noDefault()
                .endRecord();

        assertThatThrownBy(() -> new AvroSchemaConverter(new TestingTypeManager(), IGNORE).convertAvroSchema(schema))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Schema has no valid fields: '%s'", schema);

        assertThatThrownBy(() -> new AvroSchemaConverter(new TestingTypeManager(), FAIL).convertAvroSchema(schema))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Struct type has no valid fields for schema: '%s'", SchemaBuilder.record("nested_record").fields().endRecord());

        List<Type> typesForAddDummyStrategy = ImmutableList.<Type>builder()
                .add(RowType.from(ImmutableList.of(new RowType.Field(Optional.of(DUMMY_FIELD_NAME), BOOLEAN))))
                .add(new ArrayType(RowType.from(ImmutableList.of(new RowType.Field(Optional.of(DUMMY_FIELD_NAME), BOOLEAN)))))
                .add(createType(RowType.from(ImmutableList.of(new RowType.Field(Optional.of(DUMMY_FIELD_NAME), BOOLEAN)))))
                .build();

        assertEquals(new AvroSchemaConverter(new TestingTypeManager(), MARK).convertAvroSchema(schema), typesForAddDummyStrategy);
    }

    private static Type createType(Type valueType)
    {
        Type keyType = VARCHAR;
        return new MapType(keyType, valueType, TYPE_MANAGER.getTypeOperators());
    }
}
